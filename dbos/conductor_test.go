package dbos

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

// mockWebSocketServer provides a controllable WebSocket server for testing
type mockWebSocketServer struct {
	server      *httptest.Server
	upgrader    websocket.Upgrader
	connMu      sync.Mutex
	conn        *websocket.Conn
	closed      atomic.Bool
	messages    chan []byte
	pings       chan struct{}
	stopHandler chan struct{}
	ignorePings atomic.Bool // When true, don't respond with pongs
}

func newMockWebSocketServer() *mockWebSocketServer {
	m := &mockWebSocketServer{
		upgrader:    websocket.Upgrader{},
		messages:    make(chan []byte, 100),
		pings:       make(chan struct{}, 100),
		stopHandler: make(chan struct{}),
	}

	m.server = httptest.NewServer(http.HandlerFunc(m.handleWebSocket))
	return m
}

func (m *mockWebSocketServer) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	// Check if we're closed
	if m.closed.Load() {
		http.Error(w, "Server closed", http.StatusServiceUnavailable)
		return
	}

	conn, err := m.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	m.connMu.Lock()
	// Close any existing connection
	if m.conn != nil {
		m.conn.Close()
	}
	m.conn = conn
	m.connMu.Unlock()

	// Set up ping handler to capture pings
	conn.SetPingHandler(func(string) error {
		select {
		case m.pings <- struct{}{}:
		default:
		}
		// Only send pong if not ignoring pings
		if !m.ignorePings.Load() {
			return conn.WriteMessage(websocket.PongMessage, nil)
		}
		return nil
	})

	// Read messages until connection is closed
	for {
		select {
		case <-m.stopHandler:
			return
		default:
		}

		_, message, err := conn.ReadMessage()
		if err != nil {
			// Connection closed
			return
		}

		select {
		case m.messages <- message:
		default:
		}
	}
}

func (m *mockWebSocketServer) getURL() string {
	return "ws" + strings.TrimPrefix(m.server.URL, "http")
}

func (m *mockWebSocketServer) close() {
	m.closed.Store(true)

	// Signal handler to stop
	select {
	case m.stopHandler <- struct{}{}:
	default:
	}

	m.connMu.Lock()
	if m.conn != nil {
		m.conn.Close()
		m.conn = nil
	}
	m.connMu.Unlock()
}

func (m *mockWebSocketServer) shutdown() {
	m.close()
	m.server.Close()
}

func (m *mockWebSocketServer) restart() {
	// Reset for new connections
	m.closed.Store(false)
	// Drain and recreate stop handler channel
	select {
	case <-m.stopHandler:
	default:
	}
}

func (m *mockWebSocketServer) waitForConnection(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		m.connMu.Lock()
		hasConn := m.conn != nil
		m.connMu.Unlock()
		if hasConn {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// sendBinaryMessage sends a binary WebSocket message to the connected client
func (m *mockWebSocketServer) sendBinaryMessage(data []byte) error {
	m.connMu.Lock()
	defer m.connMu.Unlock()

	if m.conn == nil {
		return fmt.Errorf("no connection")
	}

	return m.conn.WriteMessage(websocket.BinaryMessage, data)
}

// sendTextMessage sends a text WebSocket message to the connected client
func (m *mockWebSocketServer) sendTextMessage(data string) error {
	m.connMu.Lock()
	defer m.connMu.Unlock()

	if m.conn == nil {
		return fmt.Errorf("no connection")
	}

	return m.conn.WriteMessage(websocket.TextMessage, []byte(data))
}

// sendCloseMessage sends a WebSocket close message with specified code and reason
func (m *mockWebSocketServer) sendCloseMessage(code int, text string) error {
	m.connMu.Lock()
	defer m.connMu.Unlock()

	if m.conn == nil {
		return fmt.Errorf("no connection")
	}

	// Write the close message
	message := websocket.FormatCloseMessage(code, text)
	err := m.conn.WriteMessage(websocket.CloseMessage, message)

	// After sending close, we should close the connection
	// This prevents concurrent writes and simulates proper WebSocket close behavior
	m.conn.Close()
	m.conn = nil

	return err
}

// TestConductorReconnection tests various reconnection scenarios for the conductor
func TestConductorReconnection(t *testing.T) {
	t.Run("ServerRestart", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		// Create and start mock server
		mockServer := newMockWebSocketServer()
		defer mockServer.shutdown()

		// Create conductor config
		config := ConductorConfig{
			url:     mockServer.getURL(),
			apiKey:  "test-key",
			appName: "test-app",
		}

		// Create context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Create dbosContext
		dbosCtx := &dbosContext{
			ctx:    ctx,
			logger: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})),
		}

		// Create conductor
		conductor, err := NewConductor(dbosCtx, config)
		require.NoError(t, err)

		// Speed up intervals for testing
		conductor.pingInterval = 100 * time.Millisecond
		conductor.pingTimeout = 200 * time.Millisecond
		conductor.reconnectWait = 100 * time.Millisecond

		// Launch conductor
		conductor.Launch()

		// Wait for initial connection
		assert.True(t, mockServer.waitForConnection(5*time.Second), "Should establish initial connection")

		// Collect initial pings
		initialPings := 0
		timeout := time.After(1 * time.Second)
	collectInitialPings:
		for {
			select {
			case <-mockServer.pings:
				initialPings++
			case <-timeout:
				break collectInitialPings
			}
		}
		assert.Greater(t, initialPings, 0, "Should receive initial pings")
		fmt.Printf("Received %d initial pings\n", initialPings)

		// Close the server connection (simulate disconnect)
		fmt.Println("Closing server connection")
		mockServer.close()

		// Wait a bit for conductor to notice and start reconnecting
		time.Sleep(500 * time.Millisecond)

		// Restart the server
		fmt.Println("Restarting server")
		mockServer.restart()

		// Wait for reconnection
		assert.True(t, mockServer.waitForConnection(10*time.Second), "Should reconnect after server restart")

		// Collect pings after reconnection
		reconnectPings := 0
		timeout2 := time.After(1 * time.Second)
	collectReconnectPings:
		for {
			select {
			case <-mockServer.pings:
				reconnectPings++
			case <-timeout2:
				break collectReconnectPings
			}
		}
		assert.Greater(t, reconnectPings, 0, "Should receive pings after reconnection")
		t.Logf("Received %d pings after reconnection", reconnectPings)

		// Cancel the context to trigger shutdown
		cancel()

		// Give conductor time to clean up
		time.Sleep(500 * time.Millisecond)
	})

	t.Run("TestBinaryMessage", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		// Create and start mock server
		mockServer := newMockWebSocketServer()
		defer mockServer.shutdown()

		// Create conductor config
		config := ConductorConfig{
			url:     mockServer.getURL(),
			apiKey:  "test-key",
			appName: "test-app",
		}

		// Create context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Create dbosContext
		dbosCtx := &dbosContext{
			ctx:    ctx,
			logger: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})),
		}

		// Create conductor
		conductor, err := NewConductor(dbosCtx, config)
		require.NoError(t, err)

		// Speed up intervals for testing
		conductor.pingInterval = 100 * time.Millisecond
		conductor.pingTimeout = 200 * time.Millisecond
		conductor.reconnectWait = 100 * time.Millisecond

		// Launch conductor
		conductor.Launch()

		// Wait for initial connection
		assert.True(t, mockServer.waitForConnection(5*time.Second), "Should establish initial connection")

		// Collect initial pings
		initialPings := 0
		timeout := time.After(1 * time.Second)
	collectInitialPings:
		for {
			select {
			case <-mockServer.pings:
				initialPings++
			case <-timeout:
				break collectInitialPings
			}
		}
		assert.Greater(t, initialPings, 0, "Should receive initial pings")
		fmt.Printf("Received %d initial pings\n", initialPings)

		// Send binary message - conductor should disconnect and reconnect
		fmt.Println("Sending binary message to trigger disconnect")
		err = mockServer.sendBinaryMessage([]byte{0xDE, 0xAD, 0xBE, 0xEF})
		assert.NoError(t, err, "Should send binary message successfully")

		// Wait a bit for conductor to process the message and disconnect
		time.Sleep(200 * time.Millisecond)

		// Wait for reconnection after binary message
		assert.True(t, mockServer.waitForConnection(10*time.Second), "Should reconnect after receiving binary message")

		// Collect pings after reconnection
		reconnectPings := 0
		timeout2 := time.After(1 * time.Second)
	collectReconnectPings:
		for {
			select {
			case <-mockServer.pings:
				reconnectPings++
			case <-timeout2:
				break collectReconnectPings
			}
		}
		assert.Greater(t, reconnectPings, 0, "Should receive pings after reconnection from binary message")
		t.Logf("Received %d pings after reconnection from binary message", reconnectPings)

		// Cancel the context to trigger shutdown
		cancel()

		// Give conductor time to clean up
		time.Sleep(500 * time.Millisecond)
	})

	// TestConductorPingTimeout tests that conductor reconnects when server stops responding to pings
	t.Run("TestConductorPingTimeout", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		// Create and start mock server
		mockServer := newMockWebSocketServer()
		defer mockServer.shutdown()

		// Create conductor config
		config := ConductorConfig{
			url:     mockServer.getURL(),
			apiKey:  "test-key",
			appName: "test-app",
		}

		// Create context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Create dbosContext
		dbosCtx := &dbosContext{
			ctx:    ctx,
			logger: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})),
		}

		// Create conductor
		conductor, err := NewConductor(dbosCtx, config)
		require.NoError(t, err)

		// Speed up intervals for testing
		conductor.pingInterval = 100 * time.Millisecond
		conductor.pingTimeout = 200 * time.Millisecond
		conductor.reconnectWait = 100 * time.Millisecond

		// Launch conductor
		conductor.Launch()

		// Wait for initial connection
		assert.True(t, mockServer.waitForConnection(5*time.Second), "Should establish initial connection")

		// Collect initial pings
		initialPings := 0
		timeout := time.After(1 * time.Second)
	collectInitialPings:
		for {
			select {
			case <-mockServer.pings:
				initialPings++
			case <-timeout:
				break collectInitialPings
			}
		}
		assert.Greater(t, initialPings, 0, "Should receive initial pings")
		fmt.Printf("Received %d initial pings\n", initialPings)

		// Tell server to stop responding to pings (no pongs)
		fmt.Println("Server stopping pong responses")
		mockServer.ignorePings.Store(true)

		// Wait for conductor to detect the dead connection (should timeout after pingTimeout)
		// Conductor should detect no pong response and close the connection
		// This will cause the handler to exit when ReadMessage fails
		time.Sleep(conductor.pingTimeout + 100*time.Millisecond)

		// Resume responding to pings after timeout
		// This allows the new connection handler to respond properly
		fmt.Println("Server resuming pong responses")
		mockServer.ignorePings.Store(false)

		// Wait for reconnection
		assert.True(t, mockServer.waitForConnection(10*time.Second), "Should reconnect after ping timeout")

		// Collect pings after reconnection
		reconnectPings := 0
		timeout2 := time.After(1 * time.Second)
	collectReconnectPings:
		for {
			select {
			case <-mockServer.pings:
				reconnectPings++
			case <-timeout2:
				break collectReconnectPings
			}
		}
		assert.Greater(t, reconnectPings, 0, "Should receive pings after reconnection")
		t.Logf("Received %d pings after reconnection", reconnectPings)

		// Cancel the context to trigger shutdown
		cancel()

		// Give conductor time to clean up
		time.Sleep(500 * time.Millisecond)
	})

	t.Run("CloseMessages", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		// Create and start mock server
		mockServer := newMockWebSocketServer()
		defer mockServer.shutdown()

		// Create conductor config
		config := ConductorConfig{
			url:     mockServer.getURL(),
			apiKey:  "test-key",
			appName: "test-app",
		}

		// Create context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Create dbosContext
		dbosCtx := &dbosContext{
			ctx:    ctx,
			logger: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})),
		}

		// Create conductor
		conductor, err := NewConductor(dbosCtx, config)
		require.NoError(t, err)

		// Speed up intervals for testing
		conductor.pingInterval = 100 * time.Millisecond
		conductor.pingTimeout = 200 * time.Millisecond
		conductor.reconnectWait = 100 * time.Millisecond

		// Launch conductor
		conductor.Launch()

		// Wait for initial connection
		assert.True(t, mockServer.waitForConnection(5*time.Second), "Should establish initial connection")

		// Test close message codes that should trigger reconnection
		testCases := []struct {
			code   int
			reason string
			name   string
		}{
			{websocket.CloseGoingAway, "server going away", "CloseGoingAway"},
			{websocket.CloseAbnormalClosure, "abnormal closure", "CloseAbnormalClosure"},
		}

		for _, tc := range testCases {
			t.Logf("Testing %s (code %d)", tc.name, tc.code)

			// Collect pings before sending close message
			beforePings := 0
			timeout := time.After(200 * time.Millisecond)
		collectBeforePings:
			for {
				select {
				case <-mockServer.pings:
					beforePings++
				case <-timeout:
					break collectBeforePings
				}
			}
			assert.Greater(t, beforePings, 0, "Should receive pings before %s", tc.name)

			// Send close message
			err = mockServer.sendCloseMessage(tc.code, tc.reason)
			assert.NoError(t, err, "Should send %s close message successfully", tc.name)

			// Wait for conductor to process and reconnect
			time.Sleep(300 * time.Millisecond)

			// Wait for reconnection
			assert.True(t, mockServer.waitForConnection(10*time.Second), "Should reconnect after %s", tc.name)

			// Verify pings after reconnection
			afterPings := 0
			timeout2 := time.After(200 * time.Millisecond)
		collectAfterPings:
			for {
				select {
				case <-mockServer.pings:
					afterPings++
				case <-timeout2:
					break collectAfterPings
				}
			}
			assert.Greater(t, afterPings, 0, "Should receive pings after reconnection from %s", tc.name)
		}

		// Cancel the context to trigger shutdown
		cancel()

		// Give conductor time to clean up
		time.Sleep(500 * time.Millisecond)
	})
}
