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

// writeCommand represents a command to write to the WebSocket connection
type writeCommand struct {
	messageType int
	data        []byte
	response    chan error // Channel to send back the result
}

// mockWebSocketServer provides a controllable WebSocket server for testing
type mockWebSocketServer struct {
	server      *httptest.Server
	upgrader    websocket.Upgrader
	connMu      sync.Mutex // Only for connection assignment/reassignment
	conn        *websocket.Conn
	closed      atomic.Bool
	messages    chan []byte
	pings       chan struct{}
	writeCmds   chan writeCommand // Channel for write commands
	stopHandler chan struct{}
	ignorePings atomic.Bool // When true, don't respond with pongs
}

func newMockWebSocketServer() *mockWebSocketServer {
	m := &mockWebSocketServer{
		upgrader:    websocket.Upgrader{},
		messages:    make(chan []byte, 100),
		pings:       make(chan struct{}, 100),
		writeCmds:   make(chan writeCommand, 10),
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

	// Connection assignment - this is the only place we need mutex
	m.connMu.Lock()
	// Close any existing connection
	if m.conn != nil {
		m.conn.Close()
	}
	m.conn = conn
	m.connMu.Unlock()

	// Ensure the connection gets cleared when this handler exits
	defer func() {
		m.connMu.Lock()
		if m.conn == conn {
			m.conn = nil
		}
		m.connMu.Unlock()
		conn.Close()
	}()

	// Handle connection lifecycle - this function owns all I/O on conn
	fmt.Println("WebSocket connection established")
	defer fmt.Println("WebSocket connection handler exiting")

	// We need to handle pings manually since we can't use the ping handler
	// (it would cause concurrent writes with our main loop)
	pingReceived := make(chan struct{}, 10)

	// Custom ping handler that just signals - no writing
	conn.SetPingHandler(func(string) error {
		fmt.Println("received ping")
		select {
		case m.pings <- struct{}{}:
		default:
		}
		select {
		case pingReceived <- struct{}{}:
		default:
		}
		return nil
	})

	// Start dedicated read goroutine - only reads, never writes
	readDone := make(chan error, 1)
	go func() {
		defer close(readDone)
		for {
			_, _, err := conn.ReadMessage()
			if err != nil {
				fmt.Printf("WebSocket read error: %v\n", err)
				readDone <- err
				return
			}
		}
	}()

	// Main write loop - all writes happen here sequentially
	for {
		select {
		case <-m.stopHandler:
			fmt.Println("WebSocket connection closed by stop signal")
			return

		case err := <-readDone:
			fmt.Printf("WebSocket connection closed by read error: %v\n", err)
			return

		case writeCmd := <-m.writeCmds:
			// Handle write command
			err := conn.WriteMessage(writeCmd.messageType, writeCmd.data)
			if writeCmd.response != nil {
				select {
				case writeCmd.response <- err:
				default:
				}
			}
			if err != nil {
				fmt.Printf("WebSocket write error: %v\n", err)
				return
			}

		case <-pingReceived:
			// Handle ping response (send pong)
			if !m.ignorePings.Load() {
				err := conn.WriteMessage(websocket.PongMessage, nil)
				if err != nil {
					fmt.Printf("WebSocket pong write error: %v\n", err)
					return
				}
			}
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

	// Connection will be closed by the handler when it receives stop signal
	// We just need to clear our reference after a brief delay
	go func() {
		time.Sleep(100 * time.Millisecond)
		m.connMu.Lock()
		if m.conn != nil {
			m.conn = nil // Just clear reference, handler already closed
		}
		m.connMu.Unlock()
	}()
}

func (m *mockWebSocketServer) shutdown() {
	m.close()
	m.server.Close()
}

func (m *mockWebSocketServer) restart() {
	// Reset for new connections
	m.closed.Store(false)
	// Drain stop handler channel and write command channel
	select {
	case <-m.stopHandler:
	default:
	}
	// Drain any pending write commands
drainLoop:
	for {
		select {
		case cmd := <-m.writeCmds:
			if cmd.response != nil {
				select {
				case cmd.response <- fmt.Errorf("server restarting"):
				default:
				}
			}
		default:
			break drainLoop
		}
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
	// Check if we have a connection without blocking
	m.connMu.Lock()
	hasConn := m.conn != nil
	m.connMu.Unlock()

	if !hasConn {
		return fmt.Errorf("no connection")
	}

	// Send write command via channel
	response := make(chan error, 1)
	cmd := writeCommand{
		messageType: websocket.BinaryMessage,
		data:        data,
		response:    response,
	}

	select {
	case m.writeCmds <- cmd:
		// Wait for response
		select {
		case err := <-response:
			return err
		case <-time.After(1 * time.Second):
			return fmt.Errorf("write timeout")
		}
	case <-time.After(1 * time.Second):
		return fmt.Errorf("write command queue full")
	}
}

// sendCloseMessage sends a WebSocket close message with specified code and reason
func (m *mockWebSocketServer) sendCloseMessage(code int, text string) error {
	// Check if we have a connection without blocking
	m.connMu.Lock()
	hasConn := m.conn != nil
	m.connMu.Unlock()

	if !hasConn {
		return fmt.Errorf("no connection")
	}

	// Format close message
	message := websocket.FormatCloseMessage(code, text)

	// Send write command via channel
	response := make(chan error, 1)
	cmd := writeCommand{
		messageType: websocket.CloseMessage,
		data:        message,
		response:    response,
	}

	select {
	case m.writeCmds <- cmd:
		// Wait for response
		select {
		case err := <-response:
			// After sending close, close the connection from our side too
			m.connMu.Lock()
			if m.conn != nil {
				m.conn.Close()
				m.conn = nil
			}
			m.connMu.Unlock()
			return err
		case <-time.After(1 * time.Second):
			return fmt.Errorf("write timeout")
		}
	case <-time.After(1 * time.Second):
		return fmt.Errorf("write command queue full")
	}
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

			// Wait for stable connection before testing
			assert.True(t, mockServer.waitForConnection(5*time.Second), "Should have stable connection before %s", tc.name)
			time.Sleep(300 * time.Millisecond) // Give time for ping cycle to establish

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
