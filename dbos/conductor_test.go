package dbos

import (
	"context"
	"encoding/json"
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

	// We need to handle pings manually since we can't use the ping handler
	// (it would cause concurrent writes with our main loop)
	pingReceived := make(chan struct{}, 10)

	// Custom ping handler that just signals - no writing
	conn.SetPingHandler(func(string) error {
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

	// Start dedicated read goroutine - reads and forwards messages
	readDone := make(chan error, 1)
	go func() {
		defer close(readDone)
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				fmt.Printf("WebSocket read error: %v\n", err)
				readDone <- err
				return
			}
			select {
			case m.messages <- data:
			default:
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

	// Signal handler to stop but don't block
	select {
	case m.stopHandler <- struct{}{}:
	default:
	}
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

// sendTextMessage sends a text WebSocket message to the connected client
func (m *mockWebSocketServer) sendTextMessage(data []byte) error {
	m.connMu.Lock()
	hasConn := m.conn != nil
	m.connMu.Unlock()

	if !hasConn {
		return fmt.Errorf("no connection")
	}

	response := make(chan error, 1)
	cmd := writeCommand{
		messageType: websocket.TextMessage,
		data:        data,
		response:    response,
	}

	select {
	case m.writeCmds <- cmd:
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
		config := conductorConfig{
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
		conductor, err := newConductor(dbosCtx, config)
		require.NoError(t, err)

		// Speed up intervals for testing
		conductor.pingInterval = 100 * time.Millisecond
		conductor.pingTimeout = 200 * time.Millisecond
		conductor.reconnectWait = 100 * time.Millisecond

		// Launch conductor
		conductor.launch()

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
		config := conductorConfig{
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
		conductor, err := newConductor(dbosCtx, config)
		require.NoError(t, err)

		// Speed up intervals for testing
		conductor.pingInterval = 100 * time.Millisecond
		conductor.pingTimeout = 200 * time.Millisecond
		conductor.reconnectWait = 100 * time.Millisecond

		// Launch conductor
		conductor.launch()

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
		config := conductorConfig{
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
		conductor, err := newConductor(dbosCtx, config)
		require.NoError(t, err)

		// Speed up intervals for testing
		conductor.pingInterval = 100 * time.Millisecond
		conductor.pingTimeout = 200 * time.Millisecond
		conductor.reconnectWait = 100 * time.Millisecond

		// Launch conductor
		conductor.launch()

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
		config := conductorConfig{
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
		conductor, err := newConductor(dbosCtx, config)
		require.NoError(t, err)

		// Speed up intervals for testing
		conductor.pingInterval = 100 * time.Millisecond
		conductor.pingTimeout = 200 * time.Millisecond
		conductor.reconnectWait = 100 * time.Millisecond

		// Launch conductor
		conductor.launch()

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

func TestConductorAlertHandler(t *testing.T) {
	t.Run("WithHandler", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		mockServer := newMockWebSocketServer()
		defer mockServer.shutdown()

		config := conductorConfig{
			url:     mockServer.getURL(),
			apiKey:  "test-key",
			appName: "test-app",
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Track handler invocations
		var handlerName, handlerMessage string
		var handlerMetadata map[string]string
		handlerCalled := make(chan struct{}, 1)

		dbosCtx := &dbosContext{
			ctx:    ctx,
			logger: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})),
			alertHandler: func(name string, message string, metadata map[string]string) {
				handlerName = name
				handlerMessage = message
				handlerMetadata = metadata
				handlerCalled <- struct{}{}
			},
		}

		cond, err := newConductor(dbosCtx, config)
		require.NoError(t, err)
		cond.pingInterval = 100 * time.Millisecond
		cond.pingTimeout = 200 * time.Millisecond
		cond.reconnectWait = 100 * time.Millisecond

		cond.launch()
		assert.True(t, mockServer.waitForConnection(5*time.Second), "Should establish connection")

		// Send an alert message
		alertMsg := `{"type":"alert","request_id":"req-123","name":"test-alert","message":"something happened","metadata":{"key1":"val1","key2":"val2"}}`
		err = mockServer.sendTextMessage([]byte(alertMsg))
		require.NoError(t, err)

		// Wait for handler to be called
		select {
		case <-handlerCalled:
		case <-time.After(5 * time.Second):
			t.Fatal("alert handler was not called")
		}

		assert.Equal(t, "test-alert", handlerName)
		assert.Equal(t, "something happened", handlerMessage)
		assert.Equal(t, map[string]string{"key1": "val1", "key2": "val2"}, handlerMetadata)

		// Read the response sent back by conductor
		select {
		case respData := <-mockServer.messages:
			var resp alertConductorResponse
			err = json.Unmarshal(respData, &resp)
			require.NoError(t, err)
			assert.True(t, resp.Success)
			assert.Equal(t, "req-123", resp.RequestID)
			assert.Equal(t, alertMessage, resp.Type)
			assert.Nil(t, resp.ErrorMessage)
		case <-time.After(5 * time.Second):
			t.Fatal("did not receive alert response")
		}

		cancel()
		time.Sleep(500 * time.Millisecond)
	})

	t.Run("WithoutHandler", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		mockServer := newMockWebSocketServer()
		defer mockServer.shutdown()

		config := conductorConfig{
			url:     mockServer.getURL(),
			apiKey:  "test-key",
			appName: "test-app",
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		dbosCtx := &dbosContext{
			ctx:    ctx,
			logger: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})),
		}

		cond, err := newConductor(dbosCtx, config)
		require.NoError(t, err)
		cond.pingInterval = 100 * time.Millisecond
		cond.pingTimeout = 200 * time.Millisecond
		cond.reconnectWait = 100 * time.Millisecond

		cond.launch()
		assert.True(t, mockServer.waitForConnection(5*time.Second), "Should establish connection")

		// Send an alert with no handler registered
		alertMsg := `{"type":"alert","request_id":"req-456","name":"unhandled","message":"no handler","metadata":{}}`
		err = mockServer.sendTextMessage([]byte(alertMsg))
		require.NoError(t, err)

		// Should still get a success response (alert is logged but not an error)
		select {
		case respData := <-mockServer.messages:
			var resp alertConductorResponse
			err = json.Unmarshal(respData, &resp)
			require.NoError(t, err)
			assert.True(t, resp.Success)
			assert.Equal(t, "req-456", resp.RequestID)
		case <-time.After(5 * time.Second):
			t.Fatal("did not receive alert response")
		}

		cancel()
		time.Sleep(500 * time.Millisecond)
	})

	t.Run("HandlerPanic", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		mockServer := newMockWebSocketServer()
		defer mockServer.shutdown()

		config := conductorConfig{
			url:     mockServer.getURL(),
			apiKey:  "test-key",
			appName: "test-app",
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		dbosCtx := &dbosContext{
			ctx:    ctx,
			logger: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})),
			alertHandler: func(name string, message string, metadata map[string]string) {
				panic("handler exploded")
			},
		}

		cond, err := newConductor(dbosCtx, config)
		require.NoError(t, err)
		cond.pingInterval = 100 * time.Millisecond
		cond.pingTimeout = 200 * time.Millisecond
		cond.reconnectWait = 100 * time.Millisecond

		cond.launch()
		assert.True(t, mockServer.waitForConnection(5*time.Second), "Should establish connection")

		alertMsg := `{"type":"alert","request_id":"req-789","name":"panic-alert","message":"trigger panic","metadata":{}}`
		err = mockServer.sendTextMessage([]byte(alertMsg))
		require.NoError(t, err)

		// Should get a failure response with error message
		select {
		case respData := <-mockServer.messages:
			var resp alertConductorResponse
			err = json.Unmarshal(respData, &resp)
			require.NoError(t, err)
			assert.False(t, resp.Success)
			assert.Equal(t, "req-789", resp.RequestID)
			assert.NotNil(t, resp.ErrorMessage)
			assert.Contains(t, *resp.ErrorMessage, "panic in alert handler")
		case <-time.After(5 * time.Second):
			t.Fatal("did not receive alert response")
		}

		cancel()
		time.Sleep(500 * time.Millisecond)
	})
}

// TestConductorScheduleHandlers covers the happy path for each schedule
// message type the conductor can route to a DBOS node.
func TestConductorScheduleHandlers(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, schedulerPollingInterval: 100 * time.Millisecond})
	RegisterWorkflow(dbosCtx, testWorkflowForSchedule)
	require.NoError(t, dbosCtx.Launch())

	const baseSchedule = "cond-base-schedule"
	require.NoError(t, CreateSchedule(dbosCtx, testWorkflowForSchedule, CreateScheduleRequest{
		ScheduleName: baseSchedule,
		Schedule:     "0 0 0 1 1 *",
	}, WithScheduleContext("hello")))

	mockServer := newMockWebSocketServer()
	t.Cleanup(mockServer.shutdown)

	cond, err := newConductor(dbosCtx.(*dbosContext), conductorConfig{
		url:     mockServer.getURL(),
		apiKey:  "test-key",
		appName: "test-app",
	})
	require.NoError(t, err)
	cond.pingInterval = 100 * time.Millisecond
	cond.pingTimeout = 200 * time.Millisecond
	cond.reconnectWait = 100 * time.Millisecond
	cond.launch()
	t.Cleanup(func() { cond.shutdown(2 * time.Second) })
	require.True(t, mockServer.waitForConnection(5*time.Second))

	// expect waits for a response of the expected type, returning the raw bytes.
	expect := func(t *testing.T, wantType messageType) []byte {
		t.Helper()
		deadline := time.After(5 * time.Second)
		for {
			select {
			case raw := <-mockServer.messages:
				var base baseMessage
				if err := json.Unmarshal(raw, &base); err == nil && base.Type == wantType {
					return raw
				}
				// Drop unrelated traffic (e.g. executor_info on connect).
			case <-deadline:
				t.Fatalf("timed out waiting for response of type %s", wantType)
			}
		}
	}

	t.Run("list_schedules", func(t *testing.T) {
		require.NoError(t, mockServer.sendTextMessage([]byte(`{"type":"list_schedules","request_id":"r1","body":{}}`)))
		var resp listSchedulesConductorResponse
		require.NoError(t, json.Unmarshal(expect(t, listSchedulesMessage), &resp))
		require.Equal(t, "r1", resp.RequestID)
		require.Nil(t, resp.ErrorMessage)
		require.Equal(t, 1, len(resp.Output))
		require.Equal(t, baseSchedule, resp.Output[0].ScheduleName)
		require.NotNil(t, resp.Output[0].Context, "load_context defaults to true")
	})

	t.Run("list_schedules_no_context", func(t *testing.T) {
		require.NoError(t, mockServer.sendTextMessage([]byte(`{"type":"list_schedules","request_id":"r2","body":{"load_context":false}}`)))
		var resp listSchedulesConductorResponse
		require.NoError(t, json.Unmarshal(expect(t, listSchedulesMessage), &resp))
		require.Nil(t, resp.ErrorMessage)
		require.Equal(t, 1, len(resp.Output))
		require.Nil(t, resp.Output[0].Context, "load_context=false should omit context")
	})

	t.Run("get_schedule", func(t *testing.T) {
		req := fmt.Sprintf(`{"type":"get_schedule","request_id":"r3","schedule_name":%q}`, baseSchedule)
		require.NoError(t, mockServer.sendTextMessage([]byte(req)))
		var resp getScheduleConductorResponse
		require.NoError(t, json.Unmarshal(expect(t, getScheduleMessage), &resp))
		require.Nil(t, resp.ErrorMessage)
		require.NotNil(t, resp.Output)
		require.Equal(t, baseSchedule, resp.Output.ScheduleName)
	})

	t.Run("get_schedule_missing", func(t *testing.T) {
		require.NoError(t, mockServer.sendTextMessage([]byte(`{"type":"get_schedule","request_id":"r4","schedule_name":"does-not-exist"}`)))
		var resp getScheduleConductorResponse
		require.NoError(t, json.Unmarshal(expect(t, getScheduleMessage), &resp))
		require.Nil(t, resp.ErrorMessage)
		require.Nil(t, resp.Output)
	})

	t.Run("pause_resume_schedule", func(t *testing.T) {
		req := fmt.Sprintf(`{"type":"pause_schedule","request_id":"r5","schedule_name":%q}`, baseSchedule)
		require.NoError(t, mockServer.sendTextMessage([]byte(req)))
		var pauseResp pauseScheduleConductorResponse
		require.NoError(t, json.Unmarshal(expect(t, pauseScheduleMessage), &pauseResp))
		require.True(t, pauseResp.Success)
		got, err := GetSchedule(dbosCtx, baseSchedule)
		require.NoError(t, err)
		require.Equal(t, ScheduleStatusPaused, got.Status)

		req = fmt.Sprintf(`{"type":"resume_schedule","request_id":"r6","schedule_name":%q}`, baseSchedule)
		require.NoError(t, mockServer.sendTextMessage([]byte(req)))
		var resumeResp resumeScheduleConductorResponse
		require.NoError(t, json.Unmarshal(expect(t, resumeScheduleMessage), &resumeResp))
		require.True(t, resumeResp.Success)
		got, err = GetSchedule(dbosCtx, baseSchedule)
		require.NoError(t, err)
		require.Equal(t, ScheduleStatusActive, got.Status)
	})

	t.Run("backfill_schedule", func(t *testing.T) {
		// Create a fast-cron schedule so backfill produces multiple ticks.
		const fastSchedule = "cond-backfill-schedule"
		require.NoError(t, CreateSchedule(dbosCtx, testWorkflowForSchedule, CreateScheduleRequest{
			ScheduleName: fastSchedule,
			Schedule:     "*/1 * * * * *",
		}))
		t.Cleanup(func() { _ = DeleteSchedule(dbosCtx, fastSchedule) })

		startISO := time.Now().Add(-3 * time.Second).Format(time.RFC3339Nano)
		endISO := time.Now().Format(time.RFC3339Nano)
		req := fmt.Sprintf(`{"type":"backfill_schedule","request_id":"r7","schedule_name":%q,"start":%q,"end":%q}`,
			fastSchedule, startISO, endISO)
		require.NoError(t, mockServer.sendTextMessage([]byte(req)))
		var resp backfillScheduleConductorResponse
		require.NoError(t, json.Unmarshal(expect(t, backfillScheduleMessage), &resp))
		require.Nil(t, resp.ErrorMessage)
		require.NotEmpty(t, resp.WorkflowIDs)
	})

	t.Run("trigger_schedule", func(t *testing.T) {
		req := fmt.Sprintf(`{"type":"trigger_schedule","request_id":"r8","schedule_name":%q}`, baseSchedule)
		require.NoError(t, mockServer.sendTextMessage([]byte(req)))
		var resp triggerScheduleConductorResponse
		require.NoError(t, json.Unmarshal(expect(t, triggerScheduleMessage), &resp))
		require.Nil(t, resp.ErrorMessage)
		require.NotNil(t, resp.WorkflowID)
		require.Contains(t, *resp.WorkflowID, baseSchedule)
	})

	t.Run("trigger_schedule_missing", func(t *testing.T) {
		require.NoError(t, mockServer.sendTextMessage([]byte(`{"type":"trigger_schedule","request_id":"r9","schedule_name":"missing"}`)))
		var resp triggerScheduleConductorResponse
		require.NoError(t, json.Unmarshal(expect(t, triggerScheduleMessage), &resp))
		require.Nil(t, resp.WorkflowID)
		require.NotNil(t, resp.ErrorMessage)
	})
}
