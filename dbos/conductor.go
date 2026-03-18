package dbos

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/rand"
	"net"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

const (
	_PING_INTERVAL          = 20 * time.Second
	_PING_TIMEOUT           = 30 * time.Second // Should be slightly greater than server's executorPingWait (25s)
	_INITIAL_RECONNECT_WAIT = 1 * time.Second
	_MAX_RECONNECT_WAIT     = 30 * time.Second
	_HANDSHAKE_TIMEOUT      = 10 * time.Second
	_WRITE_DEADLINE         = 5 * time.Second
)

// conductorConfig contains configuration for the conductor
type conductorConfig struct {
	url     string
	apiKey  string
	appName string
}

// conductor manages the WebSocket connection to the DBOS conductor service
type conductor struct {
	dbosCtx *dbosContext
	logger  *slog.Logger

	// Connection management
	conn           *websocket.Conn
	needsReconnect atomic.Bool
	wg             sync.WaitGroup
	stopOnce       sync.Once
	writeMu        sync.Mutex // writeMu protects concurrent writes to the WebSocket connection (pings + handling messages)

	// Connection parameters
	url           url.URL
	pingInterval  time.Duration
	pingTimeout   time.Duration
	reconnectWait time.Duration

	// pingCancel cancels the ping goroutine context
	pingCancel context.CancelFunc
}

// launch starts the conductor main goroutine
func (c *conductor) launch() {
	c.logger.Info("Launching conductor")
	c.wg.Add(1)
	go c.run()
}

func newConductor(dbosCtx *dbosContext, config conductorConfig) (*conductor, error) {
	if config.apiKey == "" {
		return nil, fmt.Errorf("conductor API key is required")
	}
	if config.url == "" {
		return nil, fmt.Errorf("conductor URL is required")
	}

	baseURL, err := url.Parse(config.url)
	if err != nil {
		return nil, fmt.Errorf("invalid conductor URL: %w", err)
	}

	wsURL := url.URL{
		Scheme: baseURL.Scheme,
		Host:   baseURL.Host,
		Path:   baseURL.JoinPath("websocket", config.appName, config.apiKey).Path,
	}

	c := &conductor{
		dbosCtx:       dbosCtx,
		url:           wsURL,
		pingInterval:  _PING_INTERVAL,
		pingTimeout:   _PING_TIMEOUT,
		reconnectWait: _INITIAL_RECONNECT_WAIT,
		logger:        dbosCtx.logger.With("service", "conductor"),
	}

	// Start with needsReconnect set to true so we connect on first run
	c.needsReconnect.Store(true)

	return c, nil
}

func (c *conductor) shutdown(timeout time.Duration) {
	c.stopOnce.Do(func() {
		if c.pingCancel != nil {
			c.pingCancel()
		}

		c.closeConn()

		done := make(chan struct{})
		go func() {
			c.wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			c.logger.Info("Conductor shut down")
		case <-time.After(timeout):
			c.logger.Warn("Timeout waiting for conductor to shut down", "timeout", timeout)
		}
	})
}

// reconnectWaitWithJitter adds random jitter to the reconnect wait time to prevent thundering herd
func (c *conductor) reconnectWaitWithJitter() time.Duration {
	// Add jitter: random value between 0.5 * wait and 1.5 * wait
	jitter := 0.5 + rand.Float64() // #nosec G404 -- jitter for backoff doesn't need crypto-secure randomness
	return time.Duration(float64(c.reconnectWait) * jitter)
}

// closeConn closes the connection and signals that reconnection is needed
func (c *conductor) closeConn() {
	// Cancel ping goroutine first
	if c.pingCancel != nil {
		c.pingCancel()
		c.pingCancel = nil
	}

	// Acquire write mutex to ensure no concurrent writes during close
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if c.conn != nil {
		if err := c.conn.SetWriteDeadline(time.Now().Add(_WRITE_DEADLINE)); err != nil {
			c.logger.Warn("Failed to set write deadline", "error", err)
		}
		err := c.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "shutting down"))
		if err != nil {
			c.logger.Warn("Failed to send close message", "error", err)
		}
		err = c.conn.Close()
		if err != nil {
			c.logger.Warn("Failed to close connection", "error", err)
		}
		c.conn = nil
	}
	// Signal that we need to reconnect
	c.needsReconnect.Store(true)
}

func (c *conductor) run() {
	defer c.wg.Done()

	for {
		// Check if the context has been cancelled
		select {
		case <-c.dbosCtx.Done():
			c.logger.Info("DBOS context done, stopping conductor", "cause", context.Cause(c.dbosCtx))
			c.closeConn()
			return
		default:
		}

		// Connect if reconnection is needed
		if c.needsReconnect.Load() {
			if err := c.connect(); err != nil {
				c.logger.Warn("Failed to connect to conductor", "error", err)
				select {
				case <-c.dbosCtx.Done():
					c.logger.Info("DBOS context done, stopping conductor", "cause", context.Cause(c.dbosCtx))
					return
				case <-time.After(c.reconnectWaitWithJitter()):
					// Exponential backoff with jitter up to max wait
					if c.reconnectWait < _MAX_RECONNECT_WAIT {
						c.reconnectWait *= 2
						if c.reconnectWait > _MAX_RECONNECT_WAIT {
							c.reconnectWait = _MAX_RECONNECT_WAIT
						}
					}
					continue
				}
			}
			// Reset reconnect wait and clear reconnect flag on successful connection
			c.reconnectWait = _INITIAL_RECONNECT_WAIT
			c.needsReconnect.Store(false)
		}

		// This shouldn't happen but check anyway
		if c.conn == nil {
			c.needsReconnect.Store(true)
			continue
		}

		// Read message (will timeout based on read deadline set in connect)
		messageType, message, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				c.logger.Warn("Unexpected WebSocket close", "error", err)
			} else if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				c.logger.Debug("Read deadline reached", "error", err)
			} else {
				c.logger.Debug("Connection closed", "error", err)
			}
			// Close connection to trigger reconnection
			c.closeConn()
			continue
		}

		// Only accept text messages
		if messageType != websocket.TextMessage {
			c.logger.Warn("Received unexpected message type, forcing reconnection", "type", messageType)
			c.closeConn()
			continue
		}

		ht := time.Now()
		if err := c.handleMessage(message); err != nil {
			c.logger.Error("Failed to handle message", "error", err)
		}
		c.logger.Debug("Handled message", "message", messageType, "latency_us", time.Since(ht).Microseconds())
	}
}

func (c *conductor) connect() error {
	c.logger.Debug("Connecting to conductor")

	dialer := websocket.Dialer{
		HandshakeTimeout: _HANDSHAKE_TIMEOUT,
	}

	conn, resp, err := dialer.Dial(c.url.String(), nil)
	if err != nil {
		// Include HTTP response details if available
		baseErr := fmt.Errorf("failed to dial conductor: %w", err)
		if resp != nil {
			// Read response body if available
			body := ""
			if resp.Body != nil {
				bodyBytes, readErr := io.ReadAll(resp.Body)
				if closeErr := resp.Body.Close(); closeErr != nil {
					c.logger.Debug("Failed to close response body", "error", closeErr)
				}
				if readErr == nil && len(bodyBytes) > 0 {
					body = string(bodyBytes)
				}
			}
			return fmt.Errorf("%w (%s)", baseErr, body)
		}
		return baseErr
	}

	// Set initial read deadline
	if err := conn.SetReadDeadline(time.Now().Add(c.pingTimeout)); err != nil {
		cErr := conn.Close()
		if cErr != nil {
			c.logger.Warn("Failed to close connection", "error", cErr)
		}
		return fmt.Errorf("failed to set read deadline: %w", err)
	}

	// Set pong handler to reset read deadline
	conn.SetPongHandler(func(appData string) error {
		c.logger.Debug("Received pong from conductor")
		return conn.SetReadDeadline(time.Now().Add(c.pingTimeout))
	})

	// Store the connection
	c.conn = conn

	// Create a cancellable context for the ping goroutine
	pingCtx, pingCancel := context.WithCancel(c.dbosCtx)
	c.pingCancel = pingCancel

	// Start ping goroutine
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ticker := time.NewTicker(c.pingInterval)
		defer ticker.Stop()

		for {
			select {
			case <-pingCtx.Done():
				c.logger.Debug("Exiting Conductor ping goroutine", "cause", context.Cause(pingCtx))
				return
			case <-ticker.C:
				if err := c.ping(); err != nil {
					c.logger.Warn("Ping failed, signaling reconnection", "error", err)
					// Signal that we need to reconnect and exit ping goroutine
					c.needsReconnect.Store(true)
					return
				}
			}
		}
	}()

	c.logger.Info("Connected to DBOS conductor")
	return nil
}

func (c *conductor) ping() error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if c.conn == nil {
		return fmt.Errorf("no connection")
	}

	c.logger.Debug("Sending ping to conductor")

	if err := c.conn.SetWriteDeadline(time.Now().Add(_WRITE_DEADLINE)); err != nil {
		c.logger.Warn("Failed to set write deadline for ping", "error", err)
	}
	if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
		return fmt.Errorf("failed to send ping: %w", err)
	}
	if err := c.conn.SetWriteDeadline(time.Time{}); err != nil {
		c.logger.Warn("Failed to clear write deadline", "error", err)
	}

	return nil
}

func (c *conductor) handleMessage(data []byte) error {
	var base baseMessage
	if err := json.Unmarshal(data, &base); err != nil {
		c.logger.Error("Failed to parse message", "error", err)
		return fmt.Errorf("failed to parse base message: %w", err)
	}
	c.logger.Debug("Received message", "type", base.Type, "request_id", base.RequestID)

	switch base.Type {
	case executorInfo:
		return c.handleExecutorInfoRequest(data, base.RequestID)
	case recoveryMessage:
		return c.handleRecoveryRequest(data, base.RequestID)
	case cancelWorkflowMessage:
		return c.handleCancelWorkflowRequest(data, base.RequestID)
	case resumeWorkflowMessage:
		return c.handleResumeWorkflowRequest(data, base.RequestID)
	case listWorkflowsMessage:
		return c.handleListWorkflowsRequest(data, base.RequestID)
	case listQueuedWorkflowsMessage:
		return c.handleListQueuedWorkflowsRequest(data, base.RequestID)
	case listStepsMessage:
		return c.handleListStepsRequest(data, base.RequestID)
	case getWorkflowMessage:
		return c.handleGetWorkflowRequest(data, base.RequestID)
	case forkWorkflowMessage:
		return c.handleForkWorkflowRequest(data, base.RequestID)
	case existPendingWorkflowsMessage:
		return c.handleExistPendingWorkflowsRequest(data, base.RequestID)
	case retentionMessage:
		return c.handleRetentionRequest(data, base.RequestID)
	case getMetricsMessage:
		return c.handleGetMetricsRequest(data, base.RequestID)
	default:
		c.logger.Warn("Unknown message type", "type", base.Type)
		return c.handleUnknownMessageType(base.RequestID, base.Type, "Unknown message type")
	}
}

func (c *conductor) handleExecutorInfoRequest(data []byte, requestID string) error {
	var req executorInfoRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse executor info request", "error", err)
		return fmt.Errorf("failed to parse executor info request: %w", err)
	}
	c.logger.Debug("Handling executor info request", "request_id", req)

	hostname, err := os.Hostname()
	if err != nil {
		c.logger.Error("Failed to get hostname", "error", err)
		return fmt.Errorf("failed to get hostname: %w", err)
	}

	response := executorInfoResponse{
		baseResponse: baseResponse{
			baseMessage: baseMessage{
				Type:      executorInfo,
				RequestID: requestID,
			},
		},
		ExecutorID:         c.dbosCtx.GetExecutorID(),
		ApplicationVersion: c.dbosCtx.GetApplicationVersion(),
		Hostname:           &hostname,
		DBOSVersion:        getDBOSVersion(),
		Language:           "go",
	}

	return c.sendResponse(response, string(executorInfo))
}

func (c *conductor) handleRecoveryRequest(data []byte, requestID string) error {
	var req recoveryConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse recovery request", "error", err)
		return fmt.Errorf("failed to parse recovery request: %w", err)
	}
	c.logger.Debug("Handling recovery request", "executor_ids", req.ExecutorIDs, "request_id", requestID)

	success := true
	var errorMsg *string

	_, err := recoverPendingWorkflows(c.dbosCtx, req.ExecutorIDs)
	if err != nil {
		c.logger.Error("Failed to recover pending workflows", "executor_ids", req.ExecutorIDs, "error", err)
		errStr := fmt.Sprintf("failed to recover pending workflows: %v", err)
		errorMsg = &errStr
		success = false
	} else {
		c.logger.Info("Successfully recovered pending workflows", "executor_ids", req.ExecutorIDs)
	}

	response := recoveryConductorResponse{
		baseResponse: baseResponse{
			baseMessage: baseMessage{
				Type:      recoveryMessage,
				RequestID: requestID,
			},
			ErrorMessage: errorMsg,
		},
		Success: success,
	}

	return c.sendResponse(response, string(recoveryMessage))
}

func (c *conductor) handleCancelWorkflowRequest(data []byte, requestID string) error {
	var req cancelWorkflowConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse cancel workflow request", "error", err)
		return fmt.Errorf("failed to parse cancel workflow request: %w", err)
	}
	c.logger.Debug("Handling cancel workflow request", "workflow_id", req.WorkflowID, "request_id", requestID)

	success := true
	var errorMsg *string

	if err := c.dbosCtx.CancelWorkflow(c.dbosCtx, req.WorkflowID); err != nil {
		c.logger.Error("Failed to cancel workflow", "workflow_id", req.WorkflowID, "error", err)
		errStr := fmt.Sprintf("failed to cancel workflow: %v", err)
		errorMsg = &errStr
		success = false
	} else {
		c.logger.Info("Successfully cancelled workflow", "workflow_id", req.WorkflowID)
	}

	response := cancelWorkflowConductorResponse{
		baseResponse: baseResponse{
			baseMessage: baseMessage{
				Type:      cancelWorkflowMessage,
				RequestID: requestID,
			},
			ErrorMessage: errorMsg,
		},
		Success: success,
	}

	return c.sendResponse(response, string(cancelWorkflowMessage))
}

func (c *conductor) handleResumeWorkflowRequest(data []byte, requestID string) error {
	var req resumeWorkflowConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse resume workflow request", "error", err)
		return fmt.Errorf("failed to parse resume workflow request: %w", err)
	}
	c.logger.Debug("Handling resume workflow request", "workflow_id", req.WorkflowID, "request_id", requestID)

	success := true
	var errorMsg *string

	_, err := c.dbosCtx.ResumeWorkflow(c.dbosCtx, req.WorkflowID)
	if err != nil {
		c.logger.Error("Failed to resume workflow", "workflow_id", req.WorkflowID, "error", err)
		errStr := fmt.Sprintf("failed to resume workflow: %v", err)
		errorMsg = &errStr
		success = false
	} else {
		c.logger.Info("Successfully resumed workflow", "workflow_id", req.WorkflowID)
	}

	response := resumeWorkflowConductorResponse{
		baseResponse: baseResponse{
			baseMessage: baseMessage{
				Type:      resumeWorkflowMessage,
				RequestID: requestID,
			},
			ErrorMessage: errorMsg,
		},
		Success: success,
	}

	return c.sendResponse(response, string(resumeWorkflowMessage))
}

func (c *conductor) handleRetentionRequest(data []byte, requestID string) error {
	var req retentionConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse retention request", "error", err)
		return fmt.Errorf("failed to parse retention request: %w", err)
	}
	c.logger.Debug("Handling retention request", "request", req, "request_id", requestID)

	success := true
	var errorMsg *string

	// Handle garbage collection if parameters are provided
	if req.Body.GCCutoffEpochMs != nil || req.Body.GCRowsThreshold != nil {
		var cutoffMs *int64
		if req.Body.GCCutoffEpochMs != nil {
			ms := int64(*req.Body.GCCutoffEpochMs)
			cutoffMs = &ms
		}

		var rowsThreshold *int
		if req.Body.GCRowsThreshold != nil {
			rowsThreshold = req.Body.GCRowsThreshold
		}

		input := garbageCollectWorkflowsInput{
			cutoffEpochTimestampMs: cutoffMs,
			rowsThreshold:          rowsThreshold,
		}

		err := retry(c.dbosCtx, func() error {
			return c.dbosCtx.systemDB.garbageCollectWorkflows(c.dbosCtx, input)
		}, withRetrierLogger(c.logger))
		if err != nil {
			c.logger.Error("Failed to garbage collect workflows", "error", err)
			errStr := fmt.Sprintf("failed to garbage collect workflows: %v", err)
			errorMsg = &errStr
			success = false
		} else {
			c.logger.Info("Successfully garbage collected workflows", "cutoff_ms", cutoffMs, "rows_threshold", rowsThreshold)
		}
	}

	// Handle timeout enforcement if parameter is provided and garbage collection succeeded
	if success && req.Body.TimeoutCutoffEpochMs != nil {
		cutoffTime := time.UnixMilli(int64(*req.Body.TimeoutCutoffEpochMs))
		err := retry(c.dbosCtx, func() error {
			return c.dbosCtx.systemDB.cancelAllBefore(c.dbosCtx, cutoffTime)
		}, withRetrierLogger(c.logger))
		if err != nil {
			c.logger.Error("Failed to timeout workflows", "cutoff_ms", *req.Body.TimeoutCutoffEpochMs, "error", err)
			errStr := fmt.Sprintf("failed to timeout workflows: %v", err)
			errorMsg = &errStr
			success = false
		} else {
			c.logger.Info("Successfully timed out workflows", "cutoff_ms", *req.Body.TimeoutCutoffEpochMs)
		}
	}

	response := retentionConductorResponse{
		baseResponse: baseResponse{
			baseMessage: baseMessage{
				Type:      retentionMessage,
				RequestID: requestID,
			},
			ErrorMessage: errorMsg,
		},
		Success: success,
	}

	return c.sendResponse(response, string(retentionMessage))
}

func (c *conductor) handleGetMetricsRequest(data []byte, requestID string) error {
	var req getMetricsConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse get metrics request", "error", err)
		return fmt.Errorf("failed to parse get metrics request: %w", err)
	}
	c.logger.Debug("Handling get metrics request",
		"start_time", req.StartTime,
		"end_time", req.EndTime,
		"metric_class", req.MetricClass,
		"request_id", requestID)

	var errorMsg *string
	var metricsData []metricData

	if req.MetricClass == "workflow_step_count" {
		var err error
		metricsData, err = retryWithResult(c.dbosCtx, func() ([]metricData, error) {
			return c.dbosCtx.systemDB.getMetrics(c.dbosCtx, req.StartTime, req.EndTime)
		}, withRetrierLogger(c.logger))
		if err != nil {
			c.logger.Error("Failed to get metrics", "error", err)
			errStr := fmt.Sprintf("Exception encountered when getting metrics: %v", err)
			errorMsg = &errStr
		}
	} else {
		errStr := fmt.Sprintf("Unexpected metric class: %s", req.MetricClass)
		errorMsg = &errStr
		c.logger.Warn("Unexpected metric class", "metric_class", req.MetricClass)
	}

	response := getMetricsConductorResponse{
		baseResponse: baseResponse{
			baseMessage: baseMessage{
				Type:      getMetricsMessage,
				RequestID: requestID,
			},
			ErrorMessage: errorMsg,
		},
		Metrics: metricsData,
	}

	return c.sendResponse(response, string(getMetricsMessage))
}

func (c *conductor) handleListWorkflowsRequest(data []byte, requestID string) error {
	var req listWorkflowsConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse list workflows request", "error", err)
		return fmt.Errorf("failed to parse list workflows request: %w", err)
	}
	c.logger.Debug("Handling list workflows request", "request", req)

	var opts []ListWorkflowsOption
	opts = append(opts, WithLoadInput(req.Body.LoadInput))
	opts = append(opts, WithLoadOutput(req.Body.LoadOutput))
	if req.Body.SortDesc {
		opts = append(opts, WithSortDesc())
	}
	if req.Body.QueuesOnly {
		opts = append(opts, WithQueuesOnly())
	}
	if len(req.Body.WorkflowUUIDs) > 0 {
		opts = append(opts, WithWorkflowIDs(req.Body.WorkflowUUIDs))
	}
	if len(req.Body.WorkflowName) > 0 {
		opts = append(opts, WithName(req.Body.WorkflowName.toSlice()...))
	}
	if len(req.Body.AuthenticatedUser) > 0 {
		opts = append(opts, WithUser(req.Body.AuthenticatedUser.toSlice()...))
	}
	if len(req.Body.ApplicationVersion) > 0 {
		opts = append(opts, WithAppVersion(req.Body.ApplicationVersion.toSlice()...))
	}
	if req.Body.Limit != nil {
		opts = append(opts, WithLimit(*req.Body.Limit))
	}
	if req.Body.Offset != nil {
		opts = append(opts, WithOffset(*req.Body.Offset))
	}
	if req.Body.StartTime != nil {
		opts = append(opts, WithStartTime(*req.Body.StartTime))
	}
	if req.Body.EndTime != nil {
		opts = append(opts, WithEndTime(*req.Body.EndTime))
	}
	if len(req.Body.Status) > 0 {
		statuses := make([]WorkflowStatusType, len(req.Body.Status))
		for i, s := range req.Body.Status {
			statuses[i] = WorkflowStatusType(s)
		}
		opts = append(opts, WithStatus(statuses))
	}
	if len(req.Body.ForkedFrom) > 0 {
		opts = append(opts, WithForkedFrom(req.Body.ForkedFrom.toSlice()...))
	}
	if len(req.Body.ParentWorkflowID) > 0 {
		opts = append(opts, WithParentWorkflowID(req.Body.ParentWorkflowID.toSlice()...))
	}
	if len(req.Body.QueueName) > 0 {
		opts = append(opts, WithQueueName(req.Body.QueueName.toSlice()...))
	}
	if len(req.Body.WorkflowIDPrefix) > 0 {
		opts = append(opts, WithWorkflowIDPrefix(req.Body.WorkflowIDPrefix.toSlice()...))
	}
	if len(req.Body.ExecutorID) > 0 {
		opts = append(opts, WithExecutorIDs(req.Body.ExecutorID.toSlice()))
	}

	workflows, err := c.dbosCtx.ListWorkflows(c.dbosCtx, opts...)
	if err != nil {
		c.logger.Error("Failed to list workflows", "error", err)
		errorMsg := fmt.Sprintf("failed to list workflows: %v", err)
		response := listWorkflowsConductorResponse{
			baseResponse: baseResponse{
				baseMessage: baseMessage{
					Type:      listWorkflowsMessage,
					RequestID: requestID,
				},
				ErrorMessage: &errorMsg,
			},
			Output: []listWorkflowsConductorResponseBody{},
		}
		return c.sendResponse(response, "list workflows response")
	}

	formattedWorkflows := make([]listWorkflowsConductorResponseBody, len(workflows))
	for i, wf := range workflows {
		formattedWorkflows[i] = formatListWorkflowsResponseBody(wf)
	}

	response := listWorkflowsConductorResponse{
		baseResponse: baseResponse{
			baseMessage: baseMessage{
				Type:      listWorkflowsMessage,
				RequestID: requestID,
			},
		},
		Output: formattedWorkflows,
	}

	return c.sendResponse(response, string(listWorkflowsMessage))
}

func (c *conductor) handleListQueuedWorkflowsRequest(data []byte, requestID string) error {
	var req listWorkflowsConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse list queued workflows request", "error", err)
		return fmt.Errorf("failed to parse list queued workflows request: %w", err)
	}
	c.logger.Debug("Handling list queued workflows request", "request", req)

	// Build functional options for ListWorkflows
	var opts []ListWorkflowsOption
	opts = append(opts, WithLoadInput(req.Body.LoadInput))
	opts = append(opts, WithLoadOutput(false)) // Don't load output for queued workflows
	opts = append(opts, WithQueuesOnly())      // Only include workflows that are in queues
	if len(req.Body.WorkflowUUIDs) > 0 {
		opts = append(opts, WithWorkflowIDs(req.Body.WorkflowUUIDs))
	}

	// Add status filter for queued workflows
	queuedStatuses := make([]WorkflowStatusType, 0)
	if len(req.Body.Status) > 0 {
		for _, s := range req.Body.Status {
			status := WorkflowStatusType(s)
			if status != WorkflowStatusPending && status != WorkflowStatusEnqueued {
				c.logger.Warn("Received unexpected filtering status for listing queued workflows", "status", status)
			}
			queuedStatuses = append(queuedStatuses, status)
		}
	}
	if len(queuedStatuses) == 0 {
		queuedStatuses = []WorkflowStatusType{WorkflowStatusPending, WorkflowStatusEnqueued}
	}
	opts = append(opts, WithStatus(queuedStatuses))

	if req.Body.SortDesc {
		opts = append(opts, WithSortDesc())
	}
	if len(req.Body.WorkflowName) > 0 {
		opts = append(opts, WithName(req.Body.WorkflowName.toSlice()...))
	}
	if req.Body.Limit != nil {
		opts = append(opts, WithLimit(*req.Body.Limit))
	}
	if req.Body.Offset != nil {
		opts = append(opts, WithOffset(*req.Body.Offset))
	}
	if req.Body.StartTime != nil {
		opts = append(opts, WithStartTime(*req.Body.StartTime))
	}
	if req.Body.EndTime != nil {
		opts = append(opts, WithEndTime(*req.Body.EndTime))
	}
	if len(req.Body.QueueName) > 0 {
		opts = append(opts, WithQueueName(req.Body.QueueName.toSlice()...))
	}
	if len(req.Body.ExecutorID) > 0 {
		opts = append(opts, WithExecutorIDs(req.Body.ExecutorID.toSlice()))
	}
	if len(req.Body.WorkflowIDPrefix) > 0 {
		opts = append(opts, WithWorkflowIDPrefix(req.Body.WorkflowIDPrefix.toSlice()...))
	}
	if len(req.Body.ParentWorkflowID) > 0 {
		opts = append(opts, WithParentWorkflowID(req.Body.ParentWorkflowID.toSlice()...))
	}
	if len(req.Body.AuthenticatedUser) > 0 {
		opts = append(opts, WithUser(req.Body.AuthenticatedUser.toSlice()...))
	}
	if len(req.Body.ApplicationVersion) > 0 {
		opts = append(opts, WithAppVersion(req.Body.ApplicationVersion.toSlice()...))
	}

	workflows, err := c.dbosCtx.ListWorkflows(c.dbosCtx, opts...)
	if err != nil {
		c.logger.Error("Failed to list queued workflows", "error", err)
		errorMsg := fmt.Sprintf("failed to list queued workflows: %v", err)
		response := listWorkflowsConductorResponse{
			baseResponse: baseResponse{
				baseMessage: baseMessage{
					Type:      listQueuedWorkflowsMessage,
					RequestID: requestID,
				},
				ErrorMessage: &errorMsg,
			},
			Output: []listWorkflowsConductorResponseBody{},
		}
		return c.sendResponse(response, string(listQueuedWorkflowsMessage))
	}

	// Prepare response payload
	formattedWorkflows := make([]listWorkflowsConductorResponseBody, len(workflows))
	for i, wf := range workflows {
		formattedWorkflows[i] = formatListWorkflowsResponseBody(wf)
	}

	response := listWorkflowsConductorResponse{
		baseResponse: baseResponse{
			baseMessage: baseMessage{
				Type:      listQueuedWorkflowsMessage,
				RequestID: requestID,
			},
		},
		Output: formattedWorkflows,
	}

	return c.sendResponse(response, string(listQueuedWorkflowsMessage))
}

func (c *conductor) handleListStepsRequest(data []byte, requestID string) error {
	var req listStepsConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse list steps request", "error", err)
		return fmt.Errorf("failed to parse list steps request: %w", err)
	}
	c.logger.Debug("Handling list steps request", "request", req)

	// Get workflow steps using the public GetWorkflowSteps method
	steps, err := GetWorkflowSteps(c.dbosCtx, req.WorkflowID)
	if err != nil {
		c.logger.Error("Failed to list workflow steps", "workflow_id", req.WorkflowID, "error", err)
		errorMsg := fmt.Sprintf("failed to list workflow steps: %v", err)
		response := listStepsConductorResponse{
			baseResponse: baseResponse{
				baseMessage: baseMessage{
					Type:      listStepsMessage,
					RequestID: requestID,
				},
				ErrorMessage: &errorMsg,
			},
			Output: nil,
		}
		return c.sendResponse(response, string(listStepsMessage))
	}

	// Convert steps to response format
	var formattedSteps *[]workflowStepsConductorResponseBody
	if steps != nil {
		stepsList := make([]workflowStepsConductorResponseBody, len(steps))
		for i, step := range steps {
			stepsList[i] = formatWorkflowStepsResponseBody(step)
		}
		formattedSteps = &stepsList
	}

	response := listStepsConductorResponse{
		baseResponse: baseResponse{
			baseMessage: baseMessage{
				Type:      listStepsMessage,
				RequestID: requestID,
			},
		},
		Output: formattedSteps,
	}

	return c.sendResponse(response, string(listStepsMessage))
}

func (c *conductor) handleGetWorkflowRequest(data []byte, requestID string) error {
	var req getWorkflowConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse get workflow request", "error", err)
		return fmt.Errorf("failed to parse get workflow request: %w", err)
	}
	c.logger.Debug("Handling get workflow request", "workflow_id", req.WorkflowID)

	workflows, err := c.dbosCtx.ListWorkflows(c.dbosCtx, WithWorkflowIDs([]string{req.WorkflowID}))
	if err != nil {
		c.logger.Error("Failed to get workflow", "workflow_id", req.WorkflowID, "error", err)
		errorMsg := fmt.Sprintf("failed to get workflow: %v", err)
		response := getWorkflowConductorResponse{
			baseResponse: baseResponse{
				baseMessage: baseMessage{
					Type:      getWorkflowMessage,
					RequestID: requestID,
				},
				ErrorMessage: &errorMsg,
			},
			Output: nil,
		}
		return c.sendResponse(response, "get workflow response")
	}

	var formattedWorkflow *listWorkflowsConductorResponseBody
	if len(workflows) > 0 {
		formatted := formatListWorkflowsResponseBody(workflows[0])
		formattedWorkflow = &formatted
	}

	response := getWorkflowConductorResponse{
		baseResponse: baseResponse{
			baseMessage: baseMessage{
				Type:      getWorkflowMessage,
				RequestID: requestID,
			},
		},
		Output: formattedWorkflow,
	}

	return c.sendResponse(response, string(getWorkflowMessage))
}

func (c *conductor) handleForkWorkflowRequest(data []byte, requestID string) error {
	var req forkWorkflowConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse fork workflow request", "error", err)
		return fmt.Errorf("failed to parse fork workflow request: %w", err)
	}
	c.logger.Debug("Handling fork workflow request", "request", req)

	// Validate StartStep to prevent integer overflow
	if req.Body.StartStep < 0 {
		return fmt.Errorf("invalid StartStep: cannot be negative")
	}
	if req.Body.StartStep > math.MaxInt32/2 {
		return fmt.Errorf("invalid StartStep: cannot be greater than %d", math.MaxInt32/2)
	}
	input := ForkWorkflowInput{
		OriginalWorkflowID: req.Body.WorkflowID,
		StartStep:          uint(req.Body.StartStep), // #nosec G115 -- validated above
	}

	// Set optional fields
	if req.Body.NewWorkflowID != nil {
		input.ForkedWorkflowID = *req.Body.NewWorkflowID
	}
	if req.Body.ApplicationVersion != nil {
		input.ApplicationVersion = *req.Body.ApplicationVersion
	}

	// Execute the fork workflow
	handle, err := c.dbosCtx.ForkWorkflow(c.dbosCtx, input)
	var newWorkflowID *string
	var errorMsg *string

	if err != nil {
		c.logger.Error("Failed to fork workflow", "original_workflow_id", req.Body.WorkflowID, "error", err)
		errStr := fmt.Sprintf("failed to fork workflow: %v", err)
		errorMsg = &errStr
	} else {
		workflowID := handle.GetWorkflowID()
		newWorkflowID = &workflowID
		c.logger.Info("Successfully forked workflow", "original_workflow_id", req.Body.WorkflowID, "new_workflow_id", workflowID)
	}

	response := forkWorkflowConductorResponse{
		baseResponse: baseResponse{
			baseMessage: baseMessage{
				Type:      forkWorkflowMessage,
				RequestID: requestID,
			},
			ErrorMessage: errorMsg,
		},
		NewWorkflowID: newWorkflowID,
	}

	return c.sendResponse(response, string(forkWorkflowMessage))
}

func (c *conductor) handleExistPendingWorkflowsRequest(data []byte, requestID string) error {
	var req existPendingWorkflowsConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse exist pending workflows request", "error", err)
		return fmt.Errorf("failed to parse exist pending workflows request: %w", err)
	}
	c.logger.Debug("Handling exist pending workflows request", "executor_id", req.ExecutorID, "application_version", req.ApplicationVersion)

	opts := []ListWorkflowsOption{
		WithStatus([]WorkflowStatusType{WorkflowStatusPending}),
		WithLimit(1), // We only need to know if any exist, so limit to 1 for efficiency
		WithExecutorIDs([]string{req.ExecutorID}),
		WithAppVersion(req.ApplicationVersion),
	}

	workflows, err := c.dbosCtx.ListWorkflows(c.dbosCtx, opts...)
	var errorMsg *string
	if err != nil {
		c.logger.Error("Failed to check for pending workflows", "executor_id", req.ExecutorID, "application_version", req.ApplicationVersion, "error", err)
		errStr := fmt.Sprintf("failed to check for pending workflows: %v", err)
		errorMsg = &errStr
	}

	response := existPendingWorkflowsConductorResponse{
		baseResponse: baseResponse{
			baseMessage: baseMessage{
				Type:      existPendingWorkflowsMessage,
				RequestID: requestID,
			},
			ErrorMessage: errorMsg,
		},
		Exist: len(workflows) > 0,
	}

	return c.sendResponse(response, string(existPendingWorkflowsMessage))
}

func (c *conductor) handleUnknownMessageType(requestID string, msgType messageType, errorMsg string) error {
	if c.conn == nil {
		return fmt.Errorf("no connection")
	}

	response := baseResponse{
		baseMessage: baseMessage{
			Type:      msgType,
			RequestID: requestID,
		},
		ErrorMessage: &errorMsg,
	}

	return c.sendResponse(response, "unknown message type response")
}

func (c *conductor) sendResponse(response any, responseType string) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if c.conn == nil {
		return fmt.Errorf("no connection")
	}

	data, err := json.Marshal(response)
	if err != nil {
		return fmt.Errorf("failed to marshal %s: %w", responseType, err)
	}

	c.logger.Debug("Sending response", "type", responseType, "len", len(data))

	if err := c.conn.SetWriteDeadline(time.Now().Add(_WRITE_DEADLINE)); err != nil {
		c.logger.Warn("Failed to set write deadline", "type", responseType, "error", err)
	}
	if err := c.conn.WriteMessage(websocket.TextMessage, data); err != nil {
		c.logger.Error("Failed to send response", "type", responseType, "error", err)
		return fmt.Errorf("failed to send message: %w", err)
	}
	if err := c.conn.SetWriteDeadline(time.Time{}); err != nil {
		c.logger.Warn("Failed to clear write deadline", "type", responseType, "error", err)
	}

	return nil
}
