package conductor

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/rand/v2"
	"net"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/models"
	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/sysdb"

	"github.com/gorilla/websocket"
)

// Executor is the narrow surface of the DBOS runtime the conductor needs.
// It is implemented by an adapter in the dbos package.
type Executor interface {
	// SystemDB exposes the system database for read/admin operations.
	SystemDB() sysdb.SystemDatabase
	GetExecutorID() string
	GetApplicationVersion() string
	// AlertHandler returns the user-registered alert handler, or nil.
	AlertHandler() models.AlertHandler
	// DecodeStoredValue deserializes a stored value using its recorded
	// serialization format and re-marshals it as plain JSON.
	DecodeStoredValue(ctx context.Context, value, serialization string) (string, error)
	// RecoverPendingWorkflows returns the IDs of the recovered workflows.
	RecoverPendingWorkflows(ctx context.Context, executorIDs []string) ([]string, error)
	ListWorkflows(ctx context.Context, opts ...models.ListWorkflowsOption) ([]models.WorkflowStatus, error)
	GetWorkflowSteps(ctx context.Context, workflowID string, opts ...models.GetWorkflowStepsOption) ([]models.StepInfo, error)
	CancelWorkflows(ctx context.Context, workflowIDs []string, opts ...models.CancelWorkflowOptions) error
	ResumeWorkflows(ctx context.Context, workflowIDs []string, opts ...models.ResumeWorkflowOption) error
	// ForkWorkflow returns the ID of the newly forked workflow.
	ForkWorkflow(ctx context.Context, input models.ForkWorkflowInput) (string, error)
	GetWorkflowAggregates(ctx context.Context, input models.GetWorkflowAggregatesInput) ([]sysdb.WorkflowAggregateRow, error)
	GetStepAggregates(ctx context.Context, input models.GetStepAggregatesInput) ([]sysdb.StepAggregateRow, error)
	ListSchedules(ctx context.Context, opts ...models.ListSchedulesOption) ([]models.WorkflowSchedule, error)
	GetSchedule(ctx context.Context, scheduleName string) (*models.WorkflowSchedule, error)
	PauseSchedule(ctx context.Context, scheduleName string) error
	ResumeSchedule(ctx context.Context, scheduleName string) error
	ListQueues(ctx context.Context) ([]models.QueueConfig, error)
	RetrieveQueue(ctx context.Context, name string) (*models.QueueConfig, error)
}

const (
	_PING_INTERVAL          = 20 * time.Second
	_PING_TIMEOUT           = 30 * time.Second // Should be slightly greater than server's executorPingWait (25s)
	_INITIAL_RECONNECT_WAIT = 1 * time.Second
	_MAX_RECONNECT_WAIT     = 30 * time.Second
	_HANDSHAKE_TIMEOUT      = 10 * time.Second
	_WRITE_DEADLINE         = 5 * time.Second
)

// Config contains configuration for the conductor
// Config configures the conductor connection.
type Config struct {
	URL              string
	APIKey           string
	AppName          string
	ExecutorMetadata map[string]any
}

// conductor manages the WebSocket connection to the DBOS conductor service
type Conductor struct {
	ctx    context.Context
	exec   Executor
	logger *slog.Logger

	// Connection management
	conn           *websocket.Conn
	needsReconnect atomic.Bool
	wg             sync.WaitGroup
	stopOnce       sync.Once
	writeMu        sync.Mutex // writeMu protects concurrent writes to the WebSocket connection (pings + handling messages)

	// Connection parameters
	url           url.URL
	PingInterval  time.Duration
	PingTimeout   time.Duration
	ReconnectWait time.Duration

	// User-defined metadata for this executor
	executorMetadata map[string]any

	// pingCancel cancels the ping goroutine context
	pingCancel context.CancelFunc
}

// launch starts the conductor main goroutine
func (c *Conductor) Launch() {
	c.logger.Info("Launching conductor")
	c.wg.Add(1)
	go c.run()
}

func New(ctx context.Context, exec Executor, logger *slog.Logger, config Config) (*Conductor, error) {
	if config.APIKey == "" {
		return nil, fmt.Errorf("conductor API key is required")
	}
	if config.URL == "" {
		return nil, fmt.Errorf("conductor URL is required")
	}

	baseURL, err := url.Parse(config.URL)
	if err != nil {
		return nil, fmt.Errorf("invalid conductor URL: %w", err)
	}

	wsURL := url.URL{
		Scheme: baseURL.Scheme,
		Host:   baseURL.Host,
		Path:   baseURL.JoinPath("websocket", config.AppName, config.APIKey).Path,
	}

	c := &Conductor{
		ctx:              ctx,
		exec:             exec,
		url:              wsURL,
		PingInterval:     _PING_INTERVAL,
		PingTimeout:      _PING_TIMEOUT,
		ReconnectWait:    _INITIAL_RECONNECT_WAIT,
		logger:           logger.With("service", "conductor"),
		executorMetadata: config.ExecutorMetadata,
	}

	// Start with needsReconnect set to true so we connect on first run
	c.needsReconnect.Store(true)

	return c, nil
}

func (c *Conductor) Shutdown(timeout time.Duration) {
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
func (c *Conductor) reconnectWaitWithJitter() time.Duration {
	// Add jitter: random value between 0.5 * wait and 1.5 * wait
	jitter := 0.5 + rand.Float64() // #nosec G404 -- jitter for backoff doesn't need crypto-secure randomness
	return time.Duration(float64(c.ReconnectWait) * jitter)
}

// closeConn closes the connection and signals that reconnection is needed
func (c *Conductor) closeConn() {
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

func (c *Conductor) run() {
	defer c.wg.Done()

	for {
		// Check if the context has been cancelled
		select {
		case <-c.ctx.Done():
			c.logger.Info("DBOS context done, stopping conductor", "cause", context.Cause(c.ctx))
			c.closeConn()
			return
		default:
		}

		// Connect if reconnection is needed
		if c.needsReconnect.Load() {
			if err := c.connect(); err != nil {
				c.logger.Warn("Failed to connect to conductor", "error", err)
				select {
				case <-c.ctx.Done():
					c.logger.Info("DBOS context done, stopping conductor", "cause", context.Cause(c.ctx))
					return
				case <-time.After(c.reconnectWaitWithJitter()):
					// Exponential backoff with jitter up to max wait
					if c.ReconnectWait < _MAX_RECONNECT_WAIT {
						c.ReconnectWait *= 2
						if c.ReconnectWait > _MAX_RECONNECT_WAIT {
							c.ReconnectWait = _MAX_RECONNECT_WAIT
						}
					}
					continue
				}
			}
			// Reset reconnect wait and clear reconnect flag on successful connection
			c.ReconnectWait = _INITIAL_RECONNECT_WAIT
			c.needsReconnect.Store(false)
		}

		// This shouldn't happen but check anyway
		if c.conn == nil {
			c.needsReconnect.Store(true)
			continue
		}

		// Read message (will timeout based on read deadline set in connect)
		MessageType, message, err := c.conn.ReadMessage()
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
		if MessageType != websocket.TextMessage {
			c.logger.Warn("Received unexpected message type, forcing reconnection", "type", MessageType)
			c.closeConn()
			continue
		}

		ht := time.Now()
		if err := c.handleMessage(message); err != nil {
			c.logger.Error("Failed to handle message", "error", err)
		}
		c.logger.Debug("Handled message", "message", MessageType, "latency_us", time.Since(ht).Microseconds())
	}
}

func (c *Conductor) connect() error {
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
	if err := conn.SetReadDeadline(time.Now().Add(c.PingTimeout)); err != nil {
		cErr := conn.Close()
		if cErr != nil {
			c.logger.Warn("Failed to close connection", "error", cErr)
		}
		return fmt.Errorf("failed to set read deadline: %w", err)
	}

	// Set pong handler to reset read deadline
	conn.SetPongHandler(func(appData string) error {
		c.logger.Debug("Received pong from conductor")
		return conn.SetReadDeadline(time.Now().Add(c.PingTimeout))
	})

	// Store the connection
	c.conn = conn

	// Create a cancellable context for the ping goroutine
	pingCtx, pingCancel := context.WithCancel(c.ctx)
	c.pingCancel = pingCancel

	// Start ping goroutine
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ticker := time.NewTicker(c.PingInterval)
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

func (c *Conductor) ping() error {
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

func (c *Conductor) handleMessage(data []byte) error {
	var base BaseMessage
	if err := json.Unmarshal(data, &base); err != nil {
		c.logger.Error("Failed to parse message", "error", err)
		return fmt.Errorf("failed to parse base message: %w", err)
	}
	c.logger.Debug("Received message", "type", base.Type, "request_id", base.RequestID)

	switch base.Type {
	case ExecutorInfo:
		return c.handleExecutorInfoRequest(data, base.RequestID)
	case RecoveryMessage:
		return c.handleRecoveryRequest(data, base.RequestID)
	case CancelWorkflowMessage:
		return c.handleCancelWorkflowRequest(data, base.RequestID)
	case ResumeWorkflowMessage:
		return c.handleResumeWorkflowRequest(data, base.RequestID)
	case ListWorkflowsMessage:
		return c.handleListWorkflowsRequest(data, base.RequestID)
	case ListQueuedWorkflowsMessage:
		return c.handleListQueuedWorkflowsRequest(data, base.RequestID)
	case ListStepsMessage:
		return c.handleListStepsRequest(data, base.RequestID)
	case GetWorkflowMessage:
		return c.handleGetWorkflowRequest(data, base.RequestID)
	case ForkWorkflowMessage:
		return c.handleForkWorkflowRequest(data, base.RequestID)
	case ForkFromFailureMessage:
		return c.handleForkFromFailureRequest(data, base.RequestID)
	case ExistPendingWorkflowsMessage:
		return c.handleExistPendingWorkflowsRequest(data, base.RequestID)
	case RetentionMessage:
		return c.handleRetentionRequest(data, base.RequestID)
	case GetMetricsMessage:
		return c.handleGetMetricsRequest(data, base.RequestID)
	case ExportWorkflowMessage:
		return c.handleExportWorkflowRequest(data, base.RequestID)
	case ImportWorkflowMessage:
		return c.handleImportWorkflowRequest(data, base.RequestID)
	case DeleteWorkflowMessage:
		return c.handleDeleteWorkflowRequest(data, base.RequestID)
	case AlertMessage:
		return c.handleAlertRequest(data, base.RequestID)
	case ListSchedulesMessage:
		return c.handleListSchedulesRequest(data, base.RequestID)
	case GetScheduleMessage:
		return c.handleGetScheduleRequest(data, base.RequestID)
	case PauseScheduleMessage:
		return c.handlePauseScheduleRequest(data, base.RequestID)
	case ResumeScheduleMessage:
		return c.handleResumeScheduleRequest(data, base.RequestID)
	case BackfillScheduleMessage:
		return c.handleBackfillScheduleRequest(data, base.RequestID)
	case TriggerScheduleMessage:
		return c.handleTriggerScheduleRequest(data, base.RequestID)
	case GetWorkflowEventsMessage:
		return c.handleGetWorkflowEventsRequest(data, base.RequestID)
	case GetWorkflowNotificationsMsg:
		return c.handleGetWorkflowNotificationsRequest(data, base.RequestID)
	case GetWorkflowStreamsMessage:
		return c.handleGetWorkflowStreamsRequest(data, base.RequestID)
	case GetWorkflowAggregatesMessage:
		return c.handleGetWorkflowAggregatesRequest(data, base.RequestID)
	case GetStepAggregatesMessage:
		return c.handleGetStepAggregatesRequest(data, base.RequestID)
	case ListAppVersionsMessage:
		return c.handleListApplicationVersionsRequest(data, base.RequestID)
	case SetLatestAppVersionMessage:
		return c.handleSetLatestApplicationVersionRequest(data, base.RequestID)
	case ListQueuesMessage:
		return c.handleListQueuesRequest(data, base.RequestID)
	case GetQueueMessage:
		return c.handleGetQueueRequest(data, base.RequestID)
	default:
		c.logger.Warn("Unknown message type", "type", base.Type)
		return c.handleUnknownMessageType(base.RequestID, base.Type, "Unknown message type")
	}
}

func (c *Conductor) handleExecutorInfoRequest(data []byte, requestID string) error {
	var req ExecutorInfoRequest
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

	response := ExecutorInfoResponse{
		BaseResponse: BaseResponse{
			BaseMessage: BaseMessage{
				Type:      ExecutorInfo,
				RequestID: requestID,
			},
		},
		ExecutorID:         c.exec.GetExecutorID(),
		ApplicationVersion: c.exec.GetApplicationVersion(),
		Hostname:           &hostname,
		DBOSVersion:        models.DBOSVersion(),
		Language:           "go",
		ExecutorMetadata:   c.executorMetadata,
	}

	return c.sendResponse(response, string(ExecutorInfo))
}

func (c *Conductor) handleRecoveryRequest(data []byte, requestID string) error {
	var req RecoveryConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse recovery request", "error", err)
		return fmt.Errorf("failed to parse recovery request: %w", err)
	}
	c.logger.Debug("Handling recovery request", "executor_ids", req.ExecutorIDs, "request_id", requestID)

	success := true
	var errorMsg *string

	_, err := c.exec.RecoverPendingWorkflows(c.ctx, req.ExecutorIDs)
	if err != nil {
		c.logger.Error("Failed to recover pending workflows", "executor_ids", req.ExecutorIDs, "error", err)
		errStr := fmt.Sprintf("failed to recover pending workflows: %v", err)
		errorMsg = &errStr
		success = false
	} else {
		c.logger.Info("Successfully recovered pending workflows", "executor_ids", req.ExecutorIDs)
	}

	response := RecoveryConductorResponse{
		BaseResponse: BaseResponse{
			BaseMessage: BaseMessage{
				Type:      RecoveryMessage,
				RequestID: requestID,
			},
			ErrorMessage: errorMsg,
		},
		Success: success,
	}

	return c.sendResponse(response, string(RecoveryMessage))
}

func (c *Conductor) handleCancelWorkflowRequest(data []byte, requestID string) error {
	var req CancelWorkflowConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse cancel workflow request", "error", err)
		return fmt.Errorf("failed to parse cancel workflow request: %w", err)
	}
	workflowIDs := req.WorkflowIDs
	if len(workflowIDs) == 0 && req.WorkflowID != "" {
		workflowIDs = []string{req.WorkflowID}
	}
	c.logger.Debug("Handling cancel workflow request", "workflow_ids", workflowIDs, "request_id", requestID)

	success := true
	var errorMsg *string

	opts := []models.CancelWorkflowOptions{}
	if req.CancelChildren {
		opts = append(opts, models.WithCancelChildren())
	}

	if err := c.exec.CancelWorkflows(c.ctx, workflowIDs, opts...); err != nil {
		c.logger.Error("Failed to cancel workflows", "workflow_ids", workflowIDs, "error", err)
		errStr := fmt.Sprintf("failed to cancel workflows: %v", err)
		errorMsg = &errStr
		success = false
	} else {
		c.logger.Info("Successfully cancelled workflows", "workflow_ids", workflowIDs)
	}

	response := CancelWorkflowConductorResponse{
		BaseResponse: BaseResponse{
			BaseMessage: BaseMessage{
				Type:      CancelWorkflowMessage,
				RequestID: requestID,
			},
			ErrorMessage: errorMsg,
		},
		Success: success,
	}

	return c.sendResponse(response, string(CancelWorkflowMessage))
}

func (c *Conductor) handleResumeWorkflowRequest(data []byte, requestID string) error {
	var req ResumeWorkflowConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse resume workflow request", "error", err)
		return fmt.Errorf("failed to parse resume workflow request: %w", err)
	}
	workflowIDs := req.WorkflowIDs
	if len(workflowIDs) == 0 && req.WorkflowID != "" {
		workflowIDs = []string{req.WorkflowID}
	}
	c.logger.Debug("Handling resume workflow request", "workflow_ids", workflowIDs, "request_id", requestID)

	success := true
	var errorMsg *string

	var resumeOpts []models.ResumeWorkflowOption
	if req.QueueName != nil {
		resumeOpts = append(resumeOpts, models.WithResumeQueue(*req.QueueName))
	}
	err := c.exec.ResumeWorkflows(c.ctx, workflowIDs, resumeOpts...)
	if err != nil {
		c.logger.Error("Failed to resume workflows", "workflow_ids", workflowIDs, "error", err)
		errStr := fmt.Sprintf("failed to resume workflows: %v", err)
		errorMsg = &errStr
		success = false
	} else {
		c.logger.Info("Successfully resumed workflows", "workflow_ids", workflowIDs)
	}

	response := ResumeWorkflowConductorResponse{
		BaseResponse: BaseResponse{
			BaseMessage: BaseMessage{
				Type:      ResumeWorkflowMessage,
				RequestID: requestID,
			},
			ErrorMessage: errorMsg,
		},
		Success: success,
	}

	return c.sendResponse(response, string(ResumeWorkflowMessage))
}

func (c *Conductor) handleRetentionRequest(data []byte, requestID string) error {
	var req RetentionConductorRequest
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

		input := sysdb.GarbageCollectWorkflowsInput{
			CutoffEpochTimestampMs: cutoffMs,
			RowsThreshold:          rowsThreshold,
		}

		err := sysdb.Retry(c.ctx, func() error {
			return c.exec.SystemDB().GarbageCollectWorkflows(c.ctx, input)
		}, sysdb.WithRetrierLogger(c.logger))
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
		err := sysdb.Retry(c.ctx, func() error {
			return c.exec.SystemDB().CancelAllBefore(c.ctx, cutoffTime)
		}, sysdb.WithRetrierLogger(c.logger))
		if err != nil {
			c.logger.Error("Failed to timeout workflows", "cutoff_ms", *req.Body.TimeoutCutoffEpochMs, "error", err)
			errStr := fmt.Sprintf("failed to timeout workflows: %v", err)
			errorMsg = &errStr
			success = false
		} else {
			c.logger.Info("Successfully timed out workflows", "cutoff_ms", *req.Body.TimeoutCutoffEpochMs)
		}
	}

	response := RetentionConductorResponse{
		BaseResponse: BaseResponse{
			BaseMessage: BaseMessage{
				Type:      RetentionMessage,
				RequestID: requestID,
			},
			ErrorMessage: errorMsg,
		},
		Success: success,
	}

	return c.sendResponse(response, string(RetentionMessage))
}

func (c *Conductor) handleGetMetricsRequest(data []byte, requestID string) error {
	var req GetMetricsConductorRequest
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
	var metricsData []sysdb.MetricData

	if req.MetricClass == "workflow_step_count" {
		var err error
		metricsData, err = sysdb.RetryWithResult(c.ctx, func() ([]sysdb.MetricData, error) {
			return c.exec.SystemDB().GetMetrics(c.ctx, req.StartTime, req.EndTime)
		}, sysdb.WithRetrierLogger(c.logger))
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

	response := GetMetricsConductorResponse{
		BaseResponse: BaseResponse{
			BaseMessage: BaseMessage{
				Type:      GetMetricsMessage,
				RequestID: requestID,
			},
			ErrorMessage: errorMsg,
		},
		Metrics: metricsData,
	}

	return c.sendResponse(response, string(GetMetricsMessage))
}

func (c *Conductor) handleListWorkflowsRequest(data []byte, requestID string) error {
	var req ListWorkflowsConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse list workflows request", "error", err)
		return fmt.Errorf("failed to parse list workflows request: %w", err)
	}
	c.logger.Debug("Handling list workflows request", "request", req)

	var opts []models.ListWorkflowsOption
	opts = append(opts, models.WithLoadInput(req.Body.LoadInput))
	opts = append(opts, models.WithLoadOutput(req.Body.LoadOutput))
	if req.Body.SortDesc {
		opts = append(opts, models.WithSortDesc())
	}
	if req.Body.QueuesOnly {
		opts = append(opts, models.WithQueuesOnly())
	}
	if len(req.Body.WorkflowUUIDs) > 0 {
		opts = append(opts, models.WithWorkflowIDs(req.Body.WorkflowUUIDs))
	}
	if len(req.Body.WorkflowName) > 0 {
		opts = append(opts, models.WithName(req.Body.WorkflowName.toSlice()...))
	}
	if len(req.Body.AuthenticatedUser) > 0 {
		opts = append(opts, models.WithUser(req.Body.AuthenticatedUser.toSlice()...))
	}
	if len(req.Body.ApplicationVersion) > 0 {
		opts = append(opts, models.WithAppVersion(req.Body.ApplicationVersion.toSlice()...))
	}
	if req.Body.Limit != nil {
		opts = append(opts, models.WithLimit(*req.Body.Limit))
	}
	if req.Body.Offset != nil {
		opts = append(opts, models.WithOffset(*req.Body.Offset))
	}
	if req.Body.StartTime != nil {
		opts = append(opts, models.WithStartTime(*req.Body.StartTime))
	}
	if req.Body.EndTime != nil {
		opts = append(opts, models.WithEndTime(*req.Body.EndTime))
	}
	if req.Body.CompletedAfter != nil {
		opts = append(opts, models.WithCompletedAfter(*req.Body.CompletedAfter))
	}
	if req.Body.CompletedBefore != nil {
		opts = append(opts, models.WithCompletedBefore(*req.Body.CompletedBefore))
	}
	if req.Body.DequeuedAfter != nil {
		opts = append(opts, models.WithDequeuedAfter(*req.Body.DequeuedAfter))
	}
	if req.Body.DequeuedBefore != nil {
		opts = append(opts, models.WithDequeuedBefore(*req.Body.DequeuedBefore))
	}
	if len(req.Body.Status) > 0 {
		statuses := make([]models.WorkflowStatusType, len(req.Body.Status))
		for i, s := range req.Body.Status {
			statuses[i] = models.WorkflowStatusType(s)
		}
		opts = append(opts, models.WithStatus(statuses))
	}
	if len(req.Body.ForkedFrom) > 0 {
		opts = append(opts, models.WithForkedFrom(req.Body.ForkedFrom.toSlice()...))
	}
	if len(req.Body.ParentWorkflowID) > 0 {
		opts = append(opts, models.WithParentWorkflowID(req.Body.ParentWorkflowID.toSlice()...))
	}
	if req.Body.WasForkedFrom != nil {
		opts = append(opts, models.WithWasForkedFrom(*req.Body.WasForkedFrom))
	}
	if req.Body.HasParent != nil {
		opts = append(opts, models.WithHasParent(*req.Body.HasParent))
	}
	if len(req.Body.QueueName) > 0 {
		opts = append(opts, models.WithQueueName(req.Body.QueueName.toSlice()...))
	}
	if len(req.Body.WorkflowIDPrefix) > 0 {
		opts = append(opts, models.WithWorkflowIDPrefix(req.Body.WorkflowIDPrefix.toSlice()...))
	}
	if len(req.Body.ExecutorID) > 0 {
		opts = append(opts, models.WithExecutorIDs(req.Body.ExecutorID.toSlice()))
	}
	if len(req.Body.Attributes) > 0 {
		opts = append(opts, models.WithFilterAttributes(req.Body.Attributes))
	}
	if len(req.Body.ScheduleName) > 0 {
		opts = append(opts, models.WithFilterScheduleName(req.Body.ScheduleName.toSlice()...))
	}

	workflows, err := c.exec.ListWorkflows(c.ctx, opts...)
	if err != nil {
		c.logger.Error("Failed to list workflows", "error", err)
		errorMsg := fmt.Sprintf("failed to list workflows: %v", err)
		response := ListWorkflowsConductorResponse{
			BaseResponse: BaseResponse{
				BaseMessage: BaseMessage{
					Type:      ListWorkflowsMessage,
					RequestID: requestID,
				},
				ErrorMessage: &errorMsg,
			},
			Output: []ListWorkflowsConductorResponseBody{},
		}
		return c.sendResponse(response, "list workflows response")
	}

	formattedWorkflows := make([]ListWorkflowsConductorResponseBody, len(workflows))
	for i, wf := range workflows {
		formattedWorkflows[i] = formatListWorkflowsResponseBody(wf)
	}

	response := ListWorkflowsConductorResponse{
		BaseResponse: BaseResponse{
			BaseMessage: BaseMessage{
				Type:      ListWorkflowsMessage,
				RequestID: requestID,
			},
		},
		Output: formattedWorkflows,
	}

	return c.sendResponse(response, string(ListWorkflowsMessage))
}

func (c *Conductor) handleListQueuedWorkflowsRequest(data []byte, requestID string) error {
	var req ListWorkflowsConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse list queued workflows request", "error", err)
		return fmt.Errorf("failed to parse list queued workflows request: %w", err)
	}
	c.logger.Debug("Handling list queued workflows request", "request", req)

	// Build functional options for ListWorkflows
	var opts []models.ListWorkflowsOption
	opts = append(opts, models.WithLoadInput(req.Body.LoadInput))
	opts = append(opts, models.WithLoadOutput(false)) // Don't load output for queued workflows
	opts = append(opts, models.WithQueuesOnly())      // Only include workflows that are in queues
	if len(req.Body.WorkflowUUIDs) > 0 {
		opts = append(opts, models.WithWorkflowIDs(req.Body.WorkflowUUIDs))
	}

	// Add status filter for queued workflows
	queuedStatuses := make([]models.WorkflowStatusType, 0)
	if len(req.Body.Status) > 0 {
		for _, s := range req.Body.Status {
			status := models.WorkflowStatusType(s)
			if status != models.WorkflowStatusPending && status != models.WorkflowStatusEnqueued && status != models.WorkflowStatusDelayed {
				c.logger.Warn("Received unexpected filtering status for listing queued workflows", "status", status)
			}
			queuedStatuses = append(queuedStatuses, status)
		}
	}
	if len(queuedStatuses) == 0 {
		queuedStatuses = []models.WorkflowStatusType{models.WorkflowStatusPending, models.WorkflowStatusEnqueued, models.WorkflowStatusDelayed}
	}
	opts = append(opts, models.WithStatus(queuedStatuses))

	if req.Body.SortDesc {
		opts = append(opts, models.WithSortDesc())
	}
	if len(req.Body.WorkflowName) > 0 {
		opts = append(opts, models.WithName(req.Body.WorkflowName.toSlice()...))
	}
	if req.Body.Limit != nil {
		opts = append(opts, models.WithLimit(*req.Body.Limit))
	}
	if req.Body.Offset != nil {
		opts = append(opts, models.WithOffset(*req.Body.Offset))
	}
	if req.Body.StartTime != nil {
		opts = append(opts, models.WithStartTime(*req.Body.StartTime))
	}
	if req.Body.EndTime != nil {
		opts = append(opts, models.WithEndTime(*req.Body.EndTime))
	}
	if req.Body.CompletedAfter != nil {
		opts = append(opts, models.WithCompletedAfter(*req.Body.CompletedAfter))
	}
	if req.Body.CompletedBefore != nil {
		opts = append(opts, models.WithCompletedBefore(*req.Body.CompletedBefore))
	}
	if req.Body.DequeuedAfter != nil {
		opts = append(opts, models.WithDequeuedAfter(*req.Body.DequeuedAfter))
	}
	if req.Body.DequeuedBefore != nil {
		opts = append(opts, models.WithDequeuedBefore(*req.Body.DequeuedBefore))
	}
	if len(req.Body.QueueName) > 0 {
		opts = append(opts, models.WithQueueName(req.Body.QueueName.toSlice()...))
	}
	if len(req.Body.ExecutorID) > 0 {
		opts = append(opts, models.WithExecutorIDs(req.Body.ExecutorID.toSlice()))
	}
	if len(req.Body.WorkflowIDPrefix) > 0 {
		opts = append(opts, models.WithWorkflowIDPrefix(req.Body.WorkflowIDPrefix.toSlice()...))
	}
	if len(req.Body.ForkedFrom) > 0 {
		opts = append(opts, models.WithForkedFrom(req.Body.ForkedFrom.toSlice()...))
	}
	if len(req.Body.ParentWorkflowID) > 0 {
		opts = append(opts, models.WithParentWorkflowID(req.Body.ParentWorkflowID.toSlice()...))
	}
	if req.Body.WasForkedFrom != nil {
		opts = append(opts, models.WithWasForkedFrom(*req.Body.WasForkedFrom))
	}
	if req.Body.HasParent != nil {
		opts = append(opts, models.WithHasParent(*req.Body.HasParent))
	}
	if len(req.Body.AuthenticatedUser) > 0 {
		opts = append(opts, models.WithUser(req.Body.AuthenticatedUser.toSlice()...))
	}
	if len(req.Body.ApplicationVersion) > 0 {
		opts = append(opts, models.WithAppVersion(req.Body.ApplicationVersion.toSlice()...))
	}
	if len(req.Body.Attributes) > 0 {
		opts = append(opts, models.WithFilterAttributes(req.Body.Attributes))
	}
	if len(req.Body.ScheduleName) > 0 {
		opts = append(opts, models.WithFilterScheduleName(req.Body.ScheduleName.toSlice()...))
	}

	workflows, err := c.exec.ListWorkflows(c.ctx, opts...)
	if err != nil {
		c.logger.Error("Failed to list queued workflows", "error", err)
		errorMsg := fmt.Sprintf("failed to list queued workflows: %v", err)
		response := ListWorkflowsConductorResponse{
			BaseResponse: BaseResponse{
				BaseMessage: BaseMessage{
					Type:      ListQueuedWorkflowsMessage,
					RequestID: requestID,
				},
				ErrorMessage: &errorMsg,
			},
			Output: []ListWorkflowsConductorResponseBody{},
		}
		return c.sendResponse(response, string(ListQueuedWorkflowsMessage))
	}

	// Prepare response payload
	formattedWorkflows := make([]ListWorkflowsConductorResponseBody, len(workflows))
	for i, wf := range workflows {
		formattedWorkflows[i] = formatListWorkflowsResponseBody(wf)
	}

	response := ListWorkflowsConductorResponse{
		BaseResponse: BaseResponse{
			BaseMessage: BaseMessage{
				Type:      ListQueuedWorkflowsMessage,
				RequestID: requestID,
			},
		},
		Output: formattedWorkflows,
	}

	return c.sendResponse(response, string(ListQueuedWorkflowsMessage))
}

func (c *Conductor) handleListStepsRequest(data []byte, requestID string) error {
	var req ListStepsConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse list steps request", "error", err)
		return fmt.Errorf("failed to parse list steps request: %w", err)
	}
	c.logger.Debug("Handling list steps request", "request", req)

	// Get workflow steps using the public GetWorkflowSteps method
	stepOpts := []models.GetWorkflowStepsOption{models.WithStepsLoadOutput(req.LoadOutput)}
	if req.Limit != nil {
		stepOpts = append(stepOpts, models.WithStepsLimit(*req.Limit))
	}
	if req.Offset != nil {
		stepOpts = append(stepOpts, models.WithStepsOffset(*req.Offset))
	}
	steps, err := c.exec.GetWorkflowSteps(c.ctx, req.WorkflowID, stepOpts...)
	if err != nil {
		c.logger.Error("Failed to list workflow steps", "workflow_id", req.WorkflowID, "error", err)
		errorMsg := fmt.Sprintf("failed to list workflow steps: %v", err)
		response := ListStepsConductorResponse{
			BaseResponse: BaseResponse{
				BaseMessage: BaseMessage{
					Type:      ListStepsMessage,
					RequestID: requestID,
				},
				ErrorMessage: &errorMsg,
			},
			Output: nil,
		}
		return c.sendResponse(response, string(ListStepsMessage))
	}

	// Convert steps to response format
	var formattedSteps *[]WorkflowStepsConductorResponseBody
	if steps != nil {
		stepsList := make([]WorkflowStepsConductorResponseBody, len(steps))
		for i, step := range steps {
			stepsList[i] = formatWorkflowStepsResponseBody(step)
		}
		formattedSteps = &stepsList
	}

	response := ListStepsConductorResponse{
		BaseResponse: BaseResponse{
			BaseMessage: BaseMessage{
				Type:      ListStepsMessage,
				RequestID: requestID,
			},
		},
		Output: formattedSteps,
	}

	return c.sendResponse(response, string(ListStepsMessage))
}

func (c *Conductor) handleGetWorkflowRequest(data []byte, requestID string) error {
	var req GetWorkflowConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse get workflow request", "error", err)
		return fmt.Errorf("failed to parse get workflow request: %w", err)
	}
	c.logger.Debug("Handling get workflow request", "workflow_id", req.WorkflowID)

	workflows, err := c.exec.ListWorkflows(c.ctx, models.WithWorkflowIDs([]string{req.WorkflowID}),
		models.WithLoadInput(req.LoadInput),
		models.WithLoadOutput(req.LoadOutput))
	if err != nil {
		c.logger.Error("Failed to get workflow", "workflow_id", req.WorkflowID, "error", err)
		errorMsg := fmt.Sprintf("failed to get workflow: %v", err)
		response := GetWorkflowConductorResponse{
			BaseResponse: BaseResponse{
				BaseMessage: BaseMessage{
					Type:      GetWorkflowMessage,
					RequestID: requestID,
				},
				ErrorMessage: &errorMsg,
			},
			Output: nil,
		}
		return c.sendResponse(response, "get workflow response")
	}

	var formattedWorkflow *ListWorkflowsConductorResponseBody
	if len(workflows) > 0 {
		formatted := formatListWorkflowsResponseBody(workflows[0])
		formattedWorkflow = &formatted
	}

	response := GetWorkflowConductorResponse{
		BaseResponse: BaseResponse{
			BaseMessage: BaseMessage{
				Type:      GetWorkflowMessage,
				RequestID: requestID,
			},
		},
		Output: formattedWorkflow,
	}

	return c.sendResponse(response, string(GetWorkflowMessage))
}

func (c *Conductor) handleForkWorkflowRequest(data []byte, requestID string) error {
	var req ForkWorkflowConductorRequest
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
	input := models.ForkWorkflowInput{
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
	if req.Body.QueueName != nil {
		input.QueueName = *req.Body.QueueName
	}
	if req.Body.QueuePartitionKey != nil {
		input.QueuePartitionKey = *req.Body.QueuePartitionKey
	}

	// Execute the fork workflow
	forkedID, err := c.exec.ForkWorkflow(c.ctx, input)
	var newWorkflowID *string
	var errorMsg *string

	if err != nil {
		c.logger.Error("Failed to fork workflow", "original_workflow_id", req.Body.WorkflowID, "error", err)
		errStr := fmt.Sprintf("failed to fork workflow: %v", err)
		errorMsg = &errStr
	} else {
		workflowID := forkedID
		newWorkflowID = &workflowID
		c.logger.Info("Successfully forked workflow", "original_workflow_id", req.Body.WorkflowID, "new_workflow_id", workflowID)
	}

	response := ForkWorkflowConductorResponse{
		BaseResponse: BaseResponse{
			BaseMessage: BaseMessage{
				Type:      ForkWorkflowMessage,
				RequestID: requestID,
			},
			ErrorMessage: errorMsg,
		},
		NewWorkflowID: newWorkflowID,
	}

	return c.sendResponse(response, string(ForkWorkflowMessage))
}

func (c *Conductor) handleForkFromFailureRequest(data []byte, requestID string) error {
	var req ForkFromFailureConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse fork from failure request", "error", err)
		return fmt.Errorf("failed to parse fork from failure request: %w", err)
	}
	c.logger.Debug("Handling fork from failure request", "request", req)

	input := sysdb.ForkFromDBInput{
		WorkflowIDs:     req.Body.WorkflowIDs,
		FromLastFailure: req.Body.FromLastFailure,
		FromLastStep:    req.Body.FromLastStep,
		FromStep:        req.Body.FromStep,
		FromStepName:    req.Body.FromStepName,
	}
	if req.Body.ApplicationVersion != nil {
		input.ApplicationVersion = *req.Body.ApplicationVersion
	}
	if req.Body.QueueName != nil {
		input.QueueName = *req.Body.QueueName
	}
	if req.Body.QueuePartitionKey != nil {
		input.QueuePartitionKey = *req.Body.QueuePartitionKey
	}

	forkedIDs, err := c.exec.SystemDB().ForkFrom(c.ctx, input)
	var errorMsg *string
	if err != nil {
		c.logger.Error("Failed to fork workflows from failure", "workflow_ids", req.Body.WorkflowIDs, "error", err)
		errStr := fmt.Sprintf("failed to fork workflows from failure: %v", err)
		errorMsg = &errStr
	} else {
		c.logger.Info("Successfully forked workflows from failure", "original_workflow_ids", req.Body.WorkflowIDs, "forked_workflow_ids", forkedIDs)
	}

	response := ForkFromFailureConductorResponse{
		BaseResponse: BaseResponse{
			BaseMessage: BaseMessage{
				Type:      ForkFromFailureMessage,
				RequestID: requestID,
			},
			ErrorMessage: errorMsg,
		},
		ForkedWorkflowIDs: forkedIDs,
	}

	return c.sendResponse(response, string(ForkFromFailureMessage))
}

func (c *Conductor) handleExistPendingWorkflowsRequest(data []byte, requestID string) error {
	var req ExistPendingWorkflowsConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse exist pending workflows request", "error", err)
		return fmt.Errorf("failed to parse exist pending workflows request: %w", err)
	}
	c.logger.Debug("Handling exist pending workflows request", "executor_id", req.ExecutorID, "application_version", req.ApplicationVersion)

	opts := []models.ListWorkflowsOption{
		models.WithStatus([]models.WorkflowStatusType{models.WorkflowStatusPending}),
		models.WithLimit(1), // We only need to know if any exist, so limit to 1 for efficiency
		models.WithExecutorIDs([]string{req.ExecutorID}),
		models.WithAppVersion(req.ApplicationVersion),
	}

	workflows, err := c.exec.ListWorkflows(c.ctx, opts...)
	var errorMsg *string
	if err != nil {
		c.logger.Error("Failed to check for pending workflows", "executor_id", req.ExecutorID, "application_version", req.ApplicationVersion, "error", err)
		errStr := fmt.Sprintf("failed to check for pending workflows: %v", err)
		errorMsg = &errStr
	}

	response := ExistPendingWorkflowsConductorResponse{
		BaseResponse: BaseResponse{
			BaseMessage: BaseMessage{
				Type:      ExistPendingWorkflowsMessage,
				RequestID: requestID,
			},
			ErrorMessage: errorMsg,
		},
		Exist: len(workflows) > 0,
	}

	return c.sendResponse(response, string(ExistPendingWorkflowsMessage))
}

func (c *Conductor) handleAlertRequest(data []byte, requestID string) error {
	var req AlertRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse alert request", "error", err)
		return fmt.Errorf("failed to parse alert request: %w", err)
	}
	c.logger.Debug("Handling alert request", "name", req.Name, "request_id", requestID)

	success := true
	var errorMsg *string

	handler := c.exec.AlertHandler()
	if handler != nil {
		func() {
			defer func() {
				if r := recover(); r != nil {
					errStr := fmt.Sprintf("panic in alert handler: %v", r)
					c.logger.Error(errStr)
					errorMsg = &errStr
					success = false
				}
			}()
			handler(req.Name, req.Message, req.Metadata)
		}()
	} else {
		c.logger.Info("Alert received (no handler registered)", "name", req.Name, "message", req.Message, "metadata", req.Metadata)
	}

	response := AlertConductorResponse{
		BaseResponse: BaseResponse{
			BaseMessage: BaseMessage{
				Type:      AlertMessage,
				RequestID: requestID,
			},
			ErrorMessage: errorMsg,
		},
		Success: success,
	}

	return c.sendResponse(response, string(AlertMessage))
}

func (c *Conductor) handleUnknownMessageType(requestID string, msgType MessageType, errorMsg string) error {
	if c.conn == nil {
		return fmt.Errorf("no connection")
	}

	response := BaseResponse{
		BaseMessage: BaseMessage{
			Type:      msgType,
			RequestID: requestID,
		},
		ErrorMessage: &errorMsg,
	}

	return c.sendResponse(response, "unknown message type response")
}

func (c *Conductor) handleExportWorkflowRequest(data []byte, requestID string) error {
	var req ExportWorkflowConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse export workflow request", "error", err)
		return fmt.Errorf("failed to parse export workflow request: %w", err)
	}
	c.logger.Debug("Handling export workflow request", "workflow_id", req.WorkflowID, "export_children", req.ExportChildren)

	var serializedWorkflow *string
	var errorMsg *string

	exported, err := sysdb.RetryWithResult(c.ctx, func() ([]sysdb.ExportedWorkflow, error) {
		return c.exec.SystemDB().ExportWorkflow(c.ctx, req.WorkflowID, req.ExportChildren)
	}, sysdb.WithRetrierLogger(c.logger))
	if err != nil {
		c.logger.Error("Failed to export workflow", "workflow_id", req.WorkflowID, "error", err)
		errStr := fmt.Sprintf("Exception encountered when exporting workflow %s: %v", req.WorkflowID, err)
		errorMsg = &errStr
	} else {
		jsonData, err := json.Marshal(exported)
		if err != nil {
			errStr := fmt.Sprintf("Failed to marshal exported workflow: %v", err)
			errorMsg = &errStr
		} else {
			var buf bytes.Buffer
			gz := gzip.NewWriter(&buf)
			if _, err := gz.Write(jsonData); err != nil {
				errStr := fmt.Sprintf("Failed to gzip exported workflow: %v", err)
				errorMsg = &errStr
			} else if err := gz.Close(); err != nil {
				errStr := fmt.Sprintf("Failed to close gzip writer: %v", err)
				errorMsg = &errStr
			} else {
				encoded := base64.StdEncoding.EncodeToString(buf.Bytes())
				serializedWorkflow = &encoded
			}
		}
	}

	response := ExportWorkflowConductorResponse{
		BaseResponse: BaseResponse{
			BaseMessage: BaseMessage{
				Type:      ExportWorkflowMessage,
				RequestID: requestID,
			},
			ErrorMessage: errorMsg,
		},
		SerializedWorkflow: serializedWorkflow,
	}

	return c.sendResponse(response, string(ExportWorkflowMessage))
}

func (c *Conductor) handleImportWorkflowRequest(data []byte, requestID string) error {
	var req ImportWorkflowConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse import workflow request", "error", err)
		return fmt.Errorf("failed to parse import workflow request: %w", err)
	}
	c.logger.Debug("Handling import workflow request")

	success := true
	var errorMsg *string

	compressed, err := base64.StdEncoding.DecodeString(req.SerializedWorkflow)
	if err != nil {
		errStr := fmt.Sprintf("Failed to base64 decode serialized workflow: %v", err)
		errorMsg = &errStr
		success = false
	} else {
		gz, err := gzip.NewReader(bytes.NewReader(compressed))
		if err != nil {
			errStr := fmt.Sprintf("Failed to create gzip reader: %v", err)
			errorMsg = &errStr
			success = false
		} else {
			jsonData, err := io.ReadAll(gz)
			if closeErr := gz.Close(); closeErr != nil && err == nil {
				err = closeErr
			}
			if err != nil {
				errStr := fmt.Sprintf("Failed to decompress workflow data: %v", err)
				errorMsg = &errStr
				success = false
			} else {
				var workflows []sysdb.ExportedWorkflow
				if err := json.Unmarshal(jsonData, &workflows); err != nil {
					errStr := fmt.Sprintf("Failed to unmarshal workflow data: %v", err)
					errorMsg = &errStr
					success = false
				} else {
					err := sysdb.Retry(c.ctx, func() error {
						return c.exec.SystemDB().ImportWorkflow(c.ctx, workflows)
					}, sysdb.WithRetrierLogger(c.logger))
					if err != nil {
						errStr := fmt.Sprintf("Exception encountered when importing workflow: %v", err)
						errorMsg = &errStr
						success = false
					}
				}
			}
		}
	}

	response := ImportWorkflowConductorResponse{
		BaseResponse: BaseResponse{
			BaseMessage: BaseMessage{
				Type:      ImportWorkflowMessage,
				RequestID: requestID,
			},
			ErrorMessage: errorMsg,
		},
		Success: success,
	}

	return c.sendResponse(response, string(ImportWorkflowMessage))
}

func (c *Conductor) handleDeleteWorkflowRequest(data []byte, requestID string) error {
	var req DeleteWorkflowConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse delete workflow request", "error", err)
		return fmt.Errorf("failed to parse delete workflow request: %w", err)
	}
	workflowIDs := req.WorkflowIDs
	if len(workflowIDs) == 0 && req.WorkflowID != "" {
		workflowIDs = []string{req.WorkflowID}
	}
	c.logger.Debug("Handling delete workflow request", "workflow_ids", workflowIDs, "delete_children", req.DeleteChildren, "request_id", requestID)

	success := true
	var errorMsg *string

	err := sysdb.Retry(c.ctx, func() error {
		return c.exec.SystemDB().DeleteWorkflows(c.ctx, sysdb.DeleteWorkflowsDBInput{
			WorkflowIDs:    workflowIDs,
			DeleteChildren: req.DeleteChildren,
		})
	}, sysdb.WithRetrierLogger(c.logger))
	if err != nil {
		c.logger.Error("Failed to delete workflows", "workflow_ids", workflowIDs, "error", err)
		errStr := fmt.Sprintf("failed to delete workflows: %v", err)
		errorMsg = &errStr
		success = false
	} else {
		c.logger.Info("Successfully deleted workflows", "workflow_ids", workflowIDs)
	}

	response := DeleteWorkflowConductorResponse{
		BaseResponse: BaseResponse{
			BaseMessage: BaseMessage{
				Type:      DeleteWorkflowMessage,
				RequestID: requestID,
			},
			ErrorMessage: errorMsg,
		},
		Success: success,
	}

	return c.sendResponse(response, string(DeleteWorkflowMessage))
}

// decodeStoredValueForConductor deserializes a value using its recorded serialization
// format and re-marshals it as plain JSON so Conductor receives a portable string
// regardless of the on-disk encoding. Custom non-JSON serializers may not round-trip
// losslessly for types that don't JSON-encode.
func (c *Conductor) decodeStoredValueForConductor(value, serialization string) (string, error) {
	return c.exec.DecodeStoredValue(c.ctx, value, serialization)
}

func (c *Conductor) handleGetWorkflowEventsRequest(data []byte, requestID string) error {
	var req GetWorkflowEventsConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse get workflow events request", "error", err)
		return fmt.Errorf("failed to parse get workflow events request: %w", err)
	}
	c.logger.Debug("Handling get workflow events request", "workflow_id", req.WorkflowID, "request_id", requestID)

	resp := GetWorkflowEventsConductorResponse{
		BaseResponse: BaseResponse{
			BaseMessage: BaseMessage{Type: GetWorkflowEventsMessage, RequestID: requestID},
		},
	}

	records, err := c.exec.SystemDB().GetAllEvents(c.ctx, req.WorkflowID)
	if err != nil {
		c.logger.Error("Failed to get workflow events", "workflow_id", req.WorkflowID, "error", err)
		errStr := fmt.Sprintf("failed to get workflow events: %v", err)
		resp.ErrorMessage = &errStr
		return c.sendResponse(resp, string(GetWorkflowEventsMessage))
	}

	events := make([]EventOutput, 0, len(records))
	for _, r := range records {
		value, err := c.decodeStoredValueForConductor(r.Value, r.Serialization)
		if err != nil {
			c.logger.Error("Failed to decode workflow event", "workflow_id", req.WorkflowID, "key", r.Key, "error", err)
			errStr := fmt.Sprintf("failed to decode event %q: %v", r.Key, err)
			resp.ErrorMessage = &errStr
			resp.Events = nil
			return c.sendResponse(resp, string(GetWorkflowEventsMessage))
		}
		events = append(events, EventOutput{Key: r.Key, Value: value})
	}
	resp.Events = events
	return c.sendResponse(resp, string(GetWorkflowEventsMessage))
}

func (c *Conductor) handleGetWorkflowNotificationsRequest(data []byte, requestID string) error {
	var req GetWorkflowNotificationsConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse get workflow notifications request", "error", err)
		return fmt.Errorf("failed to parse get workflow notifications request: %w", err)
	}
	c.logger.Debug("Handling get workflow notifications request", "workflow_id", req.WorkflowID, "request_id", requestID)

	resp := GetWorkflowNotificationsConductorResponse{
		BaseResponse: BaseResponse{
			BaseMessage: BaseMessage{Type: GetWorkflowNotificationsMsg, RequestID: requestID},
		},
	}

	records, err := c.exec.SystemDB().GetAllNotifications(c.ctx, req.WorkflowID)
	if err != nil {
		c.logger.Error("Failed to get workflow notifications", "workflow_id", req.WorkflowID, "error", err)
		errStr := fmt.Sprintf("failed to get workflow notifications: %v", err)
		resp.ErrorMessage = &errStr
		return c.sendResponse(resp, string(GetWorkflowNotificationsMsg))
	}

	notifs := make([]NotificationOutput, 0, len(records))
	for _, r := range records {
		msg, err := c.decodeStoredValueForConductor(r.Message, r.Serialization)
		if err != nil {
			c.logger.Error("Failed to decode notification message", "workflow_id", req.WorkflowID, "error", err)
			errStr := fmt.Sprintf("failed to decode notification: %v", err)
			resp.ErrorMessage = &errStr
			resp.Notifications = nil
			return c.sendResponse(resp, string(GetWorkflowNotificationsMsg))
		}
		notifs = append(notifs, NotificationOutput{
			Topic:            r.Topic,
			Message:          msg,
			CreatedAtEpochMs: r.CreatedAtEpochMs,
			Consumed:         r.Consumed,
		})
	}
	resp.Notifications = notifs
	return c.sendResponse(resp, string(GetWorkflowNotificationsMsg))
}

func (c *Conductor) handleGetWorkflowStreamsRequest(data []byte, requestID string) error {
	var req GetWorkflowStreamsConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse get workflow streams request", "error", err)
		return fmt.Errorf("failed to parse get workflow streams request: %w", err)
	}
	c.logger.Debug("Handling get workflow streams request", "workflow_id", req.WorkflowID, "request_id", requestID)

	resp := GetWorkflowStreamsConductorResponse{
		BaseResponse: BaseResponse{
			BaseMessage: BaseMessage{Type: GetWorkflowStreamsMessage, RequestID: requestID},
		},
	}

	records, err := c.exec.SystemDB().GetAllStreamEntries(c.ctx, req.WorkflowID)
	if err != nil {
		c.logger.Error("Failed to get workflow streams", "workflow_id", req.WorkflowID, "error", err)
		errStr := fmt.Sprintf("failed to get workflow streams: %v", err)
		resp.ErrorMessage = &errStr
		return c.sendResponse(resp, string(GetWorkflowStreamsMessage))
	}

	// Group consecutive records by key (rows are pre-ordered by (key, offset)).
	var streams []StreamEntryOutput
	var current *StreamEntryOutput
	for _, r := range records {
		value, err := c.decodeStoredValueForConductor(r.Value, r.Serialization)
		if err != nil {
			c.logger.Error("Failed to decode stream value", "workflow_id", req.WorkflowID, "key", r.Key, "error", err)
			errStr := fmt.Sprintf("failed to decode stream %q: %v", r.Key, err)
			resp.ErrorMessage = &errStr
			resp.Streams = nil
			return c.sendResponse(resp, string(GetWorkflowStreamsMessage))
		}
		if current == nil || current.Key != r.Key {
			streams = append(streams, StreamEntryOutput{Key: r.Key, Values: []string{value}})
			current = &streams[len(streams)-1]
			continue
		}
		current.Values = append(current.Values, value)
	}
	resp.Streams = streams
	return c.sendResponse(resp, string(GetWorkflowStreamsMessage))
}

func (c *Conductor) handleGetWorkflowAggregatesRequest(data []byte, requestID string) error {
	var req GetWorkflowAggregatesConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse get workflow aggregates request", "error", err)
		return fmt.Errorf("failed to parse get workflow aggregates request: %w", err)
	}
	c.logger.Debug("Handling get workflow aggregates request", "request_id", requestID)

	resp := GetWorkflowAggregatesConductorResponse{
		BaseResponse: BaseResponse{
			BaseMessage: BaseMessage{Type: GetWorkflowAggregatesMessage, RequestID: requestID},
		},
		Output: []sysdb.WorkflowAggregateRow{},
	}

	// An explicitly-provided time_bucket_size_ms must be > 0 (parity with the other SDKs);
	// a nil value means "no bucketing". The public API can't distinguish the two, so reject here.
	if req.Body.TimeBucketSizeMs != nil && *req.Body.TimeBucketSizeMs <= 0 {
		errStr := "time_bucket_size_ms must be > 0"
		resp.ErrorMessage = &errStr
		return c.sendResponse(resp, string(GetWorkflowAggregatesMessage))
	}

	input := models.GetWorkflowAggregatesInput{
		GroupByStatus:             req.Body.GroupByStatus,
		GroupByName:               req.Body.GroupByName,
		GroupByQueueName:          req.Body.GroupByQueueName,
		GroupByExecutorID:         req.Body.GroupByExecutorID,
		GroupByApplicationVersion: req.Body.GroupByApplicationVersion,
		SelectCount:               req.Body.SelectCount,
		SelectMinCreatedAt:        req.Body.SelectMinCreatedAt,
		SelectMaxQueueWaitMs:      req.Body.SelectMaxQueueWaitMs,
		SelectMaxTotalLatencyMs:   req.Body.SelectMaxTotalLatencyMs,
		Name:                      req.Body.Name.toSlice(),
		ApplicationVersion:        req.Body.AppVersion.toSlice(),
		ExecutorID:                req.Body.ExecutorID.toSlice(),
		QueueName:                 req.Body.QueueName.toSlice(),
		WorkflowIDPrefix:          req.Body.WorkflowIDPrefix.toSlice(),
		WorkflowIDs:               req.Body.WorkflowIDs.toSlice(),
		AuthenticatedUser:         req.Body.User.toSlice(),
		ForkedFrom:                req.Body.ForkedFrom.toSlice(),
		ParentWorkflowID:          req.Body.ParentWorkflowID.toSlice(),
		WasForkedFrom:             req.Body.WasForkedFrom,
		HasParent:                 req.Body.HasParent,
		Attributes:                req.Body.Attributes,
	}
	// Default to count when nothing is selected: the admin aggregates API omits select
	// flags when it only wants counts (e.g. grouping by time_bucket alone), and forwards
	// the body verbatim. Without this the query would error "at least one select_ flag".
	if !input.SelectCount && !input.SelectMinCreatedAt && !input.SelectMaxQueueWaitMs && !input.SelectMaxTotalLatencyMs {
		input.SelectCount = true
	}
	if req.Body.TimeBucketSizeMs != nil {
		input.TimeBucketSize = time.Duration(*req.Body.TimeBucketSizeMs) * time.Millisecond
	}
	if len(req.Body.Status) > 0 {
		statuses := make([]models.WorkflowStatusType, len(req.Body.Status))
		for i, s := range req.Body.Status {
			statuses[i] = models.WorkflowStatusType(s)
		}
		input.Status = statuses
	}
	if req.Body.StartTime != nil {
		input.StartTime = *req.Body.StartTime
	}
	if req.Body.EndTime != nil {
		input.EndTime = *req.Body.EndTime
	}
	if req.Body.CompletedAfter != nil {
		input.CompletedAfter = *req.Body.CompletedAfter
	}
	if req.Body.CompletedBefore != nil {
		input.CompletedBefore = *req.Body.CompletedBefore
	}
	if req.Body.DequeuedAfter != nil {
		input.DequeuedAfter = *req.Body.DequeuedAfter
	}
	if req.Body.DequeuedBefore != nil {
		input.DequeuedBefore = *req.Body.DequeuedBefore
	}

	rows, err := c.exec.GetWorkflowAggregates(c.ctx, input)
	if err != nil {
		c.logger.Error("Failed to get workflow aggregates", "error", err)
		errStr := fmt.Sprintf("failed to get workflow aggregates: %v", err)
		resp.ErrorMessage = &errStr
		return c.sendResponse(resp, string(GetWorkflowAggregatesMessage))
	}

	resp.Output = rows
	return c.sendResponse(resp, string(GetWorkflowAggregatesMessage))
}

func (c *Conductor) handleGetStepAggregatesRequest(data []byte, requestID string) error {
	var req GetStepAggregatesConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse get step aggregates request", "error", err)
		return fmt.Errorf("failed to parse get step aggregates request: %w", err)
	}
	c.logger.Debug("Handling get step aggregates request", "request_id", requestID)

	resp := GetStepAggregatesConductorResponse{
		BaseResponse: BaseResponse{
			BaseMessage: BaseMessage{Type: GetStepAggregatesMessage, RequestID: requestID},
		},
		Output: []sysdb.StepAggregateRow{},
	}

	// An explicitly-provided time_bucket_size_ms must be > 0 (parity with the other SDKs).
	if req.Body.TimeBucketSizeMs != nil && *req.Body.TimeBucketSizeMs <= 0 {
		errStr := "time_bucket_size_ms must be > 0"
		resp.ErrorMessage = &errStr
		return c.sendResponse(resp, string(GetStepAggregatesMessage))
	}

	input := models.GetStepAggregatesInput{
		GroupByFunctionName: req.Body.GroupByFunctionName,
		GroupByStatus:       req.Body.GroupByStatus,
		SelectCount:         req.Body.SelectCount,
		SelectMaxDurationMs: req.Body.SelectMaxDurationMs,
		Status:              req.Body.Status.toSlice(),
		FunctionName:        req.Body.FunctionName.toSlice(),
		WorkflowIDPrefix:    req.Body.WorkflowIDPrefix.toSlice(),
	}
	// Default to count when nothing is selected: the admin aggregates API omits select
	// flags when it only wants counts, and forwards the body verbatim. Without this the
	// query would error "at least one select_ flag".
	if !input.SelectCount && !input.SelectMaxDurationMs {
		input.SelectCount = true
	}
	if req.Body.TimeBucketSizeMs != nil {
		input.TimeBucketSize = time.Duration(*req.Body.TimeBucketSizeMs) * time.Millisecond
	}
	if req.Body.CompletedAfter != nil {
		input.CompletedAfter = *req.Body.CompletedAfter
	}
	if req.Body.CompletedBefore != nil {
		input.CompletedBefore = *req.Body.CompletedBefore
	}

	rows, err := c.exec.GetStepAggregates(c.ctx, input)
	if err != nil {
		c.logger.Error("Failed to get step aggregates", "error", err)
		errStr := fmt.Sprintf("Exception encountered when getting step aggregates: %v", err)
		resp.ErrorMessage = &errStr
		return c.sendResponse(resp, string(GetStepAggregatesMessage))
	}

	resp.Output = rows
	return c.sendResponse(resp, string(GetStepAggregatesMessage))
}

func (c *Conductor) sendResponse(response any, responseType string) error {
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

// toScheduleConductorOutput renders a models.WorkflowSchedule for the conductor wire format.
// When loadContext is true, Context is JSON-encoded into a string; otherwise it is omitted.
func toScheduleConductorOutput(s models.WorkflowSchedule, loadContext bool) ScheduleConductorOutput {
	out := ScheduleConductorOutput{
		ScheduleID:        s.ScheduleID,
		ScheduleName:      s.ScheduleName,
		WorkflowName:      s.WorkflowName,
		Schedule:          s.Schedule,
		Status:            string(s.Status),
		AutomaticBackfill: s.AutomaticBackfill,
	}
	if s.WorkflowClassName != "" {
		v := s.WorkflowClassName
		out.WorkflowClassName = &v
	}
	if s.LastFiredAt != nil {
		v := s.LastFiredAt.Format(time.RFC3339Nano)
		out.LastFiredAt = &v
	}
	if s.CronTimezone != "" {
		v := s.CronTimezone
		out.CronTimezone = &v
	}
	if s.QueueName != "" {
		v := s.QueueName
		out.QueueName = &v
	}
	if loadContext && s.Context != nil {
		if b, err := json.Marshal(s.Context); err == nil {
			str := string(b)
			out.Context = &str
		}
	}
	return out
}

func (c *Conductor) handleListSchedulesRequest(data []byte, requestID string) error {
	var req ListSchedulesConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse list schedules request", "error", err)
		return fmt.Errorf("failed to parse list schedules request: %w", err)
	}

	loadContext := true
	if req.Body.LoadContext != nil {
		loadContext = *req.Body.LoadContext
	}

	var opts []models.ListSchedulesOption
	if len(req.Body.Status) > 0 {
		statuses := make([]models.ScheduleStatus, len(req.Body.Status))
		for i, s := range req.Body.Status {
			statuses[i] = models.ScheduleStatus(s)
		}
		opts = append(opts, models.WithScheduleStatuses(statuses...))
	}
	if len(req.Body.WorkflowName) > 0 {
		opts = append(opts, models.WithScheduleWorkflowNames(req.Body.WorkflowName.toSlice()...))
	}
	if len(req.Body.ScheduleNamePrefix) > 0 {
		opts = append(opts, models.WithScheduleNamePrefixes(req.Body.ScheduleNamePrefix.toSlice()...))
	}

	schedules, err := c.exec.ListSchedules(c.ctx, opts...)
	output := []ScheduleConductorOutput{}
	var errorMsg *string
	if err != nil {
		c.logger.Error("Failed to list schedules", "error", err)
		msg := fmt.Sprintf("failed to list schedules: %v", err)
		errorMsg = &msg
	} else {
		output = make([]ScheduleConductorOutput, len(schedules))
		for i := range schedules {
			output[i] = toScheduleConductorOutput(schedules[i], loadContext)
		}
	}

	resp := ListSchedulesConductorResponse{
		BaseResponse: BaseResponse{
			BaseMessage:  BaseMessage{Type: ListSchedulesMessage, RequestID: requestID},
			ErrorMessage: errorMsg,
		},
		Output: output,
	}
	return c.sendResponse(resp, string(ListSchedulesMessage))
}

func (c *Conductor) handleGetScheduleRequest(data []byte, requestID string) error {
	var req GetScheduleConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse get schedule request", "error", err)
		return fmt.Errorf("failed to parse get schedule request: %w", err)
	}

	loadContext := true
	if req.LoadContext != nil {
		loadContext = *req.LoadContext
	}

	schedule, err := c.exec.GetSchedule(c.ctx, req.ScheduleName)
	var errorMsg *string
	var output *ScheduleConductorOutput
	if err != nil {
		c.logger.Error("Failed to get schedule", "schedule_name", req.ScheduleName, "error", err)
		msg := fmt.Sprintf("failed to get schedule '%s': %v", req.ScheduleName, err)
		errorMsg = &msg
	} else if schedule != nil {
		o := toScheduleConductorOutput(*schedule, loadContext)
		output = &o
	}

	resp := GetScheduleConductorResponse{
		BaseResponse: BaseResponse{
			BaseMessage:  BaseMessage{Type: GetScheduleMessage, RequestID: requestID},
			ErrorMessage: errorMsg,
		},
		Output: output,
	}
	return c.sendResponse(resp, string(GetScheduleMessage))
}

func (c *Conductor) handlePauseScheduleRequest(data []byte, requestID string) error {
	var req PauseScheduleConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse pause schedule request", "error", err)
		return fmt.Errorf("failed to parse pause schedule request: %w", err)
	}

	success := true
	var errorMsg *string
	if err := c.exec.PauseSchedule(c.ctx, req.ScheduleName); err != nil {
		c.logger.Error("Failed to pause schedule", "schedule_name", req.ScheduleName, "error", err)
		msg := fmt.Sprintf("failed to pause schedule '%s': %v", req.ScheduleName, err)
		errorMsg = &msg
		success = false
	}

	resp := PauseScheduleConductorResponse{
		BaseResponse: BaseResponse{
			BaseMessage:  BaseMessage{Type: PauseScheduleMessage, RequestID: requestID},
			ErrorMessage: errorMsg,
		},
		Success: success,
	}
	return c.sendResponse(resp, string(PauseScheduleMessage))
}

func (c *Conductor) handleResumeScheduleRequest(data []byte, requestID string) error {
	var req ResumeScheduleConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse resume schedule request", "error", err)
		return fmt.Errorf("failed to parse resume schedule request: %w", err)
	}

	success := true
	var errorMsg *string
	if err := c.exec.ResumeSchedule(c.ctx, req.ScheduleName); err != nil {
		c.logger.Error("Failed to resume schedule", "schedule_name", req.ScheduleName, "error", err)
		msg := fmt.Sprintf("failed to resume schedule '%s': %v", req.ScheduleName, err)
		errorMsg = &msg
		success = false
	}

	resp := ResumeScheduleConductorResponse{
		BaseResponse: BaseResponse{
			BaseMessage:  BaseMessage{Type: ResumeScheduleMessage, RequestID: requestID},
			ErrorMessage: errorMsg,
		},
		Success: success,
	}
	return c.sendResponse(resp, string(ResumeScheduleMessage))
}

func (c *Conductor) handleBackfillScheduleRequest(data []byte, requestID string) error {
	var req BackfillScheduleConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse backfill schedule request", "error", err)
		return fmt.Errorf("failed to parse backfill schedule request: %w", err)
	}

	var errorMsg *string
	var workflowIDs []string

	start, err := time.Parse(time.RFC3339Nano, req.Start)
	if err != nil {
		start, err = time.Parse(time.RFC3339, req.Start)
	}
	if err != nil {
		msg := fmt.Sprintf("failed to parse start time '%s': %v", req.Start, err)
		errorMsg = &msg
	} else {
		end, errEnd := time.Parse(time.RFC3339Nano, req.End)
		if errEnd != nil {
			end, errEnd = time.Parse(time.RFC3339, req.End)
		}
		if errEnd != nil {
			msg := fmt.Sprintf("failed to parse end time '%s': %v", req.End, errEnd)
			errorMsg = &msg
		} else {
			schedule, errGet := c.exec.GetSchedule(c.ctx, req.ScheduleName)
			if errGet != nil {
				msg := fmt.Sprintf("failed to get schedule '%s': %v", req.ScheduleName, errGet)
				errorMsg = &msg
			} else if schedule == nil {
				msg := fmt.Sprintf("schedule not found: %s", req.ScheduleName)
				errorMsg = &msg
			} else {
				ids, errBf := c.exec.SystemDB().BackfillSchedule(c.ctx, sysdb.BackfillScheduleDBInput{
					ScheduleName: req.ScheduleName,
					Schedule:     schedule.Schedule,
					StartTime:    start,
					EndTime:      end,
				})
				if errBf != nil {
					msg := fmt.Sprintf("failed to backfill schedule '%s': %v", req.ScheduleName, errBf)
					errorMsg = &msg
				} else {
					workflowIDs = ids
				}
			}
		}
	}

	if workflowIDs == nil {
		workflowIDs = []string{}
	}
	resp := BackfillScheduleConductorResponse{
		BaseResponse: BaseResponse{
			BaseMessage:  BaseMessage{Type: BackfillScheduleMessage, RequestID: requestID},
			ErrorMessage: errorMsg,
		},
		WorkflowIDs: workflowIDs,
	}
	return c.sendResponse(resp, string(BackfillScheduleMessage))
}

func (c *Conductor) handleTriggerScheduleRequest(data []byte, requestID string) error {
	var req TriggerScheduleConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse trigger schedule request", "error", err)
		return fmt.Errorf("failed to parse trigger schedule request: %w", err)
	}

	var errorMsg *string
	var workflowID *string
	id, err := c.exec.SystemDB().TriggerSchedule(c.ctx, req.ScheduleName)
	if err != nil {
		c.logger.Error("Failed to trigger schedule", "schedule_name", req.ScheduleName, "error", err)
		msg := fmt.Sprintf("failed to trigger schedule '%s': %v", req.ScheduleName, err)
		errorMsg = &msg
	} else {
		workflowID = &id
	}

	resp := TriggerScheduleConductorResponse{
		BaseResponse: BaseResponse{
			BaseMessage:  BaseMessage{Type: TriggerScheduleMessage, RequestID: requestID},
			ErrorMessage: errorMsg,
		},
		WorkflowID: workflowID,
	}
	return c.sendResponse(resp, string(TriggerScheduleMessage))
}

func (c *Conductor) handleListApplicationVersionsRequest(data []byte, requestID string) error {
	var req ListApplicationVersionsConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse list application versions request", "error", err)
		return fmt.Errorf("failed to parse list application versions request: %w", err)
	}

	var errorMsg *string
	output := []ApplicationVersionOutput{}
	versions, err := sysdb.RetryWithResult(c.ctx, func() ([]sysdb.VersionInfo, error) {
		return c.exec.SystemDB().ListApplicationVersions(c.ctx)
	}, sysdb.WithRetrierLogger(c.logger))
	if err != nil {
		c.logger.Error("Failed to list application versions", "error", err)
		msg := fmt.Sprintf("failed to list application versions: %v", err)
		errorMsg = &msg
	} else {
		for _, v := range versions {
			output = append(output, formatApplicationVersionOutput(v))
		}
	}

	resp := ListApplicationVersionsConductorResponse{
		BaseResponse: BaseResponse{
			BaseMessage:  BaseMessage{Type: ListAppVersionsMessage, RequestID: requestID},
			ErrorMessage: errorMsg,
		},
		Output: output,
	}
	return c.sendResponse(resp, string(ListAppVersionsMessage))
}

func (c *Conductor) handleSetLatestApplicationVersionRequest(data []byte, requestID string) error {
	var req SetLatestApplicationVersionConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse set latest application version request", "error", err)
		return fmt.Errorf("failed to parse set latest application version request: %w", err)
	}

	success := true
	var errorMsg *string
	if err := sysdb.Retry(c.ctx, func() error {
		return c.exec.SystemDB().UpdateApplicationVersionTimestamp(c.ctx, req.VersionName, time.Now().UnixMilli())
	}, sysdb.WithRetrierLogger(c.logger)); err != nil {
		c.logger.Error("Failed to set latest application version", "version_name", req.VersionName, "error", err)
		msg := fmt.Sprintf("failed to set latest application version '%s': %v", req.VersionName, err)
		errorMsg = &msg
		success = false
	}

	resp := SetLatestApplicationVersionConductorResponse{
		BaseResponse: BaseResponse{
			BaseMessage:  BaseMessage{Type: SetLatestAppVersionMessage, RequestID: requestID},
			ErrorMessage: errorMsg,
		},
		Success: success,
	}
	return c.sendResponse(resp, string(SetLatestAppVersionMessage))
}

func (c *Conductor) handleListQueuesRequest(data []byte, requestID string) error {
	var req ListQueuesConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse list queues request", "error", err)
		return fmt.Errorf("failed to parse list queues request: %w", err)
	}

	queues, err := c.exec.ListQueues(c.ctx)
	output := []QueueConductorOutput{}
	var errorMsg *string
	if err != nil {
		c.logger.Error("Failed to list queues", "error", err)
		msg := fmt.Sprintf("failed to list queues: %v", err)
		errorMsg = &msg
	} else {
		output = make([]QueueConductorOutput, len(queues))
		for i := range queues {
			output[i] = toQueueConductorOutput(queues[i])
		}
	}

	resp := ListQueuesConductorResponse{
		BaseResponse: BaseResponse{
			BaseMessage:  BaseMessage{Type: ListQueuesMessage, RequestID: requestID},
			ErrorMessage: errorMsg,
		},
		Output: output,
	}
	return c.sendResponse(resp, string(ListQueuesMessage))
}

func (c *Conductor) handleGetQueueRequest(data []byte, requestID string) error {
	var req GetQueueConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse get queue request", "error", err)
		return fmt.Errorf("failed to parse get queue request: %w", err)
	}

	queue, err := c.exec.RetrieveQueue(c.ctx, req.Name)
	var errorMsg *string
	var output *QueueConductorOutput
	if err != nil {
		c.logger.Error("Failed to get queue", "queue_name", req.Name, "error", err)
		msg := fmt.Sprintf("failed to get queue '%s': %v", req.Name, err)
		errorMsg = &msg
	} else if queue != nil {
		o := toQueueConductorOutput(*queue)
		output = &o
	}

	resp := GetQueueConductorResponse{
		BaseResponse: BaseResponse{
			BaseMessage:  BaseMessage{Type: GetQueueMessage, RequestID: requestID},
			ErrorMessage: errorMsg,
		},
		Output: output,
	}
	return c.sendResponse(resp, string(GetQueueMessage))
}
