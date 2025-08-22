package dbos

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	_PING_INTERVAL          = 20 * time.Second
	_PING_TIMEOUT           = 30 * time.Second // Should be > server's executorPingWait (25s)
	_INITIAL_RECONNECT_WAIT = 1 * time.Second
	_MAX_RECONNECT_WAIT     = 30 * time.Second
	_HANDSHAKE_TIMEOUT      = 10 * time.Second
)

// ConductorConfig contains configuration for the conductor
type ConductorConfig struct {
	url     string
	apiKey  string
	appName string
	logger  *slog.Logger
}

// Conductor manages the WebSocket connection to the DBOS conductor service
type Conductor struct {
	config   ConductorConfig
	conn     *websocket.Conn
	wg       sync.WaitGroup
	stopOnce sync.Once
	dbosCtx  *dbosContext

	// Connection parameters
	url           url.URL
	pingInterval  time.Duration
	pingTimeout   time.Duration
	reconnectWait time.Duration
}

// Launch starts the conductor connection manager goroutine
func (c *Conductor) Launch() {
	c.config.logger.Info("Launching conductor", "url", c.url.String())
	c.wg.Add(1)
	go c.run()
}

// NewConductor creates a new conductor instance
func NewConductor(config ConductorConfig, dbosCtx *dbosContext) (*Conductor, error) {
	if config.apiKey == "" {
		return nil, fmt.Errorf("conductor API key is required")
	}
	if config.url == "" {
		return nil, fmt.Errorf("conductor URL is required")
	}

	// Parse the base conductor URL
	baseURL, err := url.Parse(config.url)
	if err != nil {
		return nil, fmt.Errorf("invalid conductor URL: %w", err)
	}

	// Build WebSocket URL using net.JoinPath for proper path construction
	wsURL := url.URL{
		Scheme: baseURL.Scheme,
		Host:   baseURL.Host,
		Path:   baseURL.JoinPath("websocket", config.appName, config.apiKey).Path,
	}

	// Convert HTTP schemes to WebSocket schemes
	switch wsURL.Scheme {
	case "http":
		wsURL.Scheme = "ws"
	case "https":
		wsURL.Scheme = "wss"
	case "ws", "wss":
		// Already correct
	default:
		return nil, fmt.Errorf("unsupported URL scheme: %s (expected http, https, ws, or wss)", wsURL.Scheme)
	}

	c := &Conductor{
		config:        config,
		dbosCtx:       dbosCtx,
		url:           wsURL,
		pingInterval:  _PING_INTERVAL,
		pingTimeout:   _PING_TIMEOUT,
		reconnectWait: _INITIAL_RECONNECT_WAIT,
	}

	config.logger.Info("Conductor created", "url", wsURL.String())
	return c, nil
}

// Shutdown gracefully conductor
func (c *Conductor) Shutdown(timeout time.Duration) {
	c.stopOnce.Do(func() {
		c.config.logger.Info("Shutting down conductor")

		done := make(chan struct{})
		go func() {
			c.wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			c.config.logger.Info("Conductor shut down")
		case <-time.After(timeout):
			c.config.logger.Warn("Timeout waiting for conductor to shut down", "timeout", timeout)
		}
	})
}

// run manages the WebSocket connection lifecycle with reconnection
func (c *Conductor) run() {
	defer c.wg.Done()

	for {
		// Check if the context has been cancelled
		select {
		case <-c.dbosCtx.Done():
			c.config.logger.Info("DBOS context done, stopping conductor", "cause", context.Cause(c.dbosCtx))
			if c.conn != nil {
				c.conn.Close()
			}
			return
		default:
		}

		// Connect if not connected
		if c.conn == nil {
			if err := c.connect(); err != nil {
				c.config.logger.Warn("Failed to connect to conductor", "error", err)
				select {
				case <-c.dbosCtx.Done():
					c.config.logger.Info("DBOS context done, stopping conductor", "cause", context.Cause(c.dbosCtx))
					return
				case <-time.After(c.reconnectWait):
					// Exponential backoff up to max wait
					if c.reconnectWait < _MAX_RECONNECT_WAIT {
						c.reconnectWait *= 2
					}
					continue
				}
			}

			// Reset reconnect wait on successful connection
			c.reconnectWait = _INITIAL_RECONNECT_WAIT
		}

		// Read message (will timeout based on read deadline set in connect)
		messageType, message, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				c.config.logger.Warn("Unexpected WebSocket close", "error", err)
			} else if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				// This is expected - read deadline timeout, connection is still healthy
				c.config.logger.Debug("Read deadline reached, connection healthy")
				continue
			} else {
				c.config.logger.Debug("Connection closed", "error", err)
			}
			// Close connection to trigger reconnection
			if c.conn != nil {
				c.conn.Close()
				c.conn = nil
			}
			continue
		}

		// Only accept text messages
		if messageType != websocket.TextMessage {
			c.config.logger.Warn("Received unexpected message type, forcing reconnection", "type", messageType)
			if c.conn != nil {
				c.conn.Close()
				c.conn = nil
			}
			continue
		}

		if err := c.handleMessage(message); err != nil {
			c.config.logger.Error("Failed to handle message", "error", err)
		}
	}
}

// connect establishes a WebSocket connection to the conductor
func (c *Conductor) connect() error {
	c.config.logger.Debug("Connecting to conductor", "url", c.url.String())

	dialer := websocket.Dialer{
		HandshakeTimeout: _HANDSHAKE_TIMEOUT,
	}

	conn, _, err := dialer.Dial(c.url.String(), nil)
	if err != nil {
		return fmt.Errorf("failed to dial conductor: %w", err)
	}

	// Set initial read deadline
	if err := conn.SetReadDeadline(time.Now().Add(c.pingTimeout)); err != nil {
		conn.Close()
		return fmt.Errorf("failed to set read deadline: %w", err)
	}

	// Set pong handler to reset read deadline
	conn.SetPongHandler(func(appData string) error {
		c.config.logger.Debug("Received pong from conductor")
		return conn.SetReadDeadline(time.Now().Add(c.pingTimeout))
	})

	c.conn = conn

	// Start ping goroutine
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ticker := time.NewTicker(c.pingInterval)
		defer ticker.Stop()

		for {
			select {
			case <-c.dbosCtx.Done():
				c.config.logger.Debug("Ping goroutine stopped, context done")
				return
			case <-ticker.C:
				if err := c.ping(); err != nil {
					c.config.logger.Warn("Ping failed, closing connection", "error", err)
					// Close the connection to trigger reconnection in main loop
					conn.Close()
					return
				}
			}
		}
	}()

	c.config.logger.Info("Connected to DBOS conductor")
	return nil
}

// ping sends a ping to the conductor
func (c *Conductor) ping() error {
	if c.conn == nil {
		return fmt.Errorf("no connection")
	}

	c.config.logger.Debug("Sending ping to conductor")

	if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
		return fmt.Errorf("failed to send ping: %w", err)
	}

	return nil
}

// handleMessage processes an incoming message from the conductor
func (c *Conductor) handleMessage(data []byte) error {
	var base baseMessage
	if err := json.Unmarshal(data, &base); err != nil {
		c.config.logger.Error("Failed to parse message", "error", err)
		return fmt.Errorf("failed to parse base message: %w", err)
	}
	c.config.logger.Debug("Received message", "type", base.Type, "request_id", base.RequestID)

	switch base.Type {
	case ExecutorInfo:
		return c.handleExecutorInfoRequest(data, base.RequestID)
	case CancelWorkflowMessage:
		return c.handleCancelWorkflowRequest(data, base.RequestID)
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
	default:
		c.config.logger.Warn("Unknown message type", "type", base.Type)
		return c.sendErrorResponse(base.RequestID, base.Type, "Unknown message type")
	}
}

// handleExecutorInfoRequest handles executor info requests from the conductor
func (c *Conductor) handleExecutorInfoRequest(data []byte, requestID string) error {
	var req ExecutorInfoRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.config.logger.Error("Failed to parse executor info request", "error", err)
		return fmt.Errorf("failed to parse executor info request: %w", err)
	}
	c.config.logger.Debug("Handling executor info request", "request_id", req)

	hostname, err := os.Hostname()
	if err != nil {
		c.config.logger.Error("Failed to get hostname", "error", err)
		return fmt.Errorf("failed to get hostname: %w", err)
	}

	response := ExecutorInfoResponse{
		baseMessage: baseMessage{
			Type:      ExecutorInfo,
			RequestID: requestID,
		},
		ExecutorID:         c.dbosCtx.GetExecutorID(),
		ApplicationVersion: c.dbosCtx.GetApplicationVersion(),
		Hostname:           &hostname,
	}

	return c.sendExecutorInfoResponse(response)
}

// handleCancelWorkflowRequest handles cancel workflow requests from the conductor
func (c *Conductor) handleCancelWorkflowRequest(data []byte, requestID string) error {
	var req cancelWorkflowConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.config.logger.Error("Failed to parse cancel workflow request", "error", err)
		return fmt.Errorf("failed to parse cancel workflow request: %w", err)
	}
	c.config.logger.Debug("Handling cancel workflow request", "workflow_id", req.WorkflowID, "request_id", requestID)

	// Cancel the workflow using the dbosContext
	success := true
	var errorMsg *string

	if err := c.dbosCtx.CancelWorkflow(c.dbosCtx, req.WorkflowID); err != nil {
		c.config.logger.Error("Failed to cancel workflow", "workflow_id", req.WorkflowID, "error", err)
		errStr := fmt.Sprintf("failed to cancel workflow: %v", err)
		errorMsg = &errStr
		success = false
	} else {
		c.config.logger.Info("Successfully cancelled workflow", "workflow_id", req.WorkflowID)
	}

	response := cancelWorkflowConductorResponse{
		baseMessage: baseMessage{
			Type:      CancelWorkflowMessage,
			RequestID: requestID,
		},
		Success:      success,
		ErrorMessage: errorMsg,
	}

	return c.sendCancelWorkflowResponse(response)
}

// sendExecutorInfoResponse sends an ExecutorInfoResponse to the conductor
func (c *Conductor) sendExecutorInfoResponse(response ExecutorInfoResponse) error {
	if c.conn == nil {
		return fmt.Errorf("no connection")
	}

	data, err := json.Marshal(response)
	if err != nil {
		return fmt.Errorf("failed to marshal executor info response: %w", err)
	}

	c.config.logger.Debug("Sending executor info response", "data", response)

	if err := c.conn.WriteMessage(websocket.TextMessage, data); err != nil {
		c.config.logger.Error("Failed to send executor info response", "error", err)
		return fmt.Errorf("failed to send message: %w", err)
	}

	return nil
}

// sendCancelWorkflowResponse sends a CancelWorkflowResponse to the conductor
func (c *Conductor) sendCancelWorkflowResponse(response cancelWorkflowConductorResponse) error {
	if c.conn == nil {
		return fmt.Errorf("no connection")
	}

	data, err := json.Marshal(response)
	if err != nil {
		return fmt.Errorf("failed to marshal cancel workflow response: %w", err)
	}

	c.config.logger.Debug("Sending cancel workflow response", "success", response.Success, "request_id", response.RequestID)

	if err := c.conn.WriteMessage(websocket.TextMessage, data); err != nil {
		c.config.logger.Error("Failed to send cancel workflow response", "error", err)
		return fmt.Errorf("failed to send message: %w", err)
	}

	return nil
}

// handleListWorkflowsRequest handles list workflows requests from the conductor
func (c *Conductor) handleListWorkflowsRequest(data []byte, requestID string) error {
	var req listWorkflowsConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.config.logger.Error("Failed to parse list workflows request", "error", err)
		return fmt.Errorf("failed to parse list workflows request: %w", err)
	}
	c.config.logger.Debug("Handling list workflows request", "request", req)

	// Build functional options for ListWorkflows
	var opts []ListWorkflowsOption
	opts = append(opts, WithLoadInput(req.Body.LoadInput))
	opts = append(opts, WithLoadOutput(req.Body.LoadOutput))
	opts = append(opts, WithSortDesc(req.Body.SortDesc))
	if len(req.Body.WorkflowUUIDs) > 0 {
		opts = append(opts, WithWorkflowIDs(req.Body.WorkflowUUIDs))
	}
	if req.Body.WorkflowName != nil {
		opts = append(opts, WithName(*req.Body.WorkflowName))
	}
	if req.Body.AuthenticatedUser != nil {
		opts = append(opts, WithUser(*req.Body.AuthenticatedUser))
	}
	if req.Body.ApplicationVersion != nil {
		opts = append(opts, WithAppVersion(*req.Body.ApplicationVersion))
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
	if req.Body.Status != nil {
		opts = append(opts, WithStatus([]WorkflowStatusType{WorkflowStatusType(*req.Body.Status)}))
	}

	workflows, err := c.dbosCtx.ListWorkflows(c.dbosCtx, opts...)
	if err != nil {
		c.config.logger.Error("Failed to list workflows", "error", err)
		errorMsg := fmt.Sprintf("failed to list workflows: %v", err)
		response := listWorkflowsConductorResponse{
			baseMessage: baseMessage{
				Type:      ListWorkflowsMessage,
				RequestID: requestID,
			},
			Output:       []listWorkflowsConductorResponseBody{},
			ErrorMessage: &errorMsg,
		}
		return c.sendListWorkflowsResponse(response)
	}

	// Prepare response payload
	formattedWorkflows := make([]listWorkflowsConductorResponseBody, len(workflows))
	for i, wf := range workflows {
		formattedWorkflows[i] = formatListWorkflowsResponseBody(wf)
	}

	response := listWorkflowsConductorResponse{
		baseMessage: baseMessage{
			Type:      ListWorkflowsMessage,
			RequestID: requestID,
		},
		Output: formattedWorkflows,
	}

	return c.sendListWorkflowsResponse(response)
}

// handleListQueuedWorkflowsRequest handles list queued workflows requests from the conductor
func (c *Conductor) handleListQueuedWorkflowsRequest(data []byte, requestID string) error {
	var req listWorkflowsConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.config.logger.Error("Failed to parse list queued workflows request", "error", err)
		return fmt.Errorf("failed to parse list queued workflows request: %w", err)
	}
	c.config.logger.Debug("Handling list queued workflows request", "request", req)

	// Build functional options for ListWorkflows
	var opts []ListWorkflowsOption
	opts = append(opts, WithLoadInput(req.Body.LoadInput))
	opts = append(opts, WithLoadOutput(false)) // Don't load output for queued workflows
	opts = append(opts, WithSortDesc(req.Body.SortDesc))

	// Add status filter for queued workflows
	queuedStatuses := make([]WorkflowStatusType, 0)
	if req.Body.Status != nil {
		// If a specific status is requested, use that status
		status := WorkflowStatusType(*req.Body.Status)
		if status != WorkflowStatusPending && status != WorkflowStatusEnqueued {
			c.config.logger.Warn("Received unexpected filtering status for listing queued workflows", "status", status)
		}
		queuedStatuses = append(queuedStatuses, status)
	}
	if len(queuedStatuses) > 0 {
		queuedStatuses = []WorkflowStatusType{WorkflowStatusPending, WorkflowStatusEnqueued}
	}
	opts = append(opts, WithStatus(queuedStatuses))

	if req.Body.WorkflowName != nil {
		opts = append(opts, WithName(*req.Body.WorkflowName))
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
	if req.Body.QueueName != nil {
		opts = append(opts, WithQueueName(*req.Body.QueueName))
	}

	workflows, err := c.dbosCtx.ListWorkflows(c.dbosCtx, opts...)
	if err != nil {
		c.config.logger.Error("Failed to list queued workflows", "error", err)
		errorMsg := fmt.Sprintf("failed to list queued workflows: %v", err)
		response := listWorkflowsConductorResponse{
			baseMessage: baseMessage{
				Type:      ListQueuedWorkflowsMessage,
				RequestID: requestID,
			},
			Output:       []listWorkflowsConductorResponseBody{},
			ErrorMessage: &errorMsg,
		}
		return c.sendListWorkflowsResponse(response)
	}

	// If no queue name was specified, only include workflows that have a queue name
	var filteredWorkflows []WorkflowStatus
	if req.Body.QueueName == nil {
		for _, wf := range workflows {
			if wf.QueueName != "" {
				filteredWorkflows = append(filteredWorkflows, wf)
			}
		}
	}

	// Prepare response payload
	formattedWorkflows := make([]listWorkflowsConductorResponseBody, len(filteredWorkflows))
	for i, wf := range filteredWorkflows {
		formattedWorkflows[i] = formatListWorkflowsResponseBody(wf)
	}

	response := listWorkflowsConductorResponse{
		baseMessage: baseMessage{
			Type:      ListWorkflowsMessage,
			RequestID: requestID,
		},
		Output: formattedWorkflows,
	}

	return c.sendListWorkflowsResponse(response)
}

// sendListWorkflowsResponse sends a ListWorkflowsResponse to the conductor
func (c *Conductor) sendListWorkflowsResponse(response listWorkflowsConductorResponse) error {
	if c.conn == nil {
		return fmt.Errorf("no connection")
	}

	data, err := json.Marshal(response)
	if err != nil {
		return fmt.Errorf("failed to marshal list workflows response: %w", err)
	}

	c.config.logger.Debug("Sending list workflows response", "workflow_count", len(response.Output))

	if err := c.conn.WriteMessage(websocket.TextMessage, data); err != nil {
		c.config.logger.Error("Failed to send list workflows response", "error", err)
		return fmt.Errorf("failed to send message: %w", err)
	}

	return nil
}

// handleListStepsRequest handles list steps requests from the conductor
func (c *Conductor) handleListStepsRequest(data []byte, requestID string) error {
	var req listStepsConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.config.logger.Error("Failed to parse list steps request", "error", err)
		return fmt.Errorf("failed to parse list steps request: %w", err)
	}
	c.config.logger.Debug("Handling list steps request", "request", req)

	// Get workflow steps using the existing systemDB method
	steps, err := c.dbosCtx.systemDB.getWorkflowSteps(c.dbosCtx, req.WorkflowID)
	if err != nil {
		c.config.logger.Error("Failed to list workflow steps", "workflow_id", req.WorkflowID, "error", err)
		errorMsg := fmt.Sprintf("failed to list workflow steps: %v", err)
		response := listStepsConductorResponse{
			baseMessage: baseMessage{
				Type:      ListStepsMessage,
				RequestID: requestID,
			},
			Output:       nil,
			ErrorMessage: &errorMsg,
		}
		return c.sendListStepsResponse(response)
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
		baseMessage: baseMessage{
			Type:      ListStepsMessage,
			RequestID: requestID,
		},
		Output: formattedSteps,
	}

	return c.sendListStepsResponse(response)
}

// sendListStepsResponse sends a ListStepsResponse to the conductor
func (c *Conductor) sendListStepsResponse(response listStepsConductorResponse) error {
	if c.conn == nil {
		return fmt.Errorf("no connection")
	}

	data, err := json.Marshal(response)
	if err != nil {
		return fmt.Errorf("failed to marshal list steps response: %w", err)
	}

	var stepCount int
	if response.Output != nil {
		stepCount = len(*response.Output)
	}
	c.config.logger.Debug("Sending list steps response", "step_count", stepCount)

	if err := c.conn.WriteMessage(websocket.TextMessage, data); err != nil {
		c.config.logger.Error("Failed to send list steps response", "error", err)
		return fmt.Errorf("failed to send message: %w", err)
	}

	return nil
}

// handleGetWorkflowRequest handles get workflow requests from the conductor
func (c *Conductor) handleGetWorkflowRequest(data []byte, requestID string) error {
	var req getWorkflowConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.config.logger.Error("Failed to parse get workflow request", "error", err)
		return fmt.Errorf("failed to parse get workflow request: %w", err)
	}
	c.config.logger.Debug("Handling get workflow request", "workflow_id", req.WorkflowID)

	// Get workflow using ListWorkflows with the specific workflow ID filter
	workflows, err := c.dbosCtx.ListWorkflows(c.dbosCtx, WithWorkflowIDs([]string{req.WorkflowID}))
	if err != nil {
		c.config.logger.Error("Failed to get workflow", "workflow_id", req.WorkflowID, "error", err)
		errorMsg := fmt.Sprintf("failed to get workflow: %v", err)
		response := getWorkflowConductorResponse{
			baseMessage: baseMessage{
				Type:      GetWorkflowMessage,
				RequestID: requestID,
			},
			Output:       nil,
			ErrorMessage: &errorMsg,
		}
		return c.sendGetWorkflowResponse(response)
	}

	// Format the workflow for response
	var formattedWorkflow *listWorkflowsConductorResponseBody
	if len(workflows) > 0 {
		formatted := formatListWorkflowsResponseBody(workflows[0])
		formattedWorkflow = &formatted
	}

	response := getWorkflowConductorResponse{
		baseMessage: baseMessage{
			Type:      GetWorkflowMessage,
			RequestID: requestID,
		},
		Output: formattedWorkflow,
	}

	return c.sendGetWorkflowResponse(response)
}

// sendGetWorkflowResponse sends a GetWorkflowResponse to the conductor
func (c *Conductor) sendGetWorkflowResponse(response getWorkflowConductorResponse) error {
	if c.conn == nil {
		return fmt.Errorf("no connection")
	}

	data, err := json.Marshal(response)
	if err != nil {
		return fmt.Errorf("failed to marshal get workflow response: %w", err)
	}

	c.config.logger.Debug("Sending get workflow response", "has_workflow", response.Output != nil)

	if err := c.conn.WriteMessage(websocket.TextMessage, data); err != nil {
		c.config.logger.Error("Failed to send get workflow response", "error", err)
		return fmt.Errorf("failed to send message: %w", err)
	}

	return nil
}

// handleForkWorkflowRequest handles fork workflow requests from the conductor
func (c *Conductor) handleForkWorkflowRequest(data []byte, requestID string) error {
	var req forkWorkflowConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.config.logger.Error("Failed to parse fork workflow request", "error", err)
		return fmt.Errorf("failed to parse fork workflow request: %w", err)
	}
	c.config.logger.Debug("Handling fork workflow request", "request", req)

	// Build ForkWorkflowInput from the request
	input := ForkWorkflowInput{
		OriginalWorkflowID: req.Body.WorkflowID,
		StartStep:          uint(req.Body.StartStep),
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
		c.config.logger.Error("Failed to fork workflow", "original_workflow_id", req.Body.WorkflowID, "error", err)
		errStr := fmt.Sprintf("failed to fork workflow: %v", err)
		errorMsg = &errStr
	} else {
		workflowID := handle.GetWorkflowID()
		newWorkflowID = &workflowID
		c.config.logger.Info("Successfully forked workflow", "original_workflow_id", req.Body.WorkflowID, "new_workflow_id", workflowID)
	}

	response := forkWorkflowConductorResponse{
		baseMessage: baseMessage{
			Type:      ForkWorkflowMessage,
			RequestID: requestID,
		},
		NewWorkflowID: newWorkflowID,
		ErrorMessage:  errorMsg,
	}

	return c.sendForkWorkflowResponse(response)
}

// sendForkWorkflowResponse sends a ForkWorkflowResponse to the conductor
func (c *Conductor) sendForkWorkflowResponse(response forkWorkflowConductorResponse) error {
	if c.conn == nil {
		return fmt.Errorf("no connection")
	}

	data, err := json.Marshal(response)
	if err != nil {
		return fmt.Errorf("failed to marshal fork workflow response: %w", err)
	}

	c.config.logger.Debug("Sending fork workflow response", "new_workflow_id", response.NewWorkflowID)

	if err := c.conn.WriteMessage(websocket.TextMessage, data); err != nil {
		c.config.logger.Error("Failed to send fork workflow response", "error", err)
		return fmt.Errorf("failed to send message: %w", err)
	}

	return nil
}

// sendErrorResponse sends an error response for unknown message types
func (c *Conductor) sendErrorResponse(requestID string, msgType MessageType, errorMsg string) error {
	if c.conn == nil {
		return fmt.Errorf("no connection")
	}

	response := BaseResponse{
		baseMessage: baseMessage{
			Type:      msgType,
			RequestID: requestID,
		},
		ErrorMessage: &errorMsg,
	}

	data, err := json.Marshal(response)
	if err != nil {
		c.config.logger.Error("Failed to marshal error response", "error", err)
		return fmt.Errorf("failed to marshal error response: %w", err)
	}

	c.config.logger.Debug("Sending error response", "data", response)

	if err := c.conn.WriteMessage(websocket.TextMessage, data); err != nil {
		c.config.logger.Error("Failed to send error response", "error", err)
		return fmt.Errorf("failed to send message: %w", err)
	}

	return nil
}
