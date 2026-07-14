package adminserver

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/models"
)

// Executor is the narrow surface of the DBOS runtime the admin server needs.
// It is implemented by an adapter in the dbos package.
type Executor interface {
	ListWorkflows(ctx context.Context, opts ...models.ListWorkflowsOption) ([]models.WorkflowStatus, error)
	GetWorkflowSteps(ctx context.Context, workflowID string) ([]models.StepInfo, error)
	CancelWorkflow(ctx context.Context, workflowID string) error
	ResumeWorkflow(ctx context.Context, workflowID string) error
	// ForkWorkflow returns the ID of the newly forked workflow.
	ForkWorkflow(ctx context.Context, input models.ForkWorkflowInput) (string, error)
	// RecoverPendingWorkflows returns the IDs of the recovered workflows.
	RecoverPendingWorkflows(ctx context.Context, executorIDs []string) ([]string, error)
	// CancelAllBefore cancels all pending/enqueued workflows created before the cutoff.
	CancelAllBefore(ctx context.Context, cutoff time.Time) error
	// QueueMetadata lists the queues registered on this executor.
	QueueMetadata() []models.QueueConfig
	// Deactivate stops this executor's scheduler so it winds down gracefully.
	Deactivate()
}

const (
	// HTTP handler patterns with verbs
	HealthcheckPattern            = "GET /dbos-healthz"
	WorkflowRecoveryPattern       = "POST /dbos-workflow-recovery"
	DeactivatePattern             = "GET /deactivate"
	WorkflowQueuesMetadataPattern = "GET /dbos-workflow-queues-metadata"
	GarbageCollectPattern         = "POST /dbos-garbage-collect"
	GlobalTimeoutPattern          = "POST /dbos-global-timeout"
	QueuedWorkflowsPattern        = "POST /queues"
	WorkflowsPattern              = "POST /workflows"
	WorkflowPattern               = "GET /workflows/{id}"
	WorkflowStepsPattern          = "GET /workflows/{id}/steps"
	WorkflowCancelPattern         = "POST /workflows/{id}/cancel"
	WorkflowResumePattern         = "POST /workflows/{id}/resume"
	WorkflowForkPattern           = "POST /workflows/{id}/fork"
	ConductorPattern              = "GET /conductor"

	_ADMIN_SERVER_READ_HEADER_TIMEOUT = 5 * time.Second
)

// stringOrSlice unmarshals a JSON value that is either a single string ("X")
// or an array of strings (["X","Y"]). This matches the status filter contract
// used by the DBOS console and the Python/TypeScript SDKs.
type stringOrSlice []string

func (s *stringOrSlice) UnmarshalJSON(data []byte) error {
	var single string
	if err := json.Unmarshal(data, &single); err == nil {
		*s = []string{single}
		return nil
	}
	var many []string
	if err := json.Unmarshal(data, &many); err != nil {
		return err
	}
	*s = many
	return nil
}

// ListWorkflowsRequest represents the request structure for listing workflows
type ListWorkflowsRequest struct {
	WorkflowUUIDs      []string      `json:"workflow_uuids"`      // Filter by specific workflow IDs
	AuthenticatedUser  *string       `json:"authenticated_user"`  // Filter by user who initiated the workflow
	StartTime          *time.Time    `json:"start_time"`          // Filter workflows created after this time (RFC3339 format)
	EndTime            *time.Time    `json:"end_time"`            // Filter workflows created before this time (RFC3339 format)
	Status             stringOrSlice `json:"status"`              // Filter by workflow status (string or array of strings)
	ApplicationVersion *string       `json:"application_version"` // Filter by application version
	WorkflowName       *string       `json:"workflow_name"`       // Filter by workflow function name
	Limit              *int          `json:"limit"`               // Maximum number of results to return
	Offset             *int          `json:"offset"`              // Offset for pagination
	SortDesc           *bool         `json:"sort_desc"`           // Sort in descending order by creation time
	WorkflowIDPrefix   *string       `json:"workflow_id_prefix"`  // Filter by workflow ID prefix
	LoadInput          *bool         `json:"load_input"`          // Include workflow input in response
	LoadOutput         *bool         `json:"load_output"`         // Include workflow output in response
	QueueName          *string       `json:"queue_name"`          // Filter by queue name (for queued workflows)
}

// buildOptions converts the request struct into a slice of models.ListWorkflowsOption
func (req *ListWorkflowsRequest) ToListWorkflowsOptions() []models.ListWorkflowsOption {
	var opts []models.ListWorkflowsOption
	if len(req.WorkflowUUIDs) > 0 {
		opts = append(opts, models.WithWorkflowIDs(req.WorkflowUUIDs))
	}
	if req.AuthenticatedUser != nil {
		opts = append(opts, models.WithUser(*req.AuthenticatedUser))
	}
	if req.StartTime != nil {
		opts = append(opts, models.WithStartTime(*req.StartTime))
	}
	if req.EndTime != nil {
		opts = append(opts, models.WithEndTime(*req.EndTime))
	}
	if len(req.Status) > 0 {
		statuses := make([]models.WorkflowStatusType, len(req.Status))
		for i, s := range req.Status {
			statuses[i] = models.WorkflowStatusType(s)
		}
		opts = append(opts, models.WithStatus(statuses))
	}
	if req.ApplicationVersion != nil {
		opts = append(opts, models.WithAppVersion(*req.ApplicationVersion))
	}
	if req.WorkflowName != nil {
		opts = append(opts, models.WithName(*req.WorkflowName))
	}
	if req.Limit != nil {
		opts = append(opts, models.WithLimit(*req.Limit))
	}
	if req.Offset != nil {
		opts = append(opts, models.WithOffset(*req.Offset))
	}
	if req.SortDesc != nil {
		opts = append(opts, models.WithSortDesc())
	}
	if req.WorkflowIDPrefix != nil {
		opts = append(opts, models.WithWorkflowIDPrefix(*req.WorkflowIDPrefix))
	}
	if req.LoadInput != nil {
		opts = append(opts, models.WithLoadInput(*req.LoadInput))
	}
	if req.LoadOutput != nil {
		opts = append(opts, models.WithLoadOutput(*req.LoadOutput))
	}
	if req.QueueName != nil {
		opts = append(opts, models.WithQueueName(*req.QueueName))
	}
	return opts
}

type Server struct {
	server        *http.Server
	logger        *slog.Logger
	port          int
	isDeactivated atomic.Int32
	wg            sync.WaitGroup
}

// toListWorkflowResponse converts a models.WorkflowStatus to a map with all time fields in UTC
// not super ergonomic but the DBOS console excepts unix timestamps
func toListWorkflowResponse(ws models.WorkflowStatus) (map[string]any, error) {
	result := map[string]any{
		"WorkflowUUID":       ws.ID,
		"Status":             ws.Status,
		"WorkflowName":       ws.Name,
		"AuthenticatedUser":  ws.AuthenticatedUser,
		"AssumedRole":        ws.AssumedRole,
		"AuthenticatedRoles": ws.AuthenticatedRoles,
		"Output":             ws.Output,
		"ExecutorID":         ws.ExecutorID,
		"ApplicationVersion": ws.ApplicationVersion,
		"ApplicationID":      ws.ApplicationID,
		"Attempts":           ws.Attempts,
		"QueueName":          ws.QueueName,
		"Timeout":            ws.Timeout,
		"DeduplicationID":    ws.DeduplicationID,
		"Priority":           ws.Priority,
		"QueuePartitionKey":  ws.QueuePartitionKey,
		"Input":              ws.Input,
	}

	formatEpochMs := func(t time.Time) any {
		if t.IsZero() {
			return nil
		}
		return strconv.FormatInt(t.UTC().UnixMilli(), 10)
	}

	result["CreatedAt"] = formatEpochMs(ws.CreatedAt)
	result["UpdatedAt"] = formatEpochMs(ws.UpdatedAt)
	result["WorkflowDeadlineEpochMS"] = formatEpochMs(ws.Deadline)
	result["StartedAt"] = formatEpochMs(ws.StartedAt)

	if ws.Input != nil {
		// If there is a value, it should be a JSON string
		jsonInput, ok := ws.Input.(string)
		if ok {
			result["Input"] = jsonInput
		} else {
			result["Input"] = ""
		}
	}

	if ws.Output != nil {
		jsonOutput, ok := ws.Output.(string)
		if ok {
			result["Output"] = jsonOutput
		} else {
			result["Output"] = ""
		}
	}

	if ws.Error != nil {
		// Convert error to string first, then marshal as JSON
		errStr := ws.Error.Error()
		bytes, err := json.Marshal(errStr)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal error: %w", err)
		}
		result["Error"] = string(bytes)
	} else {
		result["Error"] = ""
	}

	return result, nil
}

func New(exec Executor, logger *slog.Logger, port int) *Server {
	as := &Server{
		logger: logger,
		port:   port,
	}

	mux := http.NewServeMux()

	logger.Debug("Registering admin server endpoint", "pattern", HealthcheckPattern)
	mux.HandleFunc(HealthcheckPattern, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(`{"status":"healthy"}`))
		if err != nil {
			logger.Error("Error writing health check response", "error", err)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}
	})

	logger.Debug("Registering admin server endpoint", "pattern", WorkflowRecoveryPattern)
	mux.HandleFunc(WorkflowRecoveryPattern, func(w http.ResponseWriter, r *http.Request) {
		var executorIDs []string
		if err := json.NewDecoder(r.Body).Decode(&executorIDs); err != nil {
			http.Error(w, "Invalid JSON body", http.StatusBadRequest)
			return
		}

		logger.Info("Recovering workflows for executors", "executors", executorIDs)

		workflowIDs, err := exec.RecoverPendingWorkflows(r.Context(), executorIDs)
		if err != nil {
			logger.Error("Error recovering workflows", "error", err)
			http.Error(w, fmt.Sprintf("Recovery failed: %v", err), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(workflowIDs); err != nil {
			logger.Error("Error encoding response", "error", err)
			http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
			return
		}
	})

	logger.Debug("Registering admin server endpoint", "pattern", DeactivatePattern)
	mux.HandleFunc(DeactivatePattern, func(w http.ResponseWriter, r *http.Request) {
		if as.isDeactivated.CompareAndSwap(0, 1) {
			exec.Deactivate()
		}

		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte("deactivated")); err != nil {
			logger.Error("Error writing deactivate response", "error", err)
		}
	})

	logger.Debug("Registering admin server endpoint", "pattern", ConductorPattern)
	mux.HandleFunc(ConductorPattern, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte(`{"status":true}`)); err != nil {
			logger.Error("Error writing conductor response", "error", err)
		}
	})

	logger.Debug("Registering admin server endpoint", "pattern", WorkflowQueuesMetadataPattern)
	mux.HandleFunc(WorkflowQueuesMetadataPattern, func(w http.ResponseWriter, r *http.Request) {
		queueMetadataArray := exec.QueueMetadata()

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(queueMetadataArray); err != nil {
			logger.Error("Error encoding queue metadata response", "error", err)
			http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
			return
		}
	})

	logger.Debug("Registering admin server endpoint", "pattern", GarbageCollectPattern)
	mux.HandleFunc(GarbageCollectPattern, func(w http.ResponseWriter, r *http.Request) {
		var inputs struct {
			CutoffEpochTimestampMs *int64 `json:"cutoff_epoch_timestamp_ms"`
			RowsThreshold          *int   `json:"rows_threshold"`
		}

		if err := json.NewDecoder(r.Body).Decode(&inputs); err != nil {
			http.Error(w, "Invalid JSON body", http.StatusBadRequest)
			return
		}

		// TODO: Implement garbage collection
		// err := garbageCollect(ctx, inputs.CutoffEpochTimestampMs, inputs.RowsThreshold)
		// if err != nil {
		//     logger.Error("Garbage collection failed", "error", err)
		//     http.Error(w, fmt.Sprintf("Garbage collection failed: %v", err), http.StatusInternalServerError)
		//     return
		// }

		w.WriteHeader(http.StatusNoContent)
	})

	logger.Debug("Registering admin server endpoint", "pattern", GlobalTimeoutPattern)
	mux.HandleFunc(GlobalTimeoutPattern, func(w http.ResponseWriter, r *http.Request) {
		var inputs struct {
			CutoffEpochTimestampMs int64 `json:"cutoff_epoch_timestamp_ms"`
		}

		if err := json.NewDecoder(r.Body).Decode(&inputs); err != nil {
			http.Error(w, "Invalid JSON body", http.StatusBadRequest)
			return
		}

		cutoffTime := time.UnixMilli(inputs.CutoffEpochTimestampMs)
		logger.Info("Global timeout request", "cutoff_time", cutoffTime)

		err := exec.CancelAllBefore(r.Context(), cutoffTime)
		if err != nil {
			logger.Error("Global timeout failed", "error", err)
			http.Error(w, fmt.Sprintf("Global timeout failed: %v", err), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	})

	logger.Debug("Registering admin server endpoint", "pattern", WorkflowsPattern)
	mux.HandleFunc(WorkflowsPattern, func(w http.ResponseWriter, r *http.Request) {
		var req ListWorkflowsRequest
		if r.ContentLength > 0 {
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, fmt.Sprintf("Invalid JSON input: %v", err), http.StatusBadRequest)
				return
			}
		}

		workflows, err := exec.ListWorkflows(r.Context(), req.ToListWorkflowsOptions()...)
		if err != nil {
			logger.Error("Failed to list workflows", "error", err)
			http.Error(w, fmt.Sprintf("Failed to list workflows: %v", err), http.StatusInternalServerError)
			return
		}

		// Transform to UTC before encoding
		responseWorkflows := make([]map[string]any, len(workflows))
		for i, wf := range workflows {
			responseWorkflows[i], err = toListWorkflowResponse(wf)
			if err != nil {
				logger.Error("Error transforming workflow response", "error", err)
				http.Error(w, fmt.Sprintf("Failed to format workflow response: %v", err), http.StatusInternalServerError)
				return
			}
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(responseWorkflows); err != nil {
			logger.Error("Error encoding workflows response", "error", err)
			http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
		}
	})

	logger.Debug("Registering admin server endpoint", "pattern", WorkflowPattern)
	mux.HandleFunc(WorkflowPattern, func(w http.ResponseWriter, r *http.Request) {
		workflowID := r.PathValue("id")

		// Use ListWorkflows with the specific workflow ID filter
		opts := []models.ListWorkflowsOption{models.WithWorkflowIDs([]string{workflowID})}
		workflows, err := exec.ListWorkflows(r.Context(), opts...)
		if err != nil {
			logger.Error("Failed to get workflow", "workflow_id", workflowID, "error", err)
			http.Error(w, fmt.Sprintf("Failed to get workflow: %v", err), http.StatusInternalServerError)
			return
		}

		// If no workflow found, return 404
		if len(workflows) == 0 {
			http.Error(w, "Workflow not found", http.StatusNotFound)
			return
		}

		// Return the first (and only) workflow, transformed to UTC
		workflow, err := toListWorkflowResponse(workflows[0])
		if err != nil {
			logger.Error("Error transforming workflow response", "error", err)
			http.Error(w, fmt.Sprintf("Failed to format workflow response: %v", err), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(workflow); err != nil {
			logger.Error("Error encoding workflow response", "error", err)
			http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
		}
	})

	logger.Debug("Registering admin server endpoint", "pattern", QueuedWorkflowsPattern)
	mux.HandleFunc(QueuedWorkflowsPattern, func(w http.ResponseWriter, r *http.Request) {
		var req ListWorkflowsRequest
		if r.ContentLength > 0 {
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, fmt.Sprintf("Invalid JSON input: %v", err), http.StatusBadRequest)
				return
			}
		}

		filters := req.ToListWorkflowsOptions()
		if len(req.Status) == 0 {
			filters = append(filters, models.WithStatus([]models.WorkflowStatusType{models.WorkflowStatusEnqueued, models.WorkflowStatusPending, models.WorkflowStatusDelayed}))
		}
		filters = append(filters, models.WithQueuesOnly())
		workflows, err := exec.ListWorkflows(r.Context(), filters...)
		if err != nil {
			logger.Error("Failed to list queued workflows", "error", err)
			http.Error(w, fmt.Sprintf("Failed to list queued workflows: %v", err), http.StatusInternalServerError)
			return
		}

		// Transform to UNIX timestamps before encoding
		responseWorkflows := make([]map[string]any, len(workflows))
		for i, wf := range workflows {
			responseWorkflows[i], err = toListWorkflowResponse(wf)
			if err != nil {
				logger.Error("Error transforming workflow response", "error", err)
				http.Error(w, fmt.Sprintf("Failed to format workflow response: %v", err), http.StatusInternalServerError)
				return
			}
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(responseWorkflows); err != nil {
			logger.Error("Error encoding queued workflows response", "error", err)
			http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
		}
	})

	logger.Debug("Registering admin server endpoint", "pattern", WorkflowStepsPattern)
	mux.HandleFunc(WorkflowStepsPattern, func(w http.ResponseWriter, r *http.Request) {
		workflowID := r.PathValue("id")

		steps, err := exec.GetWorkflowSteps(r.Context(), workflowID)
		if err != nil {
			logger.Error("Failed to list workflow steps", "workflow_id", workflowID, "error", err)
			http.Error(w, fmt.Sprintf("Failed to list steps: %v", err), http.StatusInternalServerError)
			return
		}

		// Transform to snake_case format with function_id and function_name
		formattedSteps := make([]map[string]any, len(steps))
		for i, step := range steps {
			formattedStep := map[string]any{
				"function_id":       step.StepID,
				"function_name":     step.StepName,
				"child_workflow_id": step.ChildWorkflowID,
			}

			// Add timestamps if present
			if !step.StartedAt.IsZero() {
				formattedStep["started_at_epoch_ms"] = step.StartedAt.UnixMilli()
			}
			if !step.CompletedAt.IsZero() {
				formattedStep["completed_at_epoch_ms"] = step.CompletedAt.UnixMilli()
			}

			if step.Output != nil {
				// If there is a value, it should be a JSON string
				jsonOutput, ok := step.Output.(string)
				if ok {
					formattedStep["output"] = jsonOutput
				} else {
					formattedStep["output"] = ""
				}
			} else {
				formattedStep["output"] = ""
			}

			// Marshal Error as JSON string if present
			if step.Error != nil {
				// Convert error to string first, then marshal as JSON
				errStr := step.Error.Error()
				bytes, err := json.Marshal(errStr)
				if err != nil {
					logger.Error("Failed to marshal step error", "error", err)
					http.Error(w, fmt.Sprintf("Failed to format step error: %v", err), http.StatusInternalServerError)
					return
				}
				formattedStep["error"] = string(bytes)
			}

			formattedSteps[i] = formattedStep
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(formattedSteps); err != nil {
			logger.Error("Error encoding steps response", "error", err)
			http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
		}
	})

	logger.Debug("Registering admin server endpoint", "pattern", WorkflowCancelPattern)
	mux.HandleFunc(WorkflowCancelPattern, func(w http.ResponseWriter, r *http.Request) {
		workflowID := r.PathValue("id")
		logger.Info("Cancelling workflow", "workflow_id", workflowID)

		err := exec.CancelWorkflow(r.Context(), workflowID)
		if err != nil {
			logger.Error("Failed to cancel workflow", "workflow_id", workflowID, "error", err)
			http.Error(w, fmt.Sprintf("Failed to cancel workflow: %v", err), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	})

	logger.Debug("Registering admin server endpoint", "pattern", WorkflowResumePattern)
	mux.HandleFunc(WorkflowResumePattern, func(w http.ResponseWriter, r *http.Request) {
		workflowID := r.PathValue("id")
		logger.Info("Resuming workflow", "workflow_id", workflowID)

		err := exec.ResumeWorkflow(r.Context(), workflowID)
		if err != nil {
			logger.Error("Failed to resume workflow", "workflow_id", workflowID, "error", err)
			http.Error(w, fmt.Sprintf("Failed to resume workflow: %v", err), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	})

	logger.Debug("Registering admin server endpoint", "pattern", WorkflowForkPattern)
	mux.HandleFunc(WorkflowForkPattern, func(w http.ResponseWriter, r *http.Request) {
		workflowID := r.PathValue("id")
		var data struct {
			StartStep          *uint   `json:"start_step"`
			ForkedWorkflowID   *string `json:"new_workflow_id"`
			ApplicationVersion *string `json:"application_version"`
		}

		if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
			http.Error(w, fmt.Sprintf("Invalid JSON input: %v", err), http.StatusBadRequest)
			return
		}

		// Prepare fork input
		input := models.ForkWorkflowInput{
			OriginalWorkflowID: workflowID,
		}
		if data.StartStep != nil {
			input.StartStep = *data.StartStep
		}
		if data.ForkedWorkflowID != nil {
			input.ForkedWorkflowID = *data.ForkedWorkflowID
		}
		if data.ApplicationVersion != nil {
			input.ApplicationVersion = *data.ApplicationVersion
		}

		logger.Info("Forking workflow", "workflow_id", workflowID, "start_step", input.StartStep)

		newWorkflowID, err := exec.ForkWorkflow(r.Context(), input)
		if err != nil {
			logger.Error("Failed to fork workflow", "workflow_id", workflowID, "error", err)
			http.Error(w, fmt.Sprintf("Failed to fork workflow: %v", err), http.StatusInternalServerError)
			return
		}

		response := map[string]string{
			"workflow_id": newWorkflowID,
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(response); err != nil {
			logger.Error("Error encoding fork response", "error", err)
			http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
		}
	})

	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           mux,
		ReadHeaderTimeout: _ADMIN_SERVER_READ_HEADER_TIMEOUT,
	}

	as.server = server
	return as
}

func (as *Server) Start() error {
	as.logger.Info("Starting admin server", "port", as.port)

	as.wg.Add(1)
	go func() {
		defer as.wg.Done()
		if err := as.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			as.logger.Error("Admin server error", "error", err)
		}
	}()

	return nil
}

func (as *Server) Shutdown(timeout time.Duration) error {
	as.logger.Info("Shutting down admin server")

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if err := as.server.Shutdown(ctx); err != nil {
		as.logger.Error("Admin server shutdown error", "error", err)
		return fmt.Errorf("failed to shutdown admin server: %w", err)
	}

	// Wait for the server goroutine to return
	done := make(chan struct{})
	go func() {
		as.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		as.logger.Info("Admin server shutdown complete")
	case <-ctx.Done():
		as.logger.Warn("Admin server shutdown timed out")
	}

	return nil
}
