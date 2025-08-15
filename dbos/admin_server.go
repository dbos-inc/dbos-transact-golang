package dbos

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"
)

const (
	// HTTP handler patterns with verbs
	_HEALTHCHECK_PATTERN              = "GET /dbos-healthz"
	_WORKFLOW_RECOVERY_PATTERN        = "POST /dbos-workflow-recovery"
	_DEACTIVATE_PATTERN               = "GET /deactivate"
	_WORKFLOW_QUEUES_METADATA_PATTERN = "GET /dbos-workflow-queues-metadata"
	_GARBAGE_COLLECT_PATTERN          = "POST /dbos-garbage-collect"
	_GLOBAL_TIMEOUT_PATTERN           = "POST /dbos-global-timeout"
	_QUEUED_WORKFLOWS_PATTERN         = "POST /queues"
	_WORKFLOWS_PATTERN                = "POST /workflows"
	_WORKFLOW_STEPS_PATTERN           = "GET /workflows/{id}/steps"
	_WORKFLOW_CANCEL_PATTERN          = "POST /workflows/{id}/cancel"
	_WORKFLOW_RESUME_PATTERN          = "POST /workflows/{id}/resume"
	_WORKFLOW_RESTART_PATTERN         = "POST /workflows/{id}/restart"
	_WORKFLOW_FORK_PATTERN            = "POST /workflows/{id}/fork"

	_ADMIN_SERVER_READ_HEADER_TIMEOUT = 5 * time.Second
	_ADMIN_SERVER_SHUTDOWN_TIMEOUT    = 10 * time.Second
)

// listWorkflowsRequest represents the request structure for listing workflows
type listWorkflowsRequest struct {
	WorkflowUUIDs      []string             `json:"workflow_uuids"`      // Filter by specific workflow IDs
	AuthenticatedUser  *string              `json:"authenticated_user"`  // Filter by user who initiated the workflow
	StartTime          *int64               `json:"start_time"`          // Filter workflows created after this time (UTC timestamp in milliseconds)
	EndTime            *int64               `json:"end_time"`            // Filter workflows created before this time (UTC timestamp in milliseconds)
	Status             []WorkflowStatusType `json:"status"`              // Filter by workflow status(es)
	ApplicationVersion *string              `json:"application_version"` // Filter by application version
	WorkflowName       *string              `json:"workflow_name"`       // Filter by workflow function name
	Limit              *int                 `json:"limit"`               // Maximum number of results to return
	Offset             *int                 `json:"offset"`              // Offset for pagination
	SortDesc           *bool                `json:"sort_desc"`           // Sort in descending order by creation time
	WorkflowIDPrefix   *string              `json:"workflow_id_prefix"`  // Filter by workflow ID prefix
	LoadInput          *bool                `json:"load_input"`          // Include workflow input in response
	LoadOutput         *bool                `json:"load_output"`         // Include workflow output in response
	QueueName          *string              `json:"queue_name"`          // Filter by queue name (for queued workflows)
}

// buildOptions converts the request struct into a slice of ListWorkflowsOption
func (req *listWorkflowsRequest) toListWorkflowsOptions() []ListWorkflowsOption {
	var opts []ListWorkflowsOption
	if len(req.WorkflowUUIDs) > 0 {
		opts = append(opts, WithWorkflowIDs(req.WorkflowUUIDs))
	}
	if req.AuthenticatedUser != nil {
		opts = append(opts, WithUser(*req.AuthenticatedUser))
	}
	if req.StartTime != nil {
		// Convert milliseconds to time.Time
		startTime := time.UnixMilli(*req.StartTime)
		opts = append(opts, WithStartTime(startTime))
	}
	if req.EndTime != nil {
		// Convert milliseconds to time.Time
		endTime := time.UnixMilli(*req.EndTime)
		opts = append(opts, WithEndTime(endTime))
	}
	if len(req.Status) > 0 {
		opts = append(opts, WithStatus(req.Status))
	}
	if req.ApplicationVersion != nil {
		opts = append(opts, WithAppVersion(*req.ApplicationVersion))
	}
	if req.WorkflowName != nil {
		opts = append(opts, WithName(*req.WorkflowName))
	}
	if req.Limit != nil {
		opts = append(opts, WithLimit(*req.Limit))
	}
	if req.Offset != nil {
		opts = append(opts, WithOffset(*req.Offset))
	}
	if req.SortDesc != nil {
		opts = append(opts, WithSortDesc(*req.SortDesc))
	}
	if req.WorkflowIDPrefix != nil {
		opts = append(opts, WithWorkflowIDPrefix(*req.WorkflowIDPrefix))
	}
	if req.LoadInput != nil {
		opts = append(opts, WithLoadInput(*req.LoadInput))
	}
	if req.LoadOutput != nil {
		opts = append(opts, WithLoadOutput(*req.LoadOutput))
	}
	if req.QueueName != nil {
		opts = append(opts, WithQueueName(*req.QueueName))
	}
	return opts
}

type adminServer struct {
	server        *http.Server
	logger        *slog.Logger
	port          int
	isDeactivated atomic.Int32
}

func newAdminServer(ctx *dbosContext, port int) *adminServer {
	as := &adminServer{
		logger: ctx.logger,
		port:   port,
	}

	mux := http.NewServeMux()

	mux.HandleFunc(_HEALTHCHECK_PATTERN, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(`{"status":"healthy"}`))
		if err != nil {
			ctx.logger.Error("Error writing health check response", "error", err)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}
	})

	mux.HandleFunc(_WORKFLOW_RECOVERY_PATTERN, func(w http.ResponseWriter, r *http.Request) {
		var executorIDs []string
		if err := json.NewDecoder(r.Body).Decode(&executorIDs); err != nil {
			http.Error(w, "Invalid JSON body", http.StatusBadRequest)
			return
		}

		ctx.logger.Info("Recovering workflows for executors", "executors", executorIDs)

		handles, err := recoverPendingWorkflows(ctx, executorIDs)
		if err != nil {
			ctx.logger.Error("Error recovering workflows", "error", err)
			http.Error(w, fmt.Sprintf("Recovery failed: %v", err), http.StatusInternalServerError)
			return
		}

		// Extract workflow IDs from handles
		workflowIDs := make([]string, len(handles))
		for i, handle := range handles {
			workflowIDs[i] = handle.GetWorkflowID()
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(workflowIDs); err != nil {
			ctx.logger.Error("Error encoding response", "error", err)
			http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
			return
		}
	})

	mux.HandleFunc(_DEACTIVATE_PATTERN, func(w http.ResponseWriter, r *http.Request) {
		if as.isDeactivated.CompareAndSwap(0, 1) {
			ctx.logger.Info("Deactivating DBOS executor", "executor_id", ctx.executorID, "app_version", ctx.applicationVersion)
			// TODO: Stop queue runner, workflow scheduler, etc
		}

		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte("deactivated")); err != nil {
			ctx.logger.Error("Error writing deactivate response", "error", err)
		}
	})

	mux.HandleFunc(_WORKFLOW_QUEUES_METADATA_PATTERN, func(w http.ResponseWriter, r *http.Request) {
		queueMetadataArray := ctx.queueRunner.listQueues()

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(queueMetadataArray); err != nil {
			ctx.logger.Error("Error encoding queue metadata response", "error", err)
			http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
			return
		}
	})

	mux.HandleFunc(_GARBAGE_COLLECT_PATTERN, func(w http.ResponseWriter, r *http.Request) {
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
		//     ctx.logger.Error("Garbage collection failed", "error", err)
		//     http.Error(w, fmt.Sprintf("Garbage collection failed: %v", err), http.StatusInternalServerError)
		//     return
		// }

		w.WriteHeader(http.StatusNoContent)
	})

	mux.HandleFunc(_GLOBAL_TIMEOUT_PATTERN, func(w http.ResponseWriter, r *http.Request) {
		var inputs struct {
			CutoffEpochTimestampMs *int64 `json:"cutoff_epoch_timestamp_ms"`
		}

		if err := json.NewDecoder(r.Body).Decode(&inputs); err != nil {
			http.Error(w, "Invalid JSON body", http.StatusBadRequest)
			return
		}

		// TODO: Implement global timeout
		// err := globalTimeout(ctx, inputs.CutoffEpochTimestampMs)
		// if err != nil {
		//     ctx.logger.Error("Global timeout failed", "error", err)
		//     http.Error(w, fmt.Sprintf("Global timeout failed: %v", err), http.StatusInternalServerError)
		//     return
		// }

		w.WriteHeader(http.StatusNoContent)
	})

	mux.HandleFunc(_WORKFLOWS_PATTERN, func(w http.ResponseWriter, r *http.Request) {
		var req listWorkflowsRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, fmt.Sprintf("Invalid JSON input: %v", err), http.StatusBadRequest)
			return
		}

		workflows, err := ctx.ListWorkflows(req.toListWorkflowsOptions()...)
		if err != nil {
			ctx.logger.Error("Failed to list workflows", "error", err)
			http.Error(w, fmt.Sprintf("Failed to list workflows: %v", err), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(workflows); err != nil {
			ctx.logger.Error("Error encoding workflows response", "error", err)
			http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
		}
	})

	mux.HandleFunc(_QUEUED_WORKFLOWS_PATTERN, func(w http.ResponseWriter, r *http.Request) {
		var req listWorkflowsRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, fmt.Sprintf("Invalid JSON input: %v", err), http.StatusBadRequest)
			return
		}

		workflows, err := ctx.ListWorkflows(req.toListWorkflowsOptions()...)
		if err != nil {
			ctx.logger.Error("Failed to list queued workflows", "error", err)
			http.Error(w, fmt.Sprintf("Failed to list queued workflows: %v", err), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(workflows); err != nil {
			ctx.logger.Error("Error encoding queued workflows response", "error", err)
			http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
		}
	})

	// GET /workflows/{id}/steps
	mux.HandleFunc(_WORKFLOW_STEPS_PATTERN, func(w http.ResponseWriter, r *http.Request) {
		workflowID := r.PathValue("id")

		steps, err := ctx.systemDB.getWorkflowSteps(ctx, workflowID)
		if err != nil {
			ctx.logger.Error("Failed to list workflow steps", "workflow_id", workflowID, "error", err)
			http.Error(w, fmt.Sprintf("Failed to list steps: %v", err), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(steps); err != nil {
			ctx.logger.Error("Error encoding steps response", "error", err)
			http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
		}
	})

	// POST /workflows/{id}/cancel
	mux.HandleFunc(_WORKFLOW_CANCEL_PATTERN, func(w http.ResponseWriter, r *http.Request) {
		workflowID := r.PathValue("id")
		ctx.logger.Info("Cancelling workflow", "workflow_id", workflowID)

		err := ctx.CancelWorkflow(workflowID)
		if err != nil {
			ctx.logger.Error("Failed to cancel workflow", "workflow_id", workflowID, "error", err)
			http.Error(w, fmt.Sprintf("Failed to cancel workflow: %v", err), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	})

	// POST /workflows/{id}/resume
	mux.HandleFunc(_WORKFLOW_RESUME_PATTERN, func(w http.ResponseWriter, r *http.Request) {
		workflowID := r.PathValue("id")
		ctx.logger.Info("Resuming workflow", "workflow_id", workflowID)

		_, err := ctx.ResumeWorkflow(ctx, workflowID)
		if err != nil {
			ctx.logger.Error("Failed to resume workflow", "workflow_id", workflowID, "error", err)
			http.Error(w, fmt.Sprintf("Failed to resume workflow: %v", err), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	})

	mux.HandleFunc(_WORKFLOW_RESTART_PATTERN, func(w http.ResponseWriter, r *http.Request) {
		workflowID := r.PathValue("id")
		ctx.logger.Info("Restarting workflow", "workflow_id", workflowID)

		// Restart is like forking but starting at step 0
		input := ForkWorkflowInput{
			OriginalWorkflowID: workflowID,
		}

		handle, err := ctx.ForkWorkflow(ctx, input)
		if err != nil {
			ctx.logger.Error("Failed to restart workflow", "workflow_id", workflowID, "error", err)
			http.Error(w, fmt.Sprintf("Failed to restart workflow: %v", err), http.StatusInternalServerError)
			return
		}

		response := map[string]string{
			"workflow_id": handle.GetWorkflowID(),
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(response); err != nil {
			ctx.logger.Error("Error encoding restart response", "error", err)
			http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
		}
	})

	mux.HandleFunc(_WORKFLOW_FORK_PATTERN, func(w http.ResponseWriter, r *http.Request) {
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
		input := ForkWorkflowInput{
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

		ctx.logger.Info("Forking workflow", "workflow_id", workflowID, "start_step", input.StartStep)

		handle, err := ctx.ForkWorkflow(ctx, input)
		if err != nil {
			ctx.logger.Error("Failed to fork workflow", "workflow_id", workflowID, "error", err)
			http.Error(w, fmt.Sprintf("Failed to fork workflow: %v", err), http.StatusInternalServerError)
			return
		}

		response := map[string]string{
			"workflow_id": handle.GetWorkflowID(),
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(response); err != nil {
			ctx.logger.Error("Error encoding fork response", "error", err)
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

func (as *adminServer) Start() error {
	as.logger.Info("Starting admin server", "port", as.port)

	go func() {
		if err := as.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			as.logger.Error("Admin server error", "error", err)
		}
	}()

	return nil
}

func (as *adminServer) Shutdown(ctx context.Context) error {
	as.logger.Info("Shutting down admin server")

	// Note: consider moving the grace period to DBOSContext.Shutdown()
	ctx, cancel := context.WithTimeout(ctx, _ADMIN_SERVER_SHUTDOWN_TIMEOUT)
	defer cancel()

	if err := as.server.Shutdown(ctx); err != nil {
		as.logger.Error("Admin server shutdown error", "error", err)
		return fmt.Errorf("failed to shutdown admin server: %w", err)
	}

	as.logger.Info("Admin server shutdown complete")
	return nil
}
