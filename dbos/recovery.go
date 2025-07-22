package dbos

import (
	"context"
	"strings"
)

func recoverPendingWorkflows(ctx context.Context, executorIDs []string) ([]WorkflowHandle[any], error) {
	workflowHandles := make([]WorkflowHandle[any], 0)
	// List pending workflows for the executors
	pendingWorkflows, err := dbos.systemDB.ListWorkflows(ctx, listWorkflowsDBInput{
		status:             []WorkflowStatusType{WorkflowStatusPending},
		executorIDs:        executorIDs,
		applicationVersion: _APP_VERSION,
	})
	if err != nil {
		return nil, err
	}

	for _, workflow := range pendingWorkflows {
		if inputStr, ok := workflow.Input.(string); ok {
			if strings.Contains(inputStr, "Failed to decode") {
				getLogger().Warn("Skipping workflow recovery due to input decoding failure", "workflow_id", workflow.ID, "name", workflow.Name)
				continue
			}
		}

		// fmt.Println("Recovering workflow:", workflow.ID, "Name:", workflow.Name, "Input:", workflow.Input, "QueueName:", workflow.QueueName)
		if workflow.QueueName != "" {
			cleared, err := dbos.systemDB.ClearQueueAssignment(ctx, workflow.ID)
			if err != nil {
				getLogger().Error("Error clearing queue assignment for workflow", "workflow_id", workflow.ID, "name", workflow.Name, "error", err)
				continue
			}
			if cleared {
				workflowHandles = append(workflowHandles, &workflowPollingHandle[any]{workflowID: workflow.ID})
			}
			continue
		}

		registeredWorkflow, exists := registry[workflow.Name]
		if !exists {
			getLogger().Error("Workflow function not found in registry", "workflow_id", workflow.ID, "name", workflow.Name)
			continue
		}

		// Convert workflow parameters to options
		opts := []workflowOption{
			WithWorkflowID(workflow.ID),
		}
		// XXX we'll figure out the exact timeout/deadline settings later
		if workflow.Timeout != 0 {
			opts = append(opts, WithTimeout(workflow.Timeout))
		}
		if !workflow.Deadline.IsZero() {
			opts = append(opts, WithDeadline(workflow.Deadline))
		}

		handle, err := registeredWorkflow.wrappedFunction(ctx, workflow.Input, opts...)
		if err != nil {
			return nil, err
		}
		workflowHandles = append(workflowHandles, handle)
	}

	return workflowHandles, nil
}
