package dbos

import (
	"strings"
)

func recoverPendingWorkflows(dbosCtx *dbosContext, executorIDs []string) ([]WorkflowHandle[any], error) {
	workflowHandles := make([]WorkflowHandle[any], 0)
	// List pending workflows for the executors
	pendingWorkflows, err := dbosCtx.systemDB.ListWorkflows(dbosCtx.ctx, listWorkflowsDBInput{
		status:             []WorkflowStatusType{WorkflowStatusPending},
		executorIDs:        executorIDs,
		applicationVersion: dbosCtx.applicationVersion,
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
			cleared, err := dbosCtx.systemDB.ClearQueueAssignment(dbosCtx.ctx, workflow.ID)
			if err != nil {
				getLogger().Error("Error clearing queue assignment for workflow", "workflow_id", workflow.ID, "name", workflow.Name, "error", err)
				continue
			}
			if cleared {
				workflowHandles = append(workflowHandles, &workflowPollingHandle[any]{workflowID: workflow.ID, dbosContext: dbosCtx})
			}
			continue
		}

		registeredWorkflow, exists := dbosCtx.workflowRegistry[workflow.Name]
		if !exists {
			getLogger().Error("Workflow function not found in registry", "workflow_id", workflow.ID, "name", workflow.Name)
			continue
		}

		// Convert workflow parameters to options
		opts := []WorkflowOption{
			WithWorkflowID(workflow.ID),
		}
		// XXX we'll figure out the exact timeout/deadline settings later
		if workflow.Timeout != 0 {
			opts = append(opts, WithTimeout(workflow.Timeout))
		}
		if !workflow.Deadline.IsZero() {
			opts = append(opts, WithDeadline(workflow.Deadline))
		}

		// Create a workflow context from the executor context
		workflowCtx := dbosCtx.withValue(dbosCtx.ctx, nil)
		handle, err := registeredWorkflow.wrappedFunction(workflowCtx, workflow.Input, opts...)
		if err != nil {
			return nil, err
		}
		workflowHandles = append(workflowHandles, handle)
	}

	return workflowHandles, nil
}
