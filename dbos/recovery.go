package dbos

import (
	"context"
	"fmt"
	"strings"
)

func recoverPendingWorkflows(ctx context.Context, executorIDs []string) ([]WorkflowHandle[any], error) {
	workflowHandles := make([]WorkflowHandle[any], 0)
	// List pending workflows for the executors
	pendingWorkflows, err := getExecutor().systemDB.ListWorkflows(ctx, ListWorkflowsDBInput{
		Status:             []WorkflowStatusType{WorkflowStatusPending},
		ExecutorIDs:        executorIDs,
		ApplicationVersion: APP_VERSION,
	})
	if err != nil {
		return nil, err
	}

	for _, workflow := range pendingWorkflows {
		if inputStr, ok := workflow.Input.(string); ok {
			if strings.Contains(inputStr, "Failed to decode") {
				fmt.Println("Skipping workflow recovery due to input decoding failure:", workflow.ID, "Name:", workflow.Name)
				continue
			}
		}

		fmt.Println("Recovering workflow:", workflow.ID, "Name:", workflow.Name, "Input:", workflow.Input, "QueueName:", workflow.QueueName)
		if workflow.QueueName != "" {
			cleared, err := getExecutor().systemDB.ClearQueueAssignment(ctx, workflow.ID)
			if err != nil {
				fmt.Println("Error clearing queue assignment for workflow:", workflow.ID, "Name:", workflow.Name, "Error:", err)
				continue
			}
			if cleared {
				workflowHandles = append(workflowHandles, &workflowPollingHandle[any]{workflowID: workflow.ID})
			}
			continue
		}

		registeredWorkflow, exists := registry[workflow.Name]
		if !exists {
			fmt.Println(NewWorkflowFunctionNotFoundError(workflow.ID, fmt.Sprintf("Workflow function %s not found in registry", workflow.Name)))
			continue
		}

		// Convert workflow parameters to options
		opts := []WorkflowOption{
			WithWorkflowID(workflow.ID),
		}
		if workflow.Timeout != 0 {
			opts = append(opts, WithTimeout(workflow.Timeout))
		}
		if !workflow.Deadline.IsZero() {
			opts = append(opts, WithDeadline(workflow.Deadline))
		}

		handle, err := registeredWorkflow(ctx, workflow.Input, opts...)
		if err != nil {
			fmt.Println("Error recovering workflow:", err)
		}
		workflowHandles = append(workflowHandles, handle)
	}

	return workflowHandles, nil
}
