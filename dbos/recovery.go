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

		fmt.Println("Recovering workflow:", workflow.ID, "Name:", workflow.Name, "Input:", workflow.Input)
		// TODO: handle clearing queue assignment if needed, and append a polling handle
		if workflow.QueueName != "" {
			continue
		}
		registeredWorkflow, exists := registry[workflow.Name]
		if !exists {
			fmt.Println("Error: workflow function not found in registry:", workflow.Name)
			continue
		}

		// Rebuild the parameters
		params := WorkflowParams{
			WorkflowID: workflow.ID,
			Timeout:    workflow.Timeout,
			Deadline:   workflow.Deadline,
		}

		handle, err := registeredWorkflow(ctx, params, workflow.Input)
		if err != nil {
			fmt.Println("Error recovering workflow:", err)
		}
		workflowHandles = append(workflowHandles, handle)
	}

	return workflowHandles, nil
}
