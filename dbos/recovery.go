package dbos

func recoverPendingWorkflows(ctx *dbosContext, executorIDs []string) ([]WorkflowHandle[any], error) {
	workflowHandles := make([]WorkflowHandle[any], 0)
	// List pending workflows for the executors
	pendingWorkflows, err := ctx.systemDB.listWorkflows(ctx, listWorkflowsDBInput{
		status:             []WorkflowStatusType{WorkflowStatusPending},
		executorIDs:        executorIDs,
		applicationVersion: ctx.applicationVersion,
		loadInput:          true,
	})
	if err != nil {
		return nil, err
	}

	for _, workflow := range pendingWorkflows {
		// Deserialize the workflow input
		var decodedInput any
		if workflow.Input != nil {
			inputStr, ok := workflow.Input.(string)
			if !ok {
				ctx.logger.Warn("Skipping workflow recovery: input is not an encoded string", "workflow_id", workflow.ID, "name", workflow.Name, "input_type", workflow.Input)
				continue
			}
			if inputStr != "" {
				decodedInput, err = deserialize(&inputStr)
				if err != nil {
					ctx.logger.Warn("Skipping workflow recovery due to input decoding failure", "workflow_id", workflow.ID, "name", workflow.Name, "error", err)
					continue
				}
			}
		}

		if workflow.QueueName != "" {
			cleared, err := ctx.systemDB.clearQueueAssignment(ctx, workflow.ID)
			if err != nil {
				ctx.logger.Error("Error clearing queue assignment for workflow", "workflow_id", workflow.ID, "name", workflow.Name, "error", err)
				continue
			}
			if cleared {
				workflowHandles = append(workflowHandles, newWorkflowPollingHandle[any](ctx, workflow.ID))
			}
			continue
		}

		wfName, ok := ctx.workflowCustomNametoFQN.Load(workflow.Name)
		if !ok {
			ctx.logger.Error("Workflow not found in registry", "workflow_name", workflow.Name)
			continue
		}

		registeredWorkflowAny, exists := ctx.workflowRegistry.Load(wfName.(string))
		if !exists {
			ctx.logger.Error("Workflow function not found in registry", "workflow_id", workflow.ID, "name", workflow.Name)
			continue
		}
		registeredWorkflow, ok := registeredWorkflowAny.(WorkflowRegistryEntry)
		if !ok {
			ctx.logger.Error("invalid workflow registry entry type", "workflow_id", workflow.ID, "name", workflow.Name)
			continue
		}

		// Convert workflow parameters to options
		opts := []WorkflowOption{
			WithWorkflowID(workflow.ID),
		}
		// Create a workflow context from the executor context
		handle, err := registeredWorkflow.wrappedFunction(ctx, decodedInput, opts...)
		if err != nil {
			return nil, err
		}
		workflowHandles = append(workflowHandles, handle)
	}

	return workflowHandles, nil
}
