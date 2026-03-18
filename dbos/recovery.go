package dbos

func recoverPendingWorkflows(ctx *dbosContext, executorIDs []string) ([]WorkflowHandle[any], error) {
	workflowHandles := make([]WorkflowHandle[any], 0)
	// List pending workflows for the executors
	pendingWorkflows, err := retryWithResult(ctx, func() ([]WorkflowStatus, error) {
		appVersion := []string{}
		if ctx.applicationVersion != "" {
			appVersion = []string{ctx.applicationVersion}
		}
		return ctx.systemDB.listWorkflows(ctx, listWorkflowsDBInput{
			status:              []WorkflowStatusType{WorkflowStatusPending},
			executorIDs:         executorIDs,
			applicationVersion: appVersion,
			loadInput:           true,
		})
	}, withRetrierLogger(ctx.logger))
	if err != nil {
		return nil, err
	}

	for _, workflow := range pendingWorkflows {
		if workflow.QueueName != "" {
			cleared, err := retryWithResult(ctx, func() (bool, error) {
				return ctx.systemDB.clearQueueAssignment(ctx, workflow.ID)
			}, withRetrierLogger(ctx.logger))
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
			withIsRecovery(),
		}
		// Create a workflow context from the executor context
		// Pass encoded input directly - decoding will happen in workflow wrapper when we know the target type
		handle, err := registeredWorkflow.wrappedFunction(ctx, workflow.Input, opts...)
		if err != nil {
			return nil, err
		}
		workflowHandles = append(workflowHandles, handle)
	}

	return workflowHandles, nil
}
