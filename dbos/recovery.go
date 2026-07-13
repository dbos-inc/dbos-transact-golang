package dbos

import "github.com/dbos-inc/dbos-transact-golang/dbos/internal/sysdb"

func recoverPendingWorkflows(ctx *dbosContext, executorIDs []string) ([]WorkflowHandle[any], error) {
	workflowHandles := make([]WorkflowHandle[any], 0)
	// List pending workflows for the executors
	pendingWorkflows, err := sysdb.RetryWithResult(ctx, func() ([]WorkflowStatus, error) {
		appVersion := []string{}
		if ctx.applicationVersion != "" {
			appVersion = []string{ctx.applicationVersion}
		}
		return ctx.systemDB.ListWorkflows(ctx, sysdb.ListWorkflowsDBInput{
			Status:             []WorkflowStatusType{WorkflowStatusPending},
			ExecutorIDs:        executorIDs,
			ApplicationVersion: appVersion,
			LoadInput:          true,
		})
	}, sysdb.WithRetrierLogger(ctx.logger))
	if err != nil {
		return nil, err
	}

	for _, workflow := range pendingWorkflows {
		if workflow.QueueName != "" {
			cleared, err := sysdb.RetryWithResult(ctx, func() (bool, error) {
				return ctx.systemDB.ClearQueueAssignment(ctx, workflow.ID)
			}, sysdb.WithRetrierLogger(ctx.logger))
			if err != nil {
				ctx.logger.Error("Error clearing queue assignment for workflow", "workflow_id", workflow.ID, "name", workflow.Name, "error", err)
				continue
			}
			if cleared {
				workflowHandles = append(workflowHandles, newWorkflowPollingHandle[any](ctx, workflow.ID))
			}
			continue
		}

		// Configured instance workflows are registered under a name qualified with their config name.
		lookupName := workflow.Name
		if workflow.ConfigName != nil && *workflow.ConfigName != "" {
			lookupName = instanceQualifiedName(workflow.Name, *workflow.ConfigName)
		}
		wfName, ok := ctx.workflowCustomNametoFQN.Load(lookupName)
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

		// Convert workflow parameters to options.
		// Auth identity is re-attached so child workflows spawned during
		// recovery inherit the same identity as the original run.
		opts := []WorkflowOption{
			WithWorkflowID(workflow.ID),
			withIsRecovery(),
			WithAuthenticatedUser(workflow.AuthenticatedUser),
			WithAssumedRole(workflow.AssumedRole),
			WithAuthenticatedRoles(workflow.AuthenticatedRoles),
		}
		// Create a workflow context from the executor context
		// Pass encoded input directly - decoding will happen in workflow wrapper when we know the target type
		handle, err := registeredWorkflow.wrappedFunction(ctx, workflow.Input, workflow.Serialization, opts...)
		if err != nil {
			return nil, err
		}
		workflowHandles = append(workflowHandles, handle)
	}

	return workflowHandles, nil
}
