package main

import (
	"fmt"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/spf13/cobra"
)

var workflowCmd = &cobra.Command{
	Use:   "workflow",
	Short: "Manage DBOS workflows",
}

var workflowListCmd = &cobra.Command{
	Use:   "list",
	Short: "List workflows for your application",
	RunE:  runWorkflowList,
}

var workflowGetCmd = &cobra.Command{
	Use:   "get [workflow-id]",
	Short: "Retrieve the status of a workflow",
	Args:  cobra.ExactArgs(1),
	RunE:  runWorkflowGet,
}

// Steps command is currently not implemented as the method is internal
// var workflowStepsCmd = &cobra.Command{
// 	Use:   "steps [workflow-id]",
// 	Short: "List the steps of a workflow",
// 	Args:  cobra.ExactArgs(1),
// 	RunE:  runWorkflowSteps,
// }

var workflowCancelCmd = &cobra.Command{
	Use:   "cancel [workflow-id]",
	Short: "Cancel a workflow so it is no longer automatically retried or restarted",
	Args:  cobra.ExactArgs(1),
	RunE:  runWorkflowCancel,
}

var workflowResumeCmd = &cobra.Command{
	Use:   "resume [workflow-id]",
	Short: "Resume a workflow that has been cancelled",
	Args:  cobra.ExactArgs(1),
	RunE:  runWorkflowResume,
}

var workflowForkCmd = &cobra.Command{
	Use:   "fork [workflow-id]",
	Short: "Fork a workflow from the beginning or from a specific step",
	Args:  cobra.ExactArgs(1),
	RunE:  runWorkflowFork,
}

func init() {
	// Add subcommands to workflow
	workflowCmd.AddCommand(workflowListCmd)
	workflowCmd.AddCommand(workflowGetCmd)
	// workflowCmd.AddCommand(workflowStepsCmd) // Not implemented - internal method
	workflowCmd.AddCommand(workflowCancelCmd)
	workflowCmd.AddCommand(workflowResumeCmd)
	workflowCmd.AddCommand(workflowForkCmd)

	// List command flags
	workflowListCmd.Flags().IntP("limit", "l", 10, "Limit the results returned")
	workflowListCmd.Flags().StringP("user", "u", "", "Retrieve workflows run by this user")
	workflowListCmd.Flags().StringP("start-time", "s", "", "Retrieve workflows starting after this timestamp (ISO 8601 format)")
	workflowListCmd.Flags().StringP("end-time", "e", "", "Retrieve workflows starting before this timestamp (ISO 8601 format)")
	workflowListCmd.Flags().StringP("status", "S", "", "Retrieve workflows with this status (PENDING, SUCCESS, ERROR, ENQUEUED, CANCELLED, or MAX_RECOVERY_ATTEMPTS_EXCEEDED)")
	workflowListCmd.Flags().StringP("application-version", "v", "", "Retrieve workflows with this application version")
	workflowListCmd.Flags().StringP("name", "n", "", "Retrieve workflows with this name")
	workflowListCmd.Flags().BoolP("sort-desc", "d", false, "Sort the results in descending order (older first)")
	workflowListCmd.Flags().IntP("offset", "o", 0, "Offset for pagination")
	workflowListCmd.Flags().StringP("queue", "q", "", "Retrieve workflows on this queue")
	workflowListCmd.Flags().BoolP("queues-only", "Q", false, "Retrieve only queued workflows")

	// Fork command flags
	workflowForkCmd.Flags().IntP("step", "s", 1, "Restart from this step")
}

func runWorkflowList(cmd *cobra.Command, args []string) error {
	// Get database URL
	dbURL, err := getDBURL(cmd)
	if err != nil {
		return err
	}

	// Create DBOS context
	ctx, err := createDBOSContext(dbURL)
	if err != nil {
		return err
	}

	// Build options from flags
	var opts []dbos.ListWorkflowsOption

	if limit, _ := cmd.Flags().GetInt("limit"); limit > 0 {
		opts = append(opts, dbos.WithLimit(limit))
	}

	if offset, _ := cmd.Flags().GetInt("offset"); offset > 0 {
		opts = append(opts, dbos.WithOffset(offset))
	}

	if user, _ := cmd.Flags().GetString("user"); user != "" {
		opts = append(opts, dbos.WithUser(user))
	}

	if name, _ := cmd.Flags().GetString("name"); name != "" {
		opts = append(opts, dbos.WithName(name))
	}

	if status, _ := cmd.Flags().GetString("status"); status != "" {
		var statusType dbos.WorkflowStatusType
		switch status {
		case "PENDING":
			statusType = dbos.WorkflowStatusPending
		case "SUCCESS":
			statusType = dbos.WorkflowStatusSuccess
		case "ERROR":
			statusType = dbos.WorkflowStatusError
		case "ENQUEUED":
			statusType = dbos.WorkflowStatusEnqueued
		case "CANCELLED":
			statusType = dbos.WorkflowStatusCancelled
		case "MAX_RECOVERY_ATTEMPTS_EXCEEDED":
			statusType = dbos.WorkflowStatusMaxRecoveryAttemptsExceeded
		default:
			return fmt.Errorf("invalid status: %s", status)
		}
		opts = append(opts, dbos.WithStatus([]dbos.WorkflowStatusType{statusType}))
	}

	if appVersion, _ := cmd.Flags().GetString("application-version"); appVersion != "" {
		opts = append(opts, dbos.WithAppVersion(appVersion))
	}

	if queue, _ := cmd.Flags().GetString("queue"); queue != "" {
		opts = append(opts, dbos.WithQueueName(queue))
	}

	if queuesOnly, _ := cmd.Flags().GetBool("queues-only"); queuesOnly {
		opts = append(opts, dbos.WithQueuesOnly())
	}

	if sortDesc, _ := cmd.Flags().GetBool("sort-desc"); sortDesc {
		opts = append(opts, dbos.WithSortDesc(true))
	}

	if startTime, _ := cmd.Flags().GetString("start-time"); startTime != "" {
		t, err := time.Parse(time.RFC3339, startTime)
		if err != nil {
			return fmt.Errorf("invalid start-time format: %w", err)
		}
		opts = append(opts, dbos.WithStartTime(t))
	}

	if endTime, _ := cmd.Flags().GetString("end-time"); endTime != "" {
		t, err := time.Parse(time.RFC3339, endTime)
		if err != nil {
			return fmt.Errorf("invalid end-time format: %w", err)
		}
		opts = append(opts, dbos.WithEndTime(t))
	}

	// List workflows
	workflows, err := ctx.ListWorkflows(ctx, opts...)
	if err != nil {
		return fmt.Errorf("failed to list workflows: %w", err)
	}

	// Ensure we have a non-nil slice for JSON output
	if workflows == nil {
		workflows = []dbos.WorkflowStatus{}
	}

	// Output results
	if jsonOutput {
		return outputJSON(workflows)
	}

	// Pretty print for non-JSON output
	if len(workflows) == 0 {
		fmt.Println("No workflows found")
		return nil
	}

	for _, wf := range workflows {
		fmt.Printf("ID: %s\n", wf.ID)
		fmt.Printf("  Name: %s\n", wf.Name)
		fmt.Printf("  Status: %s\n", wf.Status)
		fmt.Printf("  Created: %s\n", wf.CreatedAt.Format(time.RFC3339))
		if wf.QueueName != "" {
			fmt.Printf("  Queue: %s\n", wf.QueueName)
		}
		fmt.Println()
	}

	return nil
}

func runWorkflowGet(cmd *cobra.Command, args []string) error {
	workflowID := args[0]

	// Get database URL
	dbURL, err := getDBURL(cmd)
	if err != nil {
		return err
	}

	// Create DBOS context
	ctx, err := createDBOSContext(dbURL)
	if err != nil {
		return err
	}

	// Retrieve workflow
	handle, err := ctx.RetrieveWorkflow(ctx, workflowID)
	if err != nil {
		return fmt.Errorf("failed to retrieve workflow: %w", err)
	}

	// Get status
	status, err := handle.GetStatus()
	if err != nil {
		return fmt.Errorf("failed to get workflow status: %w", err)
	}

	// Output results
	if jsonOutput {
		return outputJSON(status)
	}

	// Pretty print
	fmt.Printf("Workflow ID: %s\n", status.ID)
	fmt.Printf("Name: %s\n", status.Name)
	fmt.Printf("Status: %s\n", status.Status)
	fmt.Printf("Created: %s\n", status.CreatedAt.Format(time.RFC3339))
	fmt.Printf("Updated: %s\n", status.UpdatedAt.Format(time.RFC3339))
	if status.QueueName != "" {
		fmt.Printf("Queue: %s\n", status.QueueName)
	}
	if status.Error != nil {
		fmt.Printf("Error: %v\n", status.Error)
	}

	return nil
}

// Steps function is not implemented as the method is internal
// func runWorkflowSteps(cmd *cobra.Command, args []string) error {
// 	// Implementation would go here
// 	return nil
// }

func runWorkflowCancel(cmd *cobra.Command, args []string) error {
	workflowID := args[0]

	// Get database URL
	dbURL, err := getDBURL(cmd)
	if err != nil {
		return err
	}

	// Create DBOS context
	ctx, err := createDBOSContext(dbURL)
	if err != nil {
		return err
	}

	// Cancel workflow
	err = ctx.CancelWorkflow(ctx, workflowID)
	if err != nil {
		return fmt.Errorf("failed to cancel workflow: %w", err)
	}
	fmt.Printf("Successfully cancelled workflow %s\n", workflowID)
	return nil
}

func runWorkflowResume(cmd *cobra.Command, args []string) error {
	workflowID := args[0]

	// Get database URL
	dbURL, err := getDBURL(cmd)
	if err != nil {
		return err
	}

	// Create DBOS context
	ctx, err := createDBOSContext(dbURL)
	if err != nil {
		return err
	}

	// Resume workflow
	handle, err := ctx.ResumeWorkflow(ctx, workflowID)
	if err != nil {
		return fmt.Errorf("failed to resume workflow: %w", err)
	}

	// Get status
	status, err := handle.GetStatus()
	if err != nil {
		return fmt.Errorf("failed to get workflow status: %w", err)
	}

	if jsonOutput {
		return outputJSON(status)
	}

	fmt.Printf("Successfully resumed workflow %s\n", workflowID)
	fmt.Printf("Current status: %s\n", status.Status)
	return nil
}

func runWorkflowFork(cmd *cobra.Command, args []string) error {
	workflowID := args[0]

	// Get database URL
	dbURL, err := getDBURL(cmd)
	if err != nil {
		return err
	}

	// Create DBOS context
	ctx, err := createDBOSContext(dbURL)
	if err != nil {
		return err
	}

	// Get step flag
	step, _ := cmd.Flags().GetInt("step")
	if step < 1 {
		step = 1
	}

	// Fork workflow
	handle, err := ctx.ForkWorkflow(ctx, dbos.ForkWorkflowInput{
		OriginalWorkflowID: workflowID,
		StartStep:          uint(step),
	})
	if err != nil {
		return fmt.Errorf("failed to fork workflow: %w", err)
	}

	// Get status of forked workflow
	status, err := handle.GetStatus()
	if err != nil {
		return fmt.Errorf("failed to get forked workflow status: %w", err)
	}

	if jsonOutput {
		return outputJSON(status)
	}

	fmt.Printf("Successfully forked workflow %s\n", workflowID)
	fmt.Printf("New workflow ID: %s\n", status.ID)
	fmt.Printf("Starting from step: %d\n", step)
	fmt.Printf("Status: %s\n", status.Status)
	return nil
}
