package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/spf13/cobra"
)

var renameApplicationCmd = &cobra.Command{
	Use:   "rename-application",
	Short: "Re-own a system database's rows after an application is renamed",
	RunE:  runRenameApplication,
}

var (
	renameOldName            string
	renameNewName            string
	renameAdoptUnclaimedRows bool
	renameBatchSize          int
	renameSkipConfirmation   bool
)

func init() {
	renameApplicationCmd.Flags().BoolVarP(&renameSkipConfirmation, "yes", "y", false, "Skip confirmation prompt")
	renameApplicationCmd.Flags().StringVarP(&renameOldName, "from", "f", "", "The application's previous name. Omit to only adopt unclaimed rows.")
	renameApplicationCmd.Flags().StringVarP(&renameNewName, "to", "t", "", "The application that ends up owning the rows")
	renameApplicationCmd.Flags().BoolVar(&renameAdoptUnclaimedRows, "adopt-unclaimed-rows", false, "Also take rows no application owns (application_name=NULL)")
	renameApplicationCmd.Flags().IntVar(&renameBatchSize, "batch-size", dbos.DefaultRenameBatchSize, "Workflows and steps re-owned per transaction")
	_ = renameApplicationCmd.MarkFlagRequired("to")
}

func runRenameApplication(cmd *cobra.Command, args []string) error {
	var sources []string
	if renameOldName != "" {
		sources = append(sources, fmt.Sprintf("'%s's rows", renameOldName))
	}
	if renameAdoptUnclaimedRows {
		sources = append(sources, "rows no application owns")
	}
	if len(sources) == 0 {
		return fmt.Errorf("nothing to re-own: pass --from, --adopt-unclaimed-rows, or both")
	}

	if !renameSkipConfirmation {
		prompt := fmt.Sprintf("This command re-owns %s in your DBOS system database as '%s'. "+
			"Stop the application being renamed before running this. Are you sure you want to proceed?",
			strings.Join(sources, " and "), renameNewName)
		if !confirmAction(prompt) {
			logger.Info("Operation cancelled.")
			return nil
		}
	}

	dbURL, err := getDBURL()
	if err != nil {
		return err
	}

	client, err := createContext(context.Background(), dbURL)
	if err != nil {
		return err
	}

	moved, err := dbos.RenameApplication(client, dbos.RenameApplicationInput{
		OldName:            renameOldName,
		NewName:            renameNewName,
		BatchSize:          renameBatchSize,
		AdoptUnclaimedRows: renameAdoptUnclaimedRows,
	})
	if err != nil {
		return fmt.Errorf("failed to rename application: %w", err)
	}
	return outputJSON(moved)
}
