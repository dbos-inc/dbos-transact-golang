package main

import (
	"database/sql"
	"fmt"
	"net/url"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/spf13/cobra"
)

var resetCmd = &cobra.Command{
	Use:   "reset",
	Short: "Reset the DBOS system database",
	RunE:  runReset,
}

var (
	skipConfirmation bool
	sysDBName        string
)

func init() {
	resetCmd.Flags().BoolVarP(&skipConfirmation, "yes", "y", false, "Skip confirmation prompt")
	resetCmd.Flags().StringVarP(&sysDBName, "sys-db-name", "s", "", "Specify the name of the system database to reset")
}

func runReset(cmd *cobra.Command, args []string) error {
	// Get confirmation unless skipped
	if !skipConfirmation {
		prompt := "This command resets your DBOS system database, deleting metadata about past workflows and steps. Are you sure you want to proceed?"
		if !confirmAction(prompt) {
			logger.Info("Operation cancelled.")
			return nil
		}
	}

	// Get database URL
	dbURL, err := getDBURL(cmd)
	if err != nil {
		return err
	}

	// Parse the URL to get database name
	parsedURL, err := url.Parse(dbURL)
	if err != nil {
		return fmt.Errorf("invalid database URL: %w", err)
	}

	// Extract database name from path
	dbName := parsedURL.Path
	if len(dbName) > 0 && dbName[0] == '/' {
		dbName = dbName[1:] // Remove leading slash
	}

	if dbName == "" {
		return fmt.Errorf("database name is required in URL")
	}

	// Connect to postgres database to drop and recreate the system database
	parsedURL.Path = "/postgres"
	postgresURL := parsedURL.String()

	db, err := sql.Open("pgx", postgresURL)
	if err != nil {
		return fmt.Errorf("failed to connect to postgres database: %w", err)
	}
	defer db.Close()

	// Drop the system database if it exists
	logger.Info("Resetting system database", "database", dbName)

	// First, terminate all connections to the database
	terminateQuery := fmt.Sprintf(`
		SELECT pg_terminate_backend(pid)
		FROM pg_stat_activity
		WHERE datname = '%s' AND pid <> pg_backend_pid()
	`, dbName)

	_, _ = db.Exec(terminateQuery) // Ignore errors, database might not exist

	// Drop the database if it exists
	dropQuery := fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbName)
	if _, err := db.Exec(dropQuery); err != nil {
		return fmt.Errorf("failed to drop system database: %w", err)
	}

	// Create the database
	createQuery := fmt.Sprintf("CREATE DATABASE %s", dbName)
	if _, err := db.Exec(createQuery); err != nil {
		return fmt.Errorf("failed to create system database: %w", err)
	}

	logger.Info("System database has been reset successfully", "database", dbName)
	return nil
}
