package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"
)

var resetCmd = &cobra.Command{
	Use:   "reset",
	Short: "Reset the DBOS system database",
	RunE:  runReset,
}

var (
	skipConfirmation bool
)

func init() {
	resetCmd.Flags().BoolVarP(&skipConfirmation, "yes", "y", false, "Skip confirmation prompt")
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
	dbURL, err := getDBURL()
	if err != nil {
		return err
	}

	ctx := context.Background()

	// Parse the connection string using pgxpool.ParseConfig which handles both URL and key-value formats
	config, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		return fmt.Errorf("failed to parse database URL: %w", err)
	}

	// Get the database name from the config
	dbName := config.ConnConfig.Database
	if dbName == "" {
		return fmt.Errorf("database name not found in connection string")
	}

	// Create a connection configuration pointing to the postgres database
	postgresConfig := config.ConnConfig.Copy()
	postgresConfig.Database = "postgres"

	// Connect to the postgres database
	conn, err := pgx.ConnectConfig(ctx, postgresConfig)
	if err != nil {
		return fmt.Errorf("failed to connect to PostgreSQL server: %w", err)
	}
	defer conn.Close(ctx)

	// Test the connection
	err = conn.Ping(ctx)
	if err != nil {
		return fmt.Errorf("failed to ping PostgreSQL server: %w", err)
	}

	// Drop the system database if it exists
	maskedPostgresConfig := maskPasswordInKeyValueFormat(postgresConfig.ConnString())
	logger.Info("Resetting system database", "database", dbName, "connection string", maskedPostgresConfig)
	err = dropDatabaseIfExists(ctx, conn, dbName)
	if err != nil {
		return fmt.Errorf("failed to drop system database: %w", err)
	}

	// Create the database
	createSQL := fmt.Sprintf("CREATE DATABASE %s", pgx.Identifier{dbName}.Sanitize())
	_, err = conn.Exec(ctx, createSQL)
	if err != nil {
		return fmt.Errorf("failed to create system database: %w", err)
	}

	logger.Info("System database has been reset successfully", "database", dbName)
	return nil
}
