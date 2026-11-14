package main

import (
	"context"
	"errors"
	"fmt"
	"time"

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

// holdsShardedSchema reports whether the database holds a sharded DBOS system schema.
// A database that cannot be reached, or does not exist yet, holds none: the reset reports that.
func holdsShardedSchema(ctx context.Context, config *pgx.ConnConfig) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		return false, nil
	}
	defer conn.Close(ctx)
	var found bool
	if err := conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM information_schema.columns
		WHERE table_name = 'workflow_status' AND column_name = 'shard_key')`).Scan(&found); err != nil {
		return false, fmt.Errorf("failed to check whether the system database is sharded: %w", err)
	}
	return found, nil
}

func runReset(cmd *cobra.Command, args []string) error {
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

	// Dropping the database a router or a shard points at would take every tenant's rows with it.
	sharded, err := holdsShardedSchema(ctx, config.ConnConfig)
	if err != nil {
		return err
	}
	if sharded {
		return errors.New("resetting a sharded system database is not supported: drop or empty its schema through the router instead")
	}

	// Get confirmation unless skipped
	if !skipConfirmation {
		prompt := "This command resets your DBOS system database, deleting metadata about past workflows and steps. Are you sure you want to proceed?"
		if !confirmAction(prompt) {
			logger.Info("Operation cancelled.")
			return nil
		}
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

	// Drop the system database if it exists
	logger.Info("Resetting system database", "database", dbName)
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
