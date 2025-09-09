package main

import (
	"context"
	"database/sql"
	"fmt"
	"os/exec"
	"runtime"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/spf13/cobra"
)

var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Create DBOS system tables",
	RunE:  runMigrate,
}

var (
	applicationRole string
)

func init() {
	migrateCmd.Flags().StringVarP(&applicationRole, "app-role", "r", "", "The role with which you will run your DBOS application")
}

func runMigrate(cmd *cobra.Command, args []string) error {
	// Get database URL
	dbURL, err := getDBURL(cmd)
	if err != nil {
		return err
	}

	ctx := context.Background()

	// Create DBOS context which will run migrations automatically for the system DB
	_, err = createDBOSContext(ctx, dbURL)
	if err != nil {
		return fmt.Errorf("failed to create DBOS context: %w", err)
	}

	// Grant permissions to application role if specified
	if applicationRole != "" {
		if err := grantDBOSSchemaPermissions(dbURL, applicationRole); err != nil {
			return err
		}
	}

	// Run custom migration commands from config if present
	if config != nil && len(config.Database.Migrate) > 0 {
		logger.Info("Executing migration commands from 'dbos-config.yaml'")
		for _, command := range config.Database.Migrate {
			logger.Info("Executing migration command", "command", command)

			var process *exec.Cmd
			if runtime.GOOS == "windows" {
				process = exec.Command("cmd", "/C", command)
			} else {
				process = exec.Command("sh", "-c", command)
			}
			output, err := process.CombinedOutput()
			if err != nil {
				return fmt.Errorf("migration command failed: %s\nOutput: %s", err, output)
			}
			if len(output) > 0 {
				logger.Info("Migration output", "output", string(output))
			}
		}
	}

	logger.Info("DBOS migrations completed successfully")
	return nil
}

func grantDBOSSchemaPermissions(databaseURL, roleName string) error {
	logger.Info("Granting permissions for DBOS schema", "role", roleName)

	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Grant usage on the dbos schema
	queries := []string{
		fmt.Sprintf(`GRANT USAGE ON SCHEMA dbos TO "%s"`, roleName),
		fmt.Sprintf(`GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA dbos TO "%s"`, roleName),
		fmt.Sprintf(`GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA dbos TO "%s"`, roleName),
		fmt.Sprintf(`GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA dbos TO "%s"`, roleName),
		fmt.Sprintf(`ALTER DEFAULT PRIVILEGES IN SCHEMA dbos GRANT ALL ON TABLES TO "%s"`, roleName),
		fmt.Sprintf(`ALTER DEFAULT PRIVILEGES IN SCHEMA dbos GRANT ALL ON SEQUENCES TO "%s"`, roleName),
		fmt.Sprintf(`ALTER DEFAULT PRIVILEGES IN SCHEMA dbos GRANT EXECUTE ON FUNCTIONS TO "%s"`, roleName),
	}

	for _, query := range queries {
		logger.Debug("Executing grant query", "query", query)
		if _, err := db.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("failed to execute grant: %w", err)
		}
	}

	return nil
}
