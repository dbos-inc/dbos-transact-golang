package main

import (
	"context"
	"database/sql"
	"fmt"
	"os/exec"
	"runtime"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/jackc/pgx/v5"
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
	printOnly       bool
)

func init() {
	migrateCmd.Flags().StringVarP(&applicationRole, "app-role", "r", "", "The role with which you will run your DBOS application")
	migrateCmd.Flags().BoolVar(&printOnly, "print-only", false, "Print the migration SQL to stdout without executing anything")
}

func runMigrate(cmd *cobra.Command, args []string) error {
	// Determine the schema to use (from flag or default)
	dbSchema := "dbos"
	if schema != "" {
		dbSchema = schema
	}

	if printOnly {
		return printMigrationSQL(dbSchema)
	}

	// Get database URL
	dbURL, err := getDBURL()
	if err != nil {
		return err
	}

	ctx := context.Background()

	// Create DBOS context which will run migrations automatically for the system DB
	_, err = createContext(ctx, dbURL)
	if err != nil {
		return err
	}

	// Grant permissions to application role if specified
	if applicationRole != "" {
		if err := grantDBOSSchemaPermissions(dbURL, applicationRole, dbSchema); err != nil {
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

// printMigrationSQL writes the full migration SQL to stdout without touching
// the database. Logging goes to stderr.
func printMigrationSQL(schemaName string) error {
	if config != nil && len(config.Database.Migrate) > 0 {
		logger.Warn("Skipping migration commands from 'dbos-config.yaml' in print-only mode", "commands", config.Database.Migrate)
	}

	fmt.Println("-- DBOS system database migration")
	fmt.Printf("-- Schema: %s\n", schemaName)
	for _, stmt := range dbos.MigrationStatements(schemaName) {
		fmt.Println(stmt)
	}

	if applicationRole != "" {
		fmt.Printf("-- Permissions for application role: %s\n", applicationRole)
		for _, query := range grantQueries(applicationRole, schemaName) {
			fmt.Printf("%s;\n", query)
		}
	}
	return nil
}

func grantQueries(roleName, schemaName string) []string {
	schemaSQL := pgx.Identifier{schemaName}.Sanitize()
	roleSQL := pgx.Identifier{roleName}.Sanitize()

	return []string{
		fmt.Sprintf(`GRANT USAGE ON SCHEMA %s TO %s`, schemaSQL, roleSQL),
		fmt.Sprintf(`GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA %s TO %s`, schemaSQL, roleSQL),
		fmt.Sprintf(`GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA %s TO %s`, schemaSQL, roleSQL),
		fmt.Sprintf(`GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA %s TO %s`, schemaSQL, roleSQL),
		fmt.Sprintf(`ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT ALL ON TABLES TO %s`, schemaSQL, roleSQL),
		fmt.Sprintf(`ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT ALL ON SEQUENCES TO %s`, schemaSQL, roleSQL),
		fmt.Sprintf(`ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT EXECUTE ON FUNCTIONS TO %s`, schemaSQL, roleSQL),
	}
}

func grantDBOSSchemaPermissions(databaseURL, roleName, schemaName string) error {
	logger.Info("Granting permissions for schema", "role", roleName, "schema", schemaName)

	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for _, query := range grantQueries(roleName, schemaName) {
		logger.Debug("Executing grant query", "query", query)
		if _, err := db.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("failed to execute grant: %w", err)
		}
	}

	return nil
}
