package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
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
	printMigrations string
	printUserRole   bool
)

func init() {
	migrateCmd.Flags().StringVarP(&applicationRole, "app-role", "r", "", "The role with which you will run your DBOS application")
	migrateCmd.Flags().StringVar(&printMigrations, "print-migrations", "", "Print the SQL of all migrations ('--print-migrations all') or of migrations from a number onward ('--print-migrations 3') instead of running them")
	migrateCmd.Flags().BoolVar(&printUserRole, "print-user-role", false, "Print the SQL granting the application role (--app-role) access to DBOS system tables instead of executing it")
}

func runMigrate(cmd *cobra.Command, args []string) error {
	// Determine the schema to use (from flag or default)
	dbSchema := "dbos"
	if schema != "" {
		dbSchema = schema
	}

	printMigrationsSet := cmd.Flags().Changed("print-migrations")
	if printMigrationsSet || printUserRole {
		// Print modes never touch a database; stdout is pure SQL and comments.
		if err := runMigratePrint(dbSchema, printMigrationsSet); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return nil
	}

	// Get database URL
	dbURL, err := getDBURL()
	if err != nil {
		return err
	}

	ctx := context.Background()

	migrateCtx, err := dbos.NewContext(ctx, dbos.Config{
		DatabaseURL:    dbURL,
		DatabaseSchema: schema,
		AppName:        "dbos-cli",
		Logger:         initLogger(slog.LevelError),
	})
	if err != nil {
		return err
	}
	defer func() {
		if err := dbos.Shutdown(migrateCtx, 30*time.Second); err != nil {
			logger.Debug("Failed to shut down migration context", "error", err)
		}
	}()

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

func runMigratePrint(schemaName string, printMigrationsSet bool) error {
	if printMigrationsSet && printUserRole {
		return errors.New("--print-user-role cannot be combined with --print-migrations")
	}
	if printUserRole {
		if applicationRole == "" {
			return errors.New("--print-user-role requires --app-role")
		}
		if strings.ContainsAny(schemaName, `"'`) {
			return errors.New("Schema names containing quotes are not supported")
		}
		if strings.ContainsAny(applicationRole, `"'`) {
			return errors.New("Role names containing quotes are not supported")
		}
		fmt.Printf("-- Permissions on DBOS schema %s for role %s\n", schemaName, applicationRole)
		for _, query := range grantQueries(applicationRole, schemaName) {
			fmt.Printf("%s;\n", query)
		}
		return nil
	}
	return printDBOSMigrations(schemaName, printMigrations)
}

func printDBOSMigrations(schemaName, value string) error {
	// This mode never connects, so a missing database URL is not an error: it
	// only leaves the URL out of the header comment.
	dbURL, _ := getDBURL()
	if strings.HasPrefix(dbURL, "sqlite") {
		return errors.New("--print-migrations is only supported for Postgres databases")
	}
	if strings.ContainsAny(schemaName, `"'`) {
		return errors.New("Schema names containing quotes are not supported")
	}

	from := 1
	if value != "all" {
		n, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("Invalid --print-migrations value '%s': expected 'all' or a migration number", value)
		}
		from = n
	}

	statements, err := dbos.MigrationStatements(schemaName, from)
	if err != nil {
		return err
	}

	header := "-- DBOS system database migrations"
	if dbURL != "" {
		maskedURL, err := maskPassword(dbURL)
		if err != nil {
			maskedURL = dbURL
		}
		header += " for " + maskedURL
	}
	fmt.Println(header)
	fmt.Println("-- Contains CREATE/DROP INDEX CONCURRENTLY: run outside a transaction block (e.g. plain psql, not psql --single-transaction).")
	if from == 1 {
		fmt.Println("-- This script is for FRESH databases only.")
	}
	for _, stmt := range statements {
		fmt.Println(stmt)
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
