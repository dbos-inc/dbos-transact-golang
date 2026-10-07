package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
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

	opts := []dbos.MigrateOption{dbos.WithMigrateSchema(dbSchema), dbos.WithMigrateLogger(initLogger(slog.LevelError))}
	if applicationRole != "" {
		opts = append(opts, dbos.WithMigrateApplicationRole(applicationRole))
	}
	if err := dbos.Migrate(context.Background(), dbURL, opts...); err != nil {
		return err
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
		for _, query := range dbos.PermissionStatements(schemaName, applicationRole) {
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
