package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"regexp"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/jackc/pgx/v5"
	"github.com/spf13/viper"
)

// maskPassword replaces the password in a database URL with asterisks
func maskPassword(dbURL string) (string, error) {
	parsedURL, err := url.Parse(dbURL)
	if err == nil && parsedURL.Scheme != "" {

		// Check if there is user info with a password
		if parsedURL.User != nil {
			username := parsedURL.User.Username()
			_, hasPassword := parsedURL.User.Password()
			if hasPassword {
				// Manually construct the URL with masked password to avoid encoding
				maskedURL := parsedURL.Scheme + "://" + username + ":***@" + parsedURL.Host + parsedURL.Path
				if parsedURL.RawQuery != "" {
					maskedURL += "?" + parsedURL.RawQuery
				}
				if parsedURL.Fragment != "" {
					maskedURL += "#" + parsedURL.Fragment
				}
				return maskedURL, nil
			}
		}

		return parsedURL.String(), nil
	}

	// If URL parsing failed or no scheme, try key-value format (libpq connection string)
	return maskPasswordInKeyValueFormat(dbURL), nil
}

// maskPasswordInKeyValueFormat masks password in libpq-style key-value connection strings
// Format: "user=foo password=bar database=db host=localhost"
// Supports all spacing variations: password=value, password =value, password= value, password = value
func maskPasswordInKeyValueFormat(connStr string) string {
	// Match password=value (case insensitive, handles spaces around =)
	// Pattern matches: password (case insensitive), optional spaces, =, optional spaces, then value until next space or end
	re := regexp.MustCompile(`(?i)password\s*=\s*[^\s]+`)
	return re.ReplaceAllString(connStr, "password=***")
}

// getDBURL resolves the database URL from flag, config, or environment variable
func getDBURL() (string, error) {
	var resolvedURL string
	var source string

	// 1. Check flag
	if dbURL != "" {
		resolvedURL = dbURL
		source = "flag"
	} else if viper.IsSet("database_url") {
		// 2. Check config file
		resolvedURL = viper.GetString("database_url")
		source = "DBOS config file"
	} else if envURL := os.Getenv("DBOS_SYSTEM_DATABASE_URL"); envURL != "" {
		// 3. Check environment variable (DBOS_SYSTEM_DATABASE_URL)
		resolvedURL = envURL
		source = "environment variable"
	} else {
		return "", fmt.Errorf("missing database URL: please set it using the --db-url flag, your dbos-config.yaml file, or the DBOS_SYSTEM_DATABASE_URL environment variable")
	}

	// Log the database URL in verbose mode with masked password
	maskedURL, err := maskPassword(resolvedURL)
	if err != nil {
		logger.Debug("Failed to mask database URL", "error", err)
		maskedURL = resolvedURL
	}
	logger.Debug("Using database URL", "source", source, "url", maskedURL)

	return resolvedURL, nil
}

// createDBOSContext creates a new DBOS context with the provided database URL
func createDBOSContext(ctx context.Context, dbURL string) (dbos.DBOSContext, error) {
	appName := "dbos-cli"

	config := dbos.Config{
		DatabaseURL: dbURL,
		AppName:     appName,
		Logger:      initLogger(slog.LevelError),
	}

	// Use the global schema flag if it's set
	if schema != "" {
		config.DatabaseSchema = schema
		logger.Debug("Using database schema", "schema", schema)
	}

	dbosCtx, err := dbos.NewDBOSContext(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create DBOS context: %w", err)
	}
	return dbosCtx, nil
}

// outputJSON outputs data as JSON
func outputJSON(data any) error {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(data)
}

// confirmAction prompts the user for confirmation
func confirmAction(prompt string) bool {
	fmt.Printf("%s (y/N): ", prompt)
	var response string
	fmt.Scanln(&response)
	return response == "y" || response == "Y" || response == "yes" || response == "Yes"
}

func isCockroachDB(ctx context.Context, conn *pgx.Conn) bool {
	var version string
	err := conn.QueryRow(ctx, "SHOW CLUSTER SETTING version").Scan(&version)
	return err == nil
}

// dropDatabaseIfExists drops a database in a way that works with both PostgreSQL and CockroachDB.
// For CockroachDB, it terminates active connections first, then drops the database.
// For PostgreSQL, it uses the WITH (FORCE) syntax.
func dropDatabaseIfExists(ctx context.Context, conn *pgx.Conn, dbName string) error {
	crdb := isCockroachDB(ctx, conn)

	sanitizedDBName := pgx.Identifier{dbName}.Sanitize()

	var err error
	if crdb {
		// In CockroachDB, we can't force drop, so we terminate connections manually
		// Try to terminate connections to the target database
		terminateQuery := `
			SELECT pg_terminate_backend(pid)
			FROM pg_stat_activity
			WHERE datname = $1 AND pid != pg_backend_pid()`
		_, _ = conn.Exec(ctx, terminateQuery, dbName) // Ignore errors, proceed anyway

		dropSQL := fmt.Sprintf("DROP DATABASE IF EXISTS %s", sanitizedDBName)
		_, err = conn.Exec(ctx, dropSQL)
		if err != nil {
			return fmt.Errorf("failed to drop database %s: %w", dbName, err)
		}
	} else {
		// For PostgreSQL, use WITH (FORCE) to drop even with active connections
		dropSQL := fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE)", sanitizedDBName)
		_, err = conn.Exec(ctx, dropSQL)
		if err != nil {
			return fmt.Errorf("failed to drop database %s: %w", dbName, err)
		}
	}

	return nil
}
