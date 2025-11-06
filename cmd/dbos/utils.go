package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strings"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
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
	// Find "password" key (case insensitive)
	lowerStr := strings.ToLower(connStr)
	passwordKey := "password"
	passwordIdx := strings.Index(lowerStr, passwordKey)
	if passwordIdx == -1 {
		return connStr // No password found
	}

	// Find the = sign after "password" (skip optional spaces before =)
	afterKey := passwordIdx + len(passwordKey)
	for afterKey < len(connStr) && connStr[afterKey] == ' ' {
		afterKey++
	}
	if afterKey >= len(connStr) || connStr[afterKey] != '=' {
		return connStr // No = sign found
	}

	// Find the start of the password value (skip = and optional spaces after =)
	valueStart := afterKey + 1
	for valueStart < len(connStr) && connStr[valueStart] == ' ' {
		valueStart++
	}

	// Find the end of the password value (next space or end of string)
	valueEnd := valueStart
	for valueEnd < len(connStr) && connStr[valueEnd] != ' ' {
		valueEnd++
	}

	// Replace password value with ***
	return connStr[:valueStart] + "***" + connStr[valueEnd:]
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
