package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"os"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// maskPassword replaces the password in a database URL with asterisks
func maskPassword(dbURL string) string {
	parsedURL, err := url.Parse(dbURL)
	if err != nil {
		// If we can't parse it, return the original (shouldn't happen with valid URLs)
		logger.Warn("Failed to parse database URL", "error", err)
		return dbURL
	}

	// Check if there is user info with a password
	if parsedURL.User != nil {
		username := parsedURL.User.Username()
		_, hasPassword := parsedURL.User.Password()
		if hasPassword {
			// Manually construct the URL with masked password to avoid encoding
			maskedURL := parsedURL.Scheme + "://" + username + ":********@" + parsedURL.Host + parsedURL.Path
			if parsedURL.RawQuery != "" {
				maskedURL += "?" + parsedURL.RawQuery
			}
			if parsedURL.Fragment != "" {
				maskedURL += "#" + parsedURL.Fragment
			}
			return maskedURL
		}
	}

	return parsedURL.String()
}

// getDBURL resolves the database URL from flag, config, or environment variable
func getDBURL(_ *cobra.Command) (string, error) {
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
	if verbose {
		maskedURL := maskPassword(resolvedURL)
		logger.Debug("Using database URL", "source", source, "url", maskedURL)
	}

	return resolvedURL, nil
}

// createDBOSContext creates a new DBOS context with the provided database URL
func createDBOSContext(dbURL string) (dbos.DBOSContext, error) {
	appName := "dbos-cli"

	ctx, err := dbos.NewDBOSContext(dbos.Config{
		DatabaseURL: dbURL,
		AppName:     appName,
		Logger:      initLogger(slog.LevelError),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create DBOS context: %w", err)
	}
	return ctx, nil
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
