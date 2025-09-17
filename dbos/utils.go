package dbos

import (
	"log/slog"
	"net/url"
)

var (
	logger *slog.Logger
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
