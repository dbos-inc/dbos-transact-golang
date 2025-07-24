package dbos

import (
	"context"
	"encoding/hex"
	"maps"
	"testing"
)

func TestConfigValidationErrorTypes(t *testing.T) {
	databaseURL := getDatabaseURL(t)

	t.Run("FailsWithoutAppName", func(t *testing.T) {
		config := Config{
			DatabaseURL: databaseURL,
		}

		err := Initialize(config)
		if err == nil {
			t.Fatal("expected error when app name is missing, but got none")
		}

		dbosErr, ok := err.(*DBOSError)
		if !ok {
			t.Fatalf("expected DBOSError, got %T", err)
		}

		if dbosErr.Code != InitializationError {
			t.Fatalf("expected InitializationError code, got %v", dbosErr.Code)
		}

		expectedMsg := "Error initializing DBOS Transact: missing required config field: appName"
		if dbosErr.Message != expectedMsg {
			t.Fatalf("expected error message '%s', got '%s'", expectedMsg, dbosErr.Message)
		}
	})

	t.Run("FailsWithoutDatabaseURL", func(t *testing.T) {
		config := Config{
			AppName: "test-app",
		}

		err := Initialize(config)
		if err == nil {
			t.Fatal("expected error when database URL is missing, but got none")
		}

		dbosErr, ok := err.(*DBOSError)
		if !ok {
			t.Fatalf("expected DBOSError, got %T", err)
		}

		if dbosErr.Code != InitializationError {
			t.Fatalf("expected InitializationError code, got %v", dbosErr.Code)
		}

		expectedMsg := "Error initializing DBOS Transact: missing required config field: databaseURL"
		if dbosErr.Message != expectedMsg {
			t.Fatalf("expected error message '%s', got '%s'", expectedMsg, dbosErr.Message)
		}
	})
}
func TestAppVersion(t *testing.T) {
	if _, err := hex.DecodeString(_APP_VERSION); err != nil {
		t.Fatalf("APP_VERSION is not a valid hex string: %v", err)
	}

	// Save the original registry content
	originalRegistry := make(map[string]workflowRegistryEntry)
	maps.Copy(originalRegistry, registry)

	// Restore the registry after the test
	defer func() {
		registry = originalRegistry
	}()

	// Replace the registry and verify the hash is different
	registry = make(map[string]workflowRegistryEntry)

	WithWorkflow(func(ctx context.Context, input string) (string, error) {
		return "new-registry-workflow-" + input, nil
	})
	hash2 := computeApplicationVersion()
	if _APP_VERSION == hash2 {
		t.Fatalf("APP_VERSION hash did not change after replacing registry")
	}
}
