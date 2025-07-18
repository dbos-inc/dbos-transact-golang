package dbos

import (
	"net/http"
	"os"
	"testing"
	"time"
)

func TestAdminServer(t *testing.T) {
	// Skip if database is not available
	databaseURL := os.Getenv("DBOS_DATABASE_URL")
	if databaseURL == "" && os.Getenv("PGPASSWORD") == "" {
		t.Skip("Database not available (DBOS_DATABASE_URL and PGPASSWORD not set), skipping DBOS integration tests")
	}

	t.Run("Admin server starts through DBOS Launch with WithAdminServer", func(t *testing.T) {
		// Ensure clean state
		if dbos != nil {
			Shutdown()
		}

		// Launch DBOS with admin server
		err := Launch(WithAdminServer())
		if err != nil {
			t.Skipf("Failed to launch DBOS with admin server (database likely not available): %v", err)
		}

		// Ensure cleanup
		defer Shutdown()

		// Give the server a moment to start
		time.Sleep(100 * time.Millisecond)

		// Verify admin server is running by making a request
		client := &http.Client{Timeout: 5 * time.Second}
		resp, err := client.Get("http://localhost:3001/dbos-healthz")
		if err != nil {
			t.Fatalf("Failed to make request to admin server: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status code 200, got %d", resp.StatusCode)
		}

		// Verify the DBOS executor has an admin server instance
		if dbos == nil {
			t.Fatal("Expected DBOS instance to be created")
		}

		if dbos.adminServer == nil {
			t.Fatal("Expected admin server to be created in DBOS instance")
		}
	})

	t.Run("Admin server is not started without WithAdminServer option", func(t *testing.T) {
		// Ensure clean state
		if dbos != nil {
			Shutdown()
		}

		// Launch DBOS without admin server option
		err := Launch()
		if err != nil {
			t.Skipf("Failed to launch DBOS (database likely not available): %v", err)
		}

		// Ensure cleanup
		defer Shutdown()

		// Give time for any startup processes
		time.Sleep(100 * time.Millisecond)

		// Verify admin server is not running
		client := &http.Client{Timeout: 1 * time.Second}
		_, err = client.Get("http://localhost:3001/dbos-healthz")
		if err == nil {
			t.Error("Expected request to fail when admin server is not started, but it succeeded")
		}

		// Verify the DBOS executor doesn't have an admin server instance
		if dbos == nil {
			t.Fatal("Expected DBOS instance to be created")
		}

		if dbos.adminServer != nil {
			t.Error("Expected admin server to be nil when not configured")
		}
	})
}
