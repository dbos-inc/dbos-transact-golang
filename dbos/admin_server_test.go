package dbos

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAdminServer(t *testing.T) {
	databaseURL := getDatabaseURL()

	t.Run("Admin server is not started by default", func(t *testing.T) {

		ctx, err := NewDBOSContext(Config{
			DatabaseURL: databaseURL,
			AppName:     "test-app",
		})
		require.NoError(t, err)
		err = ctx.Launch()
		require.NoError(t, err)

		// Ensure cleanup
		defer func() {
			if ctx != nil {
				ctx.Cancel()
			}
		}()

		// Give time for any startup processes
		time.Sleep(100 * time.Millisecond)

		// Verify admin server is not running
		client := &http.Client{Timeout: 1 * time.Second}
		_, err = client.Get("http://localhost:3001" + _HEALTHCHECK_PATH)
		require.Error(t, err, "Expected request to fail when admin server is not started")

		// Verify the DBOS executor doesn't have an admin server instance
		require.NotNil(t, ctx, "Expected DBOS instance to be created")

		exec := ctx.(*dbosContext)
		require.Nil(t, exec.adminServer, "Expected admin server to be nil when not configured")
	})

	t.Run("Admin server endpoints", func(t *testing.T) {
		// Launch DBOS with admin server once for all endpoint tests
		ctx, err := NewDBOSContext(Config{
			DatabaseURL: databaseURL,
			AppName:     "test-app",
			AdminServer: true,
		})
		require.NoError(t, err)
		err = ctx.Launch()
		require.NoError(t, err)

		// Ensure cleanup
		defer func() {
			if ctx != nil {
				ctx.Cancel()
			}
		}()

		// Give the server a moment to start
		time.Sleep(100 * time.Millisecond)

		// Verify the DBOS executor has an admin server instance
		require.NotNil(t, ctx, "Expected DBOS instance to be created")

		exec := ctx.(*dbosContext)
		require.NotNil(t, exec.adminServer, "Expected admin server to be created in DBOS instance")

		client := &http.Client{Timeout: 5 * time.Second}

		tests := []struct {
			name           string
			method         string
			endpoint       string
			body           io.Reader
			contentType    string
			expectedStatus int
			validateResp   func(t *testing.T, resp *http.Response)
		}{
			{
				name:           "Health endpoint responds correctly",
				method:         "GET",
				endpoint:       "http://localhost:3001" + _HEALTHCHECK_PATH,
				expectedStatus: http.StatusOK,
			},
			{
				name:           "Recovery endpoint responds correctly with valid JSON",
				method:         "POST",
				endpoint:       "http://localhost:3001" + _WORKFLOW_RECOVERY_PATH,
				body:           bytes.NewBuffer(mustMarshal([]string{"executor1", "executor2"})),
				contentType:    "application/json",
				expectedStatus: http.StatusOK,
				validateResp: func(t *testing.T, resp *http.Response) {
					var workflowIDs []string
					err := json.NewDecoder(resp.Body).Decode(&workflowIDs)
					require.NoError(t, err, "Failed to decode response as JSON array")
					assert.NotNil(t, workflowIDs, "Expected non-nil workflow IDs array")
				},
			},
			{
				name:           "Recovery endpoint rejects invalid methods",
				method:         "GET",
				endpoint:       "http://localhost:3001" + _WORKFLOW_RECOVERY_PATH,
				expectedStatus: http.StatusMethodNotAllowed,
			},
			{
				name:           "Recovery endpoint rejects invalid JSON",
				method:         "POST",
				endpoint:       "http://localhost:3001" + _WORKFLOW_RECOVERY_PATH,
				body:           strings.NewReader(`{"invalid": json}`),
				contentType:    "application/json",
				expectedStatus: http.StatusBadRequest,
			},
			{
				name:           "Queue metadata endpoint responds correctly",
				method:         "GET",
				endpoint:       "http://localhost:3001" + _WORKFLOW_QUEUES_METADATA_PATH,
				expectedStatus: http.StatusOK,
				validateResp: func(t *testing.T, resp *http.Response) {
					var queueMetadata []WorkflowQueue
					err := json.NewDecoder(resp.Body).Decode(&queueMetadata)
					require.NoError(t, err, "Failed to decode response as QueueMetadata array")
					assert.NotNil(t, queueMetadata, "Expected non-nil queue metadata array")
					// Should contain at least the internal queue
					assert.Greater(t, len(queueMetadata), 0, "Expected at least one queue in metadata")
					// Verify internal queue fields
					foundInternalQueue := false
					for _, queue := range queueMetadata {
						if queue.Name == _DBOS_INTERNAL_QUEUE_NAME { // Internal queue name
							foundInternalQueue = true
							assert.Nil(t, queue.GlobalConcurrency, "Expected internal queue to have no concurrency limit")
							assert.Nil(t, queue.WorkerConcurrency, "Expected internal queue to have no worker concurrency limit")
							assert.Nil(t, queue.RateLimit, "Expected internal queue to have no rate limit")
							break
						}
					}
					assert.True(t, foundInternalQueue, "Expected to find internal queue in metadata")
				},
			},
			{
				name:           "Queue metadata endpoint rejects invalid methods",
				method:         "POST",
				endpoint:       "http://localhost:3001" + _WORKFLOW_QUEUES_METADATA_PATH,
				expectedStatus: http.StatusMethodNotAllowed,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				var req *http.Request
				var err error

				if tt.body != nil {
					req, err = http.NewRequest(tt.method, tt.endpoint, tt.body)
				} else {
					req, err = http.NewRequest(tt.method, tt.endpoint, nil)
				}
				require.NoError(t, err, "Failed to create request")

				if tt.contentType != "" {
					req.Header.Set("Content-Type", tt.contentType)
				}

				resp, err := client.Do(req)
				require.NoError(t, err, "Failed to make request")
				defer resp.Body.Close()

				assert.Equal(t, tt.expectedStatus, resp.StatusCode)

				if tt.validateResp != nil {
					tt.validateResp(t, resp)
				}
			})
		}
	})
}

func mustMarshal(v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}
