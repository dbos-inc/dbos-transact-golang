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
			{
				name:           "Deactivate endpoint responds correctly",
				method:         "GET",
				endpoint:       "http://localhost:3001" + _DEACTIVATE_PATH,
				expectedStatus: http.StatusOK,
				validateResp: func(t *testing.T, resp *http.Response) {
					body, err := io.ReadAll(resp.Body)
					require.NoError(t, err)
					assert.Equal(t, "deactivated", string(body))
				},
			},
			{
				name:           "Deactivate endpoint rejects invalid methods",
				method:         "POST",
				endpoint:       "http://localhost:3001" + _DEACTIVATE_PATH,
				expectedStatus: http.StatusMethodNotAllowed,
			},
			{
				name:           "Garbage collect endpoint accepts valid request",
				method:         "POST",
				endpoint:       "http://localhost:3001" + _GARBAGE_COLLECT_PATH,
				body:           bytes.NewBuffer(mustMarshal(map[string]interface{}{"cutoff_epoch_timestamp_ms": 1234567890, "rows_threshold": 100})),
				contentType:    "application/json",
				expectedStatus: http.StatusNoContent,
			},
			{
				name:           "Garbage collect endpoint rejects invalid methods",
				method:         "GET",
				endpoint:       "http://localhost:3001" + _GARBAGE_COLLECT_PATH,
				expectedStatus: http.StatusMethodNotAllowed,
			},
			{
				name:           "Global timeout endpoint accepts valid request",
				method:         "POST",
				endpoint:       "http://localhost:3001" + _GLOBAL_TIMEOUT_PATH,
				body:           bytes.NewBuffer(mustMarshal(map[string]interface{}{"cutoff_epoch_timestamp_ms": 1234567890})),
				contentType:    "application/json",
				expectedStatus: http.StatusNoContent,
			},
			{
				name:           "Global timeout endpoint rejects invalid methods",
				method:         "GET",
				endpoint:       "http://localhost:3001" + _GLOBAL_TIMEOUT_PATH,
				expectedStatus: http.StatusMethodNotAllowed,
			},
			{
				name:           "Workflows endpoint accepts valid request",
				method:         "POST",
				endpoint:       "http://localhost:3001" + _WORKFLOWS_PATH,
				body:           bytes.NewBuffer(mustMarshal(map[string]interface{}{"workflow_name": "test", "limit": 10})),
				contentType:    "application/json",
				expectedStatus: http.StatusOK,
				validateResp: func(t *testing.T, resp *http.Response) {
					var workflows []interface{}
					err := json.NewDecoder(resp.Body).Decode(&workflows)
					require.NoError(t, err, "Failed to decode workflows response")
					assert.NotNil(t, workflows, "Expected non-nil workflows array")
				},
			},
			{
				name:           "Workflows endpoint rejects invalid methods",
				method:         "GET",
				endpoint:       "http://localhost:3001" + _WORKFLOWS_PATH,
				expectedStatus: http.StatusMethodNotAllowed,
			},
			{
				name:           "Queued workflows endpoint accepts valid request",
				method:         "POST",
				endpoint:       "http://localhost:3001" + _QUEUED_WORKFLOWS_PATH,
				body:           bytes.NewBuffer(mustMarshal(map[string]interface{}{"queue_name": "test", "limit": 10})),
				contentType:    "application/json",
				expectedStatus: http.StatusOK,
				validateResp: func(t *testing.T, resp *http.Response) {
					var workflows []interface{}
					err := json.NewDecoder(resp.Body).Decode(&workflows)
					require.NoError(t, err, "Failed to decode queued workflows response")
					assert.NotNil(t, workflows, "Expected non-nil queued workflows array")
				},
			},
			{
				name:           "Queued workflows endpoint rejects invalid methods",
				method:         "GET",
				endpoint:       "http://localhost:3001" + _QUEUED_WORKFLOWS_PATH,
				expectedStatus: http.StatusMethodNotAllowed,
			},
			{
				name:           "Get workflow by ID endpoint",
				method:         "GET",
				endpoint:       "http://localhost:3001/workflows/test-workflow-123",
				expectedStatus: http.StatusOK,
				validateResp: func(t *testing.T, resp *http.Response) {
					var workflow map[string]interface{}
					err := json.NewDecoder(resp.Body).Decode(&workflow)
					require.NoError(t, err, "Failed to decode workflow response")
					assert.Equal(t, "test-workflow-123", workflow["workflow_id"])
					assert.NotNil(t, workflow["status"])
				},
			},
			{
				name:           "Get workflow by ID rejects invalid methods",
				method:         "POST",
				endpoint:       "http://localhost:3001/workflows/test-workflow-123",
				expectedStatus: http.StatusMethodNotAllowed,
			},
			{
				name:           "Get workflow steps endpoint",
				method:         "GET",
				endpoint:       "http://localhost:3001/workflows/test-workflow-123/steps",
				expectedStatus: http.StatusOK,
				validateResp: func(t *testing.T, resp *http.Response) {
					var steps []interface{}
					err := json.NewDecoder(resp.Body).Decode(&steps)
					require.NoError(t, err, "Failed to decode steps response")
					assert.NotNil(t, steps, "Expected non-nil steps array")
				},
			},
			{
				name:           "Get workflow steps rejects invalid methods",
				method:         "POST",
				endpoint:       "http://localhost:3001/workflows/test-workflow-123/steps",
				expectedStatus: http.StatusMethodNotAllowed,
			},
			{
				name:           "Cancel workflow endpoint",
				method:         "POST",
				endpoint:       "http://localhost:3001/workflows/test-workflow-123/cancel",
				expectedStatus: http.StatusNoContent,
			},
			{
				name:           "Cancel workflow rejects invalid methods",
				method:         "GET",
				endpoint:       "http://localhost:3001/workflows/test-workflow-123/cancel",
				expectedStatus: http.StatusMethodNotAllowed,
			},
			{
				name:           "Resume workflow endpoint",
				method:         "POST",
				endpoint:       "http://localhost:3001/workflows/test-workflow-123/resume",
				expectedStatus: http.StatusNoContent,
			},
			{
				name:           "Resume workflow rejects invalid methods",
				method:         "GET",
				endpoint:       "http://localhost:3001/workflows/test-workflow-123/resume",
				expectedStatus: http.StatusMethodNotAllowed,
			},
			{
				name:           "Restart workflow endpoint",
				method:         "POST",
				endpoint:       "http://localhost:3001/workflows/test-workflow-123/restart",
				expectedStatus: http.StatusOK,
				validateResp: func(t *testing.T, resp *http.Response) {
					var response map[string]string
					err := json.NewDecoder(resp.Body).Decode(&response)
					require.NoError(t, err, "Failed to decode restart response")
					assert.NotEmpty(t, response["workflow_id"])
				},
			},
			{
				name:           "Restart workflow rejects invalid methods",
				method:         "GET",
				endpoint:       "http://localhost:3001/workflows/test-workflow-123/restart",
				expectedStatus: http.StatusMethodNotAllowed,
			},
			{
				name:           "Fork workflow endpoint with all parameters",
				method:         "POST",
				endpoint:       "http://localhost:3001/workflows/test-workflow-123/fork",
				body: bytes.NewBuffer(mustMarshal(map[string]interface{}{
					"start_step":          2,
					"new_workflow_id":     "forked-workflow-456",
					"application_version": "v2.0",
				})),
				contentType:    "application/json",
				expectedStatus: http.StatusOK,
				validateResp: func(t *testing.T, resp *http.Response) {
					var response map[string]string
					err := json.NewDecoder(resp.Body).Decode(&response)
					require.NoError(t, err, "Failed to decode fork response")
					assert.NotEmpty(t, response["workflow_id"])
				},
			},
			{
				name:           "Fork workflow endpoint with minimal parameters",
				method:         "POST",
				endpoint:       "http://localhost:3001/workflows/test-workflow-123/fork",
				body:           bytes.NewBuffer(mustMarshal(map[string]interface{}{})),
				contentType:    "application/json",
				expectedStatus: http.StatusOK,
				validateResp: func(t *testing.T, resp *http.Response) {
					var response map[string]string
					err := json.NewDecoder(resp.Body).Decode(&response)
					require.NoError(t, err, "Failed to decode fork response")
					assert.NotEmpty(t, response["workflow_id"])
				},
			},
			{
				name:           "Fork workflow rejects invalid methods",
				method:         "GET",
				endpoint:       "http://localhost:3001/workflows/test-workflow-123/fork",
				expectedStatus: http.StatusMethodNotAllowed,
			},
			{
				name:           "Unknown workflow endpoint returns 404",
				method:         "GET",
				endpoint:       "http://localhost:3001/workflows/test-workflow-123/unknown",
				expectedStatus: http.StatusNotFound,
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

	t.Run("Deactivation state persists", func(t *testing.T) {
		// Launch DBOS with admin server
		ctx, err := NewDBOSContext(Config{
			DatabaseURL: databaseURL,
			AppName:     "test-app",
			AdminServer: true,
			AdminPort:   3002, // Use different port to avoid conflicts
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

		client := &http.Client{Timeout: 5 * time.Second}

		// First deactivation request
		resp, err := client.Get("http://localhost:3002" + _DEACTIVATE_PATH)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.Equal(t, "deactivated", string(body))
		resp.Body.Close()

		// Second deactivation request should still return OK (idempotent)
		resp, err = client.Get("http://localhost:3002" + _DEACTIVATE_PATH)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		body, err = io.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.Equal(t, "deactivated", string(body))
		resp.Body.Close()
	})

	t.Run("Invalid JSON handling", func(t *testing.T) {
		// Launch DBOS with admin server
		ctx, err := NewDBOSContext(Config{
			DatabaseURL: databaseURL,
			AppName:     "test-app",
			AdminServer: true,
			AdminPort:   3003, // Use different port to avoid conflicts
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

		client := &http.Client{Timeout: 5 * time.Second}

		tests := []struct {
			name           string
			endpoint       string
			body           string
			expectedStatus int
		}{
			{
				name:           "Workflows endpoint with invalid JSON",
				endpoint:       "http://localhost:3003" + _WORKFLOWS_PATH,
				body:           `{"invalid": json}`,
				expectedStatus: http.StatusBadRequest,
			},
			{
				name:           "Queued workflows endpoint with invalid JSON",
				endpoint:       "http://localhost:3003" + _QUEUED_WORKFLOWS_PATH,
				body:           `not json at all`,
				expectedStatus: http.StatusBadRequest,
			},
			{
				name:           "Fork endpoint with invalid JSON",
				endpoint:       "http://localhost:3003/workflows/test-123/fork",
				body:           `{"start_step": "not a number"}`,
				expectedStatus: http.StatusBadRequest,
			},
			{
				name:           "Garbage collect endpoint with invalid JSON",
				endpoint:       "http://localhost:3003" + _GARBAGE_COLLECT_PATH,
				body:           `{invalid}`,
				expectedStatus: http.StatusBadRequest,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				req, err := http.NewRequest("POST", tt.endpoint, strings.NewReader(tt.body))
				require.NoError(t, err)
				req.Header.Set("Content-Type", "application/json")

				resp, err := client.Do(req)
				require.NoError(t, err)
				defer resp.Body.Close()

				assert.Equal(t, tt.expectedStatus, resp.StatusCode)
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
