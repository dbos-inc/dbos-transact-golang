package dbos

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const defaultAdminClientTimeout = 5 * time.Second

type AdminClient struct {
	baseURL    *url.URL
	httpClient *http.Client
}

type AdminClientError struct {
	StatusCode int
	Body       string
}

func (e *AdminClientError) Error() string {
	return fmt.Sprintf("admin API returned HTTP %d: %s", e.StatusCode, e.Body)
}

type AdminEpochMillis int64

func (m *AdminEpochMillis) UnmarshalJSON(data []byte) error {
	if bytes.Equal(data, []byte("null")) {
		*m = 0
		return nil
	}
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return fmt.Errorf("admin epoch milliseconds must be a JSON string or null: %w", err)
	}
	millis, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid admin epoch milliseconds %q: %w", value, err)
	}
	*m = AdminEpochMillis(millis)
	return nil
}

func (m AdminEpochMillis) Time() time.Time {
	return time.UnixMilli(int64(m))
}

type AdminHealthResponse struct {
	Status string `json:"status"`
}

type AdminListWorkflowsRequest struct {
	WorkflowUUIDs      []string           `json:"workflow_uuids,omitempty"`
	AuthenticatedUser  *string            `json:"authenticated_user,omitempty"`
	StartTime          *time.Time         `json:"start_time,omitempty"`
	EndTime            *time.Time         `json:"end_time,omitempty"`
	Status             WorkflowStatusType `json:"status,omitempty"`
	ApplicationVersion *string            `json:"application_version,omitempty"`
	WorkflowName       *string            `json:"workflow_name,omitempty"`
	Limit              *int               `json:"limit,omitempty"`
	Offset             *int               `json:"offset,omitempty"`
	SortDesc           *bool              `json:"sort_desc,omitempty"`
	WorkflowIDPrefix   *string            `json:"workflow_id_prefix,omitempty"`
	LoadInput          *bool              `json:"load_input,omitempty"`
	LoadOutput         *bool              `json:"load_output,omitempty"`
	QueueName          *string            `json:"queue_name,omitempty"`
}

type AdminWorkflow struct {
	WorkflowUUID            string             `json:"WorkflowUUID"`
	Status                  WorkflowStatusType `json:"Status"`
	WorkflowName            string             `json:"WorkflowName"`
	AuthenticatedUser       string             `json:"AuthenticatedUser"`
	AssumedRole             string             `json:"AssumedRole"`
	AuthenticatedRoles      []string           `json:"AuthenticatedRoles"`
	Output                  string             `json:"Output"`
	Error                   string             `json:"Error"`
	ExecutorID              string             `json:"ExecutorID"`
	ApplicationVersion      string             `json:"ApplicationVersion"`
	ApplicationID           string             `json:"ApplicationID"`
	Attempts                int                `json:"Attempts"`
	QueueName               string             `json:"QueueName"`
	Timeout                 time.Duration      `json:"Timeout"`
	DeduplicationID         string             `json:"DeduplicationID"`
	Priority                int                `json:"Priority"`
	QueuePartitionKey       string             `json:"QueuePartitionKey"`
	Input                   string             `json:"Input"`
	CreatedAt               AdminEpochMillis   `json:"CreatedAt"`
	UpdatedAt               AdminEpochMillis   `json:"UpdatedAt"`
	WorkflowDeadlineEpochMS AdminEpochMillis   `json:"WorkflowDeadlineEpochMS"`
	StartedAt               AdminEpochMillis   `json:"StartedAt"`
}

type AdminWorkflowStep struct {
	FunctionID         int    `json:"function_id"`
	FunctionName       string `json:"function_name"`
	ChildWorkflowID    string `json:"child_workflow_id"`
	StartedAtEpochMS   int64  `json:"started_at_epoch_ms"`
	CompletedAtEpochMS int64  `json:"completed_at_epoch_ms"`
	Output             string `json:"output"`
	Error              string `json:"error"`
}

type AdminForkWorkflowRequest struct {
	StartStep          *uint   `json:"start_step,omitempty"`
	NewWorkflowID      *string `json:"new_workflow_id,omitempty"`
	ApplicationVersion *string `json:"application_version,omitempty"`
}

type AdminForkWorkflowResponse struct {
	WorkflowID string `json:"workflow_id"`
}

type AdminGarbageCollectRequest struct {
	CutoffEpochTimestampMS *int64 `json:"cutoff_epoch_timestamp_ms,omitempty"`
	RowsThreshold          *int   `json:"rows_threshold,omitempty"`
}

type AdminGlobalTimeoutRequest struct {
	CutoffEpochTimestampMS int64 `json:"cutoff_epoch_timestamp_ms"`
}

func NewAdminClient(rawBaseURL string) (*AdminClient, error) {
	if rawBaseURL == "" {
		return nil, errors.New("admin client base URL is required")
	}
	baseURL, err := url.Parse(rawBaseURL)
	if err != nil {
		return nil, fmt.Errorf("invalid admin client base URL: %w", err)
	}
	if baseURL.Scheme == "" || baseURL.Host == "" {
		return nil, errors.New("admin client base URL must include scheme and host")
	}

	return &AdminClient{
		baseURL:    baseURL,
		httpClient: &http.Client{Timeout: defaultAdminClientTimeout},
	}, nil
}

func (c *AdminClient) Health(ctx context.Context) (AdminHealthResponse, error) {
	return doAdminRequest[AdminHealthResponse](c, ctx, http.MethodGet, "/dbos-healthz", nil)
}

func (c *AdminClient) RecoverWorkflows(ctx context.Context, executorIDs []string) ([]string, error) {
	return doAdminRequest[[]string](c, ctx, http.MethodPost, "/dbos-workflow-recovery", executorIDs)
}

func (c *AdminClient) ListQueues(ctx context.Context) ([]WorkflowQueue, error) {
	return doAdminRequest[[]WorkflowQueue](c, ctx, http.MethodGet, "/dbos-workflow-queues-metadata", nil)
}

func (c *AdminClient) GarbageCollect(ctx context.Context, request AdminGarbageCollectRequest) error {
	return doAdminRequestWithoutResponse(c, ctx, http.MethodPost, "/dbos-garbage-collect", request)
}

func (c *AdminClient) GlobalTimeout(ctx context.Context, cutoffTime time.Time) error {
	return doAdminRequestWithoutResponse(c, ctx, http.MethodPost, "/dbos-global-timeout", AdminGlobalTimeoutRequest{
		CutoffEpochTimestampMS: cutoffTime.UnixMilli(),
	})
}

func (c *AdminClient) ListWorkflows(ctx context.Context, request AdminListWorkflowsRequest) ([]AdminWorkflow, error) {
	return doAdminRequest[[]AdminWorkflow](c, ctx, http.MethodPost, "/workflows", request)
}

func (c *AdminClient) ListQueuedWorkflows(ctx context.Context, request AdminListWorkflowsRequest) ([]AdminWorkflow, error) {
	return doAdminRequest[[]AdminWorkflow](c, ctx, http.MethodPost, "/queues", request)
}

func (c *AdminClient) GetWorkflow(ctx context.Context, workflowID string) (AdminWorkflow, error) {
	return doAdminRequest[AdminWorkflow](c, ctx, http.MethodGet, "/workflows/"+url.PathEscape(workflowID), nil)
}

func (c *AdminClient) GetWorkflowSteps(ctx context.Context, workflowID string) ([]AdminWorkflowStep, error) {
	return doAdminRequest[[]AdminWorkflowStep](c, ctx, http.MethodGet, "/workflows/"+url.PathEscape(workflowID)+"/steps", nil)
}

func (c *AdminClient) CancelWorkflow(ctx context.Context, workflowID string) error {
	return doAdminRequestWithoutResponse(c, ctx, http.MethodPost, "/workflows/"+url.PathEscape(workflowID)+"/cancel", nil)
}

func (c *AdminClient) ResumeWorkflow(ctx context.Context, workflowID string) error {
	return doAdminRequestWithoutResponse(c, ctx, http.MethodPost, "/workflows/"+url.PathEscape(workflowID)+"/resume", nil)
}

func (c *AdminClient) ForkWorkflow(ctx context.Context, workflowID string, request AdminForkWorkflowRequest) (AdminForkWorkflowResponse, error) {
	return doAdminRequest[AdminForkWorkflowResponse](c, ctx, http.MethodPost, "/workflows/"+url.PathEscape(workflowID)+"/fork", request)
}

func (c *AdminClient) Deactivate(ctx context.Context) error {
	return doAdminRequestWithoutResponse(c, ctx, http.MethodGet, "/deactivate", nil)
}

func doAdminRequest[T any](client *AdminClient, ctx context.Context, method, path string, body any) (T, error) {
	var result T
	response, err := client.do(ctx, method, path, body)
	if err != nil {
		return result, err
	}
	defer response.Body.Close()
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return result, fmt.Errorf("decoding admin API response: %w", err)
	}
	return result, nil
}

func doAdminRequestWithoutResponse(client *AdminClient, ctx context.Context, method, path string, body any) error {
	response, err := client.do(ctx, method, path, body)
	if err != nil {
		return err
	}
	return response.Body.Close()
}

func (c *AdminClient) do(ctx context.Context, method, path string, body any) (*http.Response, error) {
	var requestBody io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("encoding admin API request: %w", err)
		}
		requestBody = bytes.NewReader(encoded)
	}

	requestURL := c.baseURL.JoinPath(strings.TrimPrefix(path, "/"))
	request, err := http.NewRequestWithContext(ctx, method, requestURL.String(), requestBody)
	if err != nil {
		return nil, fmt.Errorf("creating admin API request: %w", err)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}

	response, err := c.httpClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("calling admin API: %w", err)
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		defer response.Body.Close()
		responseBody, readErr := io.ReadAll(response.Body)
		if readErr != nil {
			return nil, fmt.Errorf("reading admin API error response: %w", readErr)
		}
		return nil, &AdminClientError{StatusCode: response.StatusCode, Body: strings.TrimSpace(string(responseBody))}
	}
	return response, nil
}
