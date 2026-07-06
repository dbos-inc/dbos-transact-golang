package dbos

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStringOrList_UnmarshalJSON(t *testing.T) {
	t.Run("single string", func(t *testing.T) {
		var s stringOrList
		err := json.Unmarshal([]byte(`"foo"`), &s)
		require.NoError(t, err)
		assert.Equal(t, []string{"foo"}, s.toSlice())
	})

	t.Run("array of strings", func(t *testing.T) {
		var s stringOrList
		err := json.Unmarshal([]byte(`["a","b","c"]`), &s)
		require.NoError(t, err)
		assert.Equal(t, []string{"a", "b", "c"}, s.toSlice())
	})

	t.Run("null", func(t *testing.T) {
		var s stringOrList
		err := json.Unmarshal([]byte(`null`), &s)
		require.NoError(t, err)
		assert.Nil(t, s.toSlice())
	})

	t.Run("empty array", func(t *testing.T) {
		var s stringOrList
		err := json.Unmarshal([]byte(`[]`), &s)
		require.NoError(t, err)
		assert.Equal(t, []string{}, s.toSlice())
	})
}

func TestListWorkflowsConductorRequestBody_StringOrListFields(t *testing.T) {
	t.Run("workflow_name as string", func(t *testing.T) {
		var req listWorkflowsConductorRequest
		err := json.Unmarshal([]byte(`{"type":"list_workflows","request_id":"x","body":{"workflow_name":"MyWorkflow"}}`), &req)
		require.NoError(t, err)
		assert.Equal(t, []string{"MyWorkflow"}, req.Body.WorkflowName.toSlice())
	})

	t.Run("workflow_name as array", func(t *testing.T) {
		var req listWorkflowsConductorRequest
		err := json.Unmarshal([]byte(`{"type":"list_workflows","request_id":"x","body":{"workflow_name":["A","B"]}}`), &req)
		require.NoError(t, err)
		assert.Equal(t, []string{"A", "B"}, req.Body.WorkflowName.toSlice())
	})

	t.Run("parent_workflow_id as string", func(t *testing.T) {
		var req listWorkflowsConductorRequest
		err := json.Unmarshal([]byte(`{"type":"list_workflows","request_id":"x","body":{"parent_workflow_id":"parent-123"}}`), &req)
		require.NoError(t, err)
		assert.Equal(t, []string{"parent-123"}, req.Body.ParentWorkflowID.toSlice())
	})

	t.Run("parent_workflow_id as array", func(t *testing.T) {
		var req listWorkflowsConductorRequest
		err := json.Unmarshal([]byte(`{"type":"list_workflows","request_id":"x","body":{"parent_workflow_id":["p1","p2"]}}`), &req)
		require.NoError(t, err)
		assert.Equal(t, []string{"p1", "p2"}, req.Body.ParentWorkflowID.toSlice())
	})
}

func TestGetWorkflowAggregatesConductorRequestBody_Unmarshal(t *testing.T) {
	t.Run("all group_by flags and a time bucket", func(t *testing.T) {
		var req getWorkflowAggregatesConductorRequest
		err := json.Unmarshal([]byte(`{
			"type":"get_workflow_aggregates",
			"request_id":"r1",
			"body":{
				"group_by_status":true,
				"group_by_name":true,
				"group_by_queue_name":true,
				"group_by_executor_id":true,
				"group_by_application_version":true,
				"time_bucket_size_ms":3600000
			}
		}`), &req)
		require.NoError(t, err)
		assert.True(t, req.Body.GroupByStatus)
		assert.True(t, req.Body.GroupByName)
		assert.True(t, req.Body.GroupByQueueName)
		assert.True(t, req.Body.GroupByExecutorID)
		assert.True(t, req.Body.GroupByApplicationVersion)
		require.NotNil(t, req.Body.TimeBucketSizeMs)
		assert.Equal(t, int64(3_600_000), *req.Body.TimeBucketSizeMs)
	})

	t.Run("status as single string", func(t *testing.T) {
		var req getWorkflowAggregatesConductorRequest
		err := json.Unmarshal([]byte(`{"type":"get_workflow_aggregates","request_id":"r","body":{"group_by_status":true,"status":"SUCCESS"}}`), &req)
		require.NoError(t, err)
		assert.Equal(t, []string{"SUCCESS"}, req.Body.Status.toSlice())
	})

	t.Run("name and app_version as arrays", func(t *testing.T) {
		var req getWorkflowAggregatesConductorRequest
		err := json.Unmarshal([]byte(`{"type":"get_workflow_aggregates","request_id":"r","body":{"group_by_name":true,"name":["wf1","wf2"],"app_version":["v1"]}}`), &req)
		require.NoError(t, err)
		assert.Equal(t, []string{"wf1", "wf2"}, req.Body.Name.toSlice())
		assert.Equal(t, []string{"v1"}, req.Body.AppVersion.toSlice())
	})

	t.Run("attributes filter", func(t *testing.T) {
		var req getWorkflowAggregatesConductorRequest
		err := json.Unmarshal([]byte(`{"type":"get_workflow_aggregates","request_id":"r","body":{"group_by_status":true,"attributes":{"customer":"acme","tier":1}}}`), &req)
		require.NoError(t, err)
		assert.Equal(t, "acme", req.Body.Attributes["customer"])
		assert.Equal(t, float64(1), req.Body.Attributes["tier"])
	})

	t.Run("relationship filters", func(t *testing.T) {
		var req getWorkflowAggregatesConductorRequest
		err := json.Unmarshal([]byte(`{"type":"get_workflow_aggregates","request_id":"r","body":{
			"group_by_status":true,
			"workflow_ids":["a","b"],
			"forked_from":"parent-1",
			"parent_workflow_id":["p1"],
			"user":"alice",
			"was_forked_from":true,
			"has_parent":false
		}}`), &req)
		require.NoError(t, err)
		assert.Equal(t, []string{"a", "b"}, req.Body.WorkflowIDs.toSlice())
		assert.Equal(t, []string{"parent-1"}, req.Body.ForkedFrom.toSlice())
		assert.Equal(t, []string{"p1"}, req.Body.ParentWorkflowID.toSlice())
		assert.Equal(t, []string{"alice"}, req.Body.User.toSlice())
		require.NotNil(t, req.Body.WasForkedFrom)
		assert.True(t, *req.Body.WasForkedFrom)
		require.NotNil(t, req.Body.HasParent)
		assert.False(t, *req.Body.HasParent)
	})
}
