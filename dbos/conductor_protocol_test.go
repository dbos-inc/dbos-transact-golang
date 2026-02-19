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
