package dbos

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func shardKeyTestWorkflow(ctx Context, x int) (int, error) { return x, nil }

// On an unsharded system database, every way of naming a shard key is an error, and a
// workflow's own key is empty.
func TestShardKeysRejectedUnsharded(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})
	RegisterWorkflow(dbosCtx, shardKeyTestWorkflow)
	queue, err := RegisterQueue(dbosCtx, "shard-key-queue")
	require.NoError(t, err)
	require.NoError(t, dbosCtx.Launch())

	_, err = RunWorkflow(dbosCtx, shardKeyTestWorkflow, 1, WithShardKey("k"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not sharded")

	_, err = RunWorkflow(dbosCtx, shardKeyTestWorkflow, 1, WithShardKey("k"), WithQueue(queue))
	require.Error(t, err)

	handle, err := RunWorkflow(dbosCtx, shardKeyTestWorkflow, 1, WithWorkflowID("plain"))
	require.NoError(t, err)
	_, err = handle.GetResult()
	require.NoError(t, err)
	status, err := handle.GetStatus()
	require.NoError(t, err)
	assert.Empty(t, status.ShardKey)

	// A binding names a key too.
	_, err = GetWorkflowSteps(BindShardKey(dbosCtx, "plain", "k"), "plain")
	require.Error(t, err)
	err = Send(dbosCtx, "plain", "m", "t", WithSendShardKey("k"))
	require.Error(t, err)

	// An empty binding binds nothing.
	_, err = GetWorkflowSteps(BindShardKey(dbosCtx, "plain", ""), "plain")
	require.NoError(t, err)

	_, err = GetShardKey(dbosCtx)
	require.Error(t, err, "outside a workflow")
}

// The package-level functions delegate to the context's methods, so a mock decides.
func TestBindShardKeysDelegates(t *testing.T) {
	ctx := &recordingContext{}
	assert.Same(t, Context(ctx), BindShardKeys(Context(ctx), map[string]string{"a": "ka", "b": "kb"}))
	assert.Equal(t, map[string]string{"a": "ka", "b": "kb"}, ctx.bound)
	assert.Same(t, Context(ctx), BindShardKey(Context(ctx), "wf", "k"))
	assert.Equal(t, map[string]string{"wf": "k"}, ctx.bound)

	key, err := GetShardKey(ctx)
	require.NoError(t, err)
	assert.Equal(t, "mock-key", key)

	_, err = GetShardKey(nil)
	require.Error(t, err)
}

type recordingContext struct {
	Context
	bound map[string]string
}

func (r *recordingContext) BindShardKeys(_ Client, keys map[string]string) Client {
	r.bound = keys
	return r
}

func (r *recordingContext) BindShardKey(_ Client, workflowID, key string) Client {
	r.bound = map[string]string{workflowID: key}
	return r
}

func (r *recordingContext) GetShardKey() (string, error) { return "mock-key", nil }
