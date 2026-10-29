package sysdb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

// The ID formats are fixed: every process on a sharded system database derives the same
// IDs, so any of them can replay another's cross-key work. Lengths count code points.
func TestKeyedIDFormats(t *testing.T) {
	assert.Equal(t, "wf", KeyedID("", "wf"))
	assert.Equal(t, "8:tenant-a:wf", KeyedID("tenant-a", "wf"))
	assert.Equal(t, "4:café:wf", KeyedID("café", "wf"))
	assert.Equal(t, "3:a:b:x:y", KeyedID("a:b", "x:y"))

	// A child under its parent's key keeps the plain ID; under another, it names the parent's key.
	assert.Equal(t, "p-3", ChildWorkflowID("p", 3, "", ""))
	assert.Equal(t, "p-3", ChildWorkflowID("p", 3, "k", "k"))
	assert.Equal(t, "1:k:p-3", ChildWorkflowID("p", 3, "k", "other"))

	assert.Equal(t, "1:k:sender::2::0", MessageIdempotencyKey("k", "sender", 2, 0))
	assert.Equal(t, "2:キー:wf::recv", waiterPayload("キー", "wf", "recv"))
}

func TestParseKeyedID(t *testing.T) {
	for _, tc := range []struct{ key, id string }{
		{"tenant-a", "wf"},
		{"café", "wf"},
		{"a:b", "x:y"},
		{"キー", "8:nested:id"},
	} {
		key, id, ok := parseKeyedID(KeyedID(tc.key, tc.id))
		assert.True(t, ok, tc.key)
		assert.Equal(t, tc.key, key)
		assert.Equal(t, tc.id, id)
	}
	for _, malformed := range []string{"", "wf", ":k:wf", "0:k:wf", "x:k:wf", "5:k:wf", "1:kx", "2:é"} {
		_, id, ok := parseKeyedID(malformed)
		assert.False(t, ok, malformed)
		assert.Equal(t, malformed, id)
	}
}

func TestSplitWaiterPayload(t *testing.T) {
	sharded := &SysDB{sharded: true}
	key, id, name, ok := sharded.splitWaiterPayload(waiterPayload("k:1", "wf::x", "recv"))
	assert.True(t, ok)
	assert.Equal(t, "k:1", key)
	assert.Equal(t, "wf", id)
	assert.Equal(t, "x::recv", name)

	_, _, _, ok = sharded.splitWaiterPayload("wf::recv")
	assert.False(t, ok, "a sharded payload names its key")

	unsharded := &SysDB{}
	key, id, name, ok = unsharded.splitWaiterPayload("wf::recv")
	assert.True(t, ok)
	assert.Empty(t, key)
	assert.Equal(t, "wf", id)
	assert.Equal(t, "recv", name)
}

func TestKeyResolution(t *testing.T) {
	ctx := WithShardKeys(context.Background(), map[string]string{"bound": "kb", "own": "kx"})
	ctx = WithShardKeyScope(ctx, "scope")
	s := &SysDB{sharded: true}

	key, err := s.keyOf(ctx, "bound")
	assert.NoError(t, err)
	assert.Equal(t, "kb", key, "a binding beats the scope")

	key, err = s.keyOf(ctx, "unbound")
	assert.NoError(t, err)
	assert.Equal(t, "scope", key, "the scope covers what is not bound")

	_, err = s.keyOf(context.Background(), "unbound")
	assert.Error(t, err, "a sharded operation needs a key")

	// A workflow's own key covers its own rows, and nothing else.
	own := WithWorkflowShardKey(ctx, "own", "k-own")
	key, err = s.ownKeyOf(own, "own")
	assert.NoError(t, err)
	assert.Equal(t, "k-own", key)
	assert.Empty(t, BoundShardKey(own, "bound"), "a workflow does not inherit its caller's bindings")

	// Unsharded, a key anywhere is an error, and no key is none.
	unsharded := &SysDB{}
	key, err = unsharded.keyOf(context.Background(), "wf")
	assert.NoError(t, err)
	assert.Empty(t, key)
	assert.Error(t, unsharded.rejectShardKey(ctx, "bound", ""))
	assert.Error(t, unsharded.rejectShardKey(context.Background(), "wf", "explicit"))
}
