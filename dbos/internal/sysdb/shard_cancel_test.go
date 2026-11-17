package sysdb

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"testing"

	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type cancelWorkflow struct {
	id, key, parentID, parentKey string
	status                       models.WorkflowStatusType
}

// Core's tests link no sharding backend, so no database runs the sharded path. cancelFake
// answers the statements that a cancel sends, in place of a database. It resolves each
// placeholder it reads against the statement's arguments, and a placeholder that names no
// argument, or an argument of the wrong type, is an error. It reads these placeholders:
// in a sharded children statement, each parent key and list of IDs; in an unsharded one,
// its list of IDs; in a cancel statement, the first list of IDs, the first shard key and
// the new status. It does not check the rest of the SQL.
type cancelFake struct {
	workflows []*cancelWorkflow
}

var (
	shardedChildrenTerm   = regexp.MustCompile(`parent_shard_key = \$(\d+) AND parent_workflow_id = ANY\(\$(\d+)\)`)
	unshardedChildrenTerm = regexp.MustCompile(`parent_workflow_id = ANY\(\$(\d+)\)`)
	cancelIDsTerm         = regexp.MustCompile(`workflow_uuid = ANY\(\$(\d+)\)`)
	cancelKeyTerm         = regexp.MustCompile(` AND shard_key = \$(\d+)`)
	cancelStatusTerm      = regexp.MustCompile(`SET status = \$(\d+)`)
)

func placeholderArg[T any](args []any, placeholder string) (T, error) {
	var zero T
	n, err := strconv.Atoi(placeholder)
	if err != nil || n < 1 || n > len(args) {
		return zero, fmt.Errorf("placeholder $%s names no argument of %d", placeholder, len(args))
	}
	v, ok := args[n-1].(T)
	if !ok {
		return zero, fmt.Errorf("placeholder $%s is bound to %T", placeholder, args[n-1])
	}
	return v, nil
}

func (f *cancelFake) answer(query string, args []any) (Rows, error) {
	if terms := shardedChildrenTerm.FindAllStringSubmatch(query, -1); len(terms) > 0 {
		var out [][]any
		for _, term := range terms {
			key, err := placeholderArg[string](args, term[1])
			if err != nil {
				return nil, err
			}
			ids, err := placeholderArg[[]string](args, term[2])
			if err != nil {
				return nil, err
			}
			for _, w := range f.workflows {
				if w.parentKey == key && slices.Contains(ids, w.parentID) {
					out = append(out, []any{w.id, w.key})
				}
			}
		}
		return &fakeRows{rows: out}, nil
	}
	if term := unshardedChildrenTerm.FindStringSubmatch(query); term != nil {
		ids, err := placeholderArg[[]string](args, term[1])
		if err != nil {
			return nil, err
		}
		var out [][]any
		for _, w := range f.workflows {
			if slices.Contains(ids, w.parentID) {
				out = append(out, []any{w.id})
			}
		}
		return &fakeRows{rows: out}, nil
	}
	idsTerm := cancelIDsTerm.FindStringSubmatch(query)
	statusTerm := cancelStatusTerm.FindStringSubmatch(query)
	if idsTerm == nil || statusTerm == nil {
		return nil, errors.New("the statement is neither a children statement nor a cancel statement")
	}
	ids, err := placeholderArg[[]string](args, idsTerm[1])
	if err != nil {
		return nil, err
	}
	status, err := placeholderArg[models.WorkflowStatusType](args, statusTerm[1])
	if err != nil {
		return nil, err
	}
	key := ""
	if keyTerm := cancelKeyTerm.FindStringSubmatch(query); keyTerm != nil {
		if key, err = placeholderArg[string](args, keyTerm[1]); err != nil {
			return nil, err
		}
	}
	var out [][]any
	for _, w := range f.workflows {
		if w.key != key || !slices.Contains(ids, w.id) {
			continue
		}
		out = append(out, []any{w.id})
		if w.status != models.WorkflowStatusSuccess && w.status != models.WorkflowStatusError && w.status != models.WorkflowStatusCancelled {
			w.status = status
		}
	}
	return &fakeRows{rows: out}, nil
}

func (f *cancelFake) statuses() map[string]models.WorkflowStatusType {
	status := make(map[string]models.WorkflowStatusType)
	for _, w := range f.workflows {
		status[w.id] = w.status
	}
	return status
}

// newCancelSysDB returns a SysDB over a fake pool and a fake caller's transaction, which
// both answer from workflows.
func newCancelSysDB(sharded bool, workflows []*cancelWorkflow) (*SysDB, *cancelFake, *fakeQueryPool, *fakeTx) {
	fake := &cancelFake{workflows: workflows}
	pool := &fakeQueryPool{answer: fake.answer}
	s := newFakeSysDB(nil)
	s.pool, s.sharded = pool, sharded
	return s, fake, pool, &fakeTx{fakeQueryPool{answer: fake.answer}}
}

func shardedCancelFixture() []*cancelWorkflow {
	return []*cancelWorkflow{
		{id: "parent", key: "k1", status: models.WorkflowStatusPending},
		// Children of the parent, under three keys.
		{id: "c1", key: "k2", parentID: "parent", parentKey: "k1", status: models.WorkflowStatusPending},
		{id: "c2", key: "k3", parentID: "parent", parentKey: "k1", status: models.WorkflowStatusEnqueued},
		{id: "c3", key: "k1", parentID: "parent", parentKey: "k1", status: models.WorkflowStatusPending},
		// Grandchildren, one under each key of the children's level, so the next query holds three terms.
		{id: "g1", key: "k2", parentID: "c1", parentKey: "k2", status: models.WorkflowStatusPending},
		{id: "g2", key: "k1", parentID: "c2", parentKey: "k3", status: models.WorkflowStatusPending},
		{id: "g3", key: "k3", parentID: "c3", parentKey: "k1", status: models.WorkflowStatusPending},
		// Same parent ID under another key: not a child.
		{id: "other", key: "k1", parentID: "c2", parentKey: "k1", status: models.WorkflowStatusPending},
	}
}

func unshardedCancelFixture() []*cancelWorkflow {
	return []*cancelWorkflow{
		{id: "parent", status: models.WorkflowStatusPending},
		{id: "c1", parentID: "parent", status: models.WorkflowStatusPending},
		{id: "c2", parentID: "parent", status: models.WorkflowStatusEnqueued},
		{id: "g1", parentID: "c1", status: models.WorkflowStatusPending},
		{id: "other", parentID: "unrelated", status: models.WorkflowStatusPending},
	}
}

// A sharded cancel walks the tree one level at a time and reads the children of a level
// with one query: each parent's key and IDs are two placeholders of that query.
func TestCancelWorkflowsShardedChildrenAcrossKeys(t *testing.T) {
	s, fake, _, _ := newCancelSysDB(true, shardedCancelFixture())
	ctx := WithShardKeys(context.Background(), map[string]string{"parent": "k1"})

	found, err := s.CancelWorkflows(ctx, CancelWorkflowsDBInput{WorkflowIDs: []string{"parent"}, CancelChildren: true})
	require.NoError(t, err)
	assert.Equal(t, []string{"parent"}, found)

	status := fake.statuses()
	for _, id := range []string{"parent", "c1", "c2", "c3", "g1", "g2", "g3"} {
		assert.Equal(t, models.WorkflowStatusCancelled, status[id], "workflow %s", id)
	}
	assert.Equal(t, models.WorkflowStatusPending, status["other"], "a workflow under another parent key is not a child")
}

// A sharded cancel runs one statement per shard key outside the caller's transaction, so
// the transaction sees none of them.
func TestCancelWorkflowsShardedIgnoresCallerTx(t *testing.T) {
	s, _, pool, tx := newCancelSysDB(true, shardedCancelFixture())
	ctx := WithShardKeys(context.Background(), map[string]string{"parent": "k1"})

	_, err := s.CancelWorkflows(ctx, CancelWorkflowsDBInput{WorkflowIDs: []string{"parent"}, CancelChildren: true, Tx: tx})
	require.NoError(t, err)
	assert.Zero(t, tx.queries, "the caller's transaction runs no statement")
	assert.NotZero(t, pool.queries, "the pool runs them")
}

// An unsharded cancel with a caller's transaction runs every statement in it, children
// reads included, and none on the pool.
func TestCancelWorkflowsUnshardedRunsInCallerTx(t *testing.T) {
	s, fake, pool, tx := newCancelSysDB(false, unshardedCancelFixture())

	_, err := s.CancelWorkflows(context.Background(), CancelWorkflowsDBInput{WorkflowIDs: []string{"parent"}, CancelChildren: true, Tx: tx})
	require.NoError(t, err)
	assert.Zero(t, pool.queries, "the pool runs no statement")
	assert.NotZero(t, tx.queries, "the caller's transaction runs them")
	status := fake.statuses()
	for _, id := range []string{"parent", "c1", "c2", "g1"} {
		assert.Equal(t, models.WorkflowStatusCancelled, status[id], "workflow %s", id)
	}
	assert.Equal(t, models.WorkflowStatusPending, status["other"])
}

// An unsharded cancel without a caller's transaction runs its statements on the pool.
func TestCancelWorkflowsUnshardedRunsOnPool(t *testing.T) {
	s, fake, pool, tx := newCancelSysDB(false, unshardedCancelFixture())

	_, err := s.CancelWorkflows(context.Background(), CancelWorkflowsDBInput{WorkflowIDs: []string{"parent"}, CancelChildren: true})
	require.NoError(t, err)
	assert.NotZero(t, pool.queries, "the pool runs the statements")
	assert.Zero(t, tx.queries)
	assert.Equal(t, models.WorkflowStatusCancelled, fake.statuses()["g1"])
}
