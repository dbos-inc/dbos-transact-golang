package sysdb

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// deleteFake answers the statements that an unsharded delete sends, in place of a
// database. It keeps each workflow's parent, and logs each children read and each
// delete of workflow_status rows with the IDs it names. It does not check the rest of
// the SQL.
type deleteFake struct {
	parentOf map[string]string
	deleted  map[string]bool
	log      []string
}

var (
	deleteChildrenTerm = regexp.MustCompile(`parent_workflow_id = ANY\(\$1\)`)
	deleteStatusTerm   = regexp.MustCompile(`DELETE FROM \S*workflow_status WHERE workflow_uuid = ANY\(\$1\)`)
	deletePayloadTerm  = regexp.MustCompile(`DELETE FROM \S*(workflow_input|workflow_output|operation_outputs) WHERE workflow_uuid = ANY\(\$1\)`)
)

func newDeleteFake() *deleteFake {
	return &deleteFake{
		parentOf: map[string]string{"parent": "", "c1": "parent", "c2": "parent", "g1": "c1", "other": "unrelated"},
		deleted:  map[string]bool{},
	}
}

func deleteIDsArg(args []any) ([]string, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("the statement has %d arguments, not 1", len(args))
	}
	ids, ok := args[0].([]string)
	if !ok {
		return nil, fmt.Errorf("$1 is bound to %T", args[0])
	}
	return ids, nil
}

func (f *deleteFake) query(query string, args []any) (Rows, error) {
	if !deleteChildrenTerm.MatchString(query) {
		return nil, errors.New("the statement is not a children statement")
	}
	ids, err := deleteIDsArg(args)
	if err != nil {
		return nil, err
	}
	f.log = append(f.log, fmt.Sprintf("list %v", ids))
	var out [][]any
	for _, id := range []string{"parent", "c1", "c2", "g1", "other"} {
		if !f.deleted[id] && slices.Contains(ids, f.parentOf[id]) {
			out = append(out, []any{id})
		}
	}
	return &fakeRows{rows: out}, nil
}

func (f *deleteFake) exec(query string, args []any) error {
	ids, err := deleteIDsArg(args)
	if err != nil {
		return err
	}
	switch {
	case deleteStatusTerm.MatchString(query):
		f.log = append(f.log, fmt.Sprintf("delete %v", ids))
		for _, id := range ids {
			f.deleted[id] = true
		}
		return nil
	case deletePayloadTerm.MatchString(query):
		return nil
	}
	return errors.New("the statement is not a delete statement")
}

// deleteConn is the pool, a transaction the pool began, or a caller's transaction, all
// over one deleteFake. It counts what it receives.
type deleteConn struct {
	fake                    *deleteFake
	queries, execs, commits int
	begun                   []*deleteConn
}

func (c *deleteConn) Query(_ context.Context, query string, args ...any) (Rows, error) {
	c.queries++
	return c.fake.query(query, args)
}

func (c *deleteConn) Exec(_ context.Context, query string, args ...any) (Result, error) {
	c.execs++
	return nil, c.fake.exec(query, args)
}

func (c *deleteConn) QueryRow(context.Context, string, ...any) Row { panic("not implemented") }

func (c *deleteConn) BeginTx(context.Context, TxOptions) (Tx, error) {
	tx := &deleteConn{fake: c.fake}
	c.begun = append(c.begun, tx)
	return tx, nil
}

func (c *deleteConn) Commit(context.Context) error   { c.commits++; return nil }
func (c *deleteConn) Rollback(context.Context) error { return nil }
func (c *deleteConn) Ping(context.Context) error     { return nil }
func (c *deleteConn) Close()                         {}

func newDeleteSysDB() (*SysDB, *deleteFake, *deleteConn) {
	fake := newDeleteFake()
	pool := &deleteConn{fake: fake}
	s := newFakeSysDB(nil)
	s.pool = pool
	return s, fake, pool
}

// Each level is deleted before its children are read, so a child that starts during
// the walk is read, or finds its parent gone.
var deleteWalk = []string{"delete [parent]", "list [parent]", "delete [c1 c2]", "list [c1 c2]", "delete [g1]", "list [g1]"}

// An unsharded delete with a caller's transaction runs every statement in it, children
// reads included, and none on the pool. It leaves the commit to the caller.
func TestDeleteWorkflowsUnshardedRunsInCallerTx(t *testing.T) {
	s, fake, pool := newDeleteSysDB()
	tx := &deleteConn{fake: fake}

	require.NoError(t, s.DeleteWorkflows(context.Background(), DeleteWorkflowsDBInput{WorkflowIDs: []string{"parent"}, DeleteChildren: true, Tx: tx}))
	assert.Equal(t, deleteWalk, fake.log)
	assert.Zero(t, pool.queries+pool.execs, "the pool runs no statement")
	assert.Empty(t, pool.begun, "the pool begins no transaction")
	assert.Zero(t, tx.commits, "the caller commits its own transaction")
	assert.False(t, fake.deleted["other"], "a workflow under another parent is not a child")
}

// An unsharded delete without a caller's transaction runs the whole walk in one
// transaction of the pool, and commits it once.
func TestDeleteWorkflowsUnshardedRunsInOneTx(t *testing.T) {
	s, fake, pool := newDeleteSysDB()

	require.NoError(t, s.DeleteWorkflows(context.Background(), DeleteWorkflowsDBInput{WorkflowIDs: []string{"parent"}, DeleteChildren: true}))
	assert.Equal(t, deleteWalk, fake.log)
	assert.Zero(t, pool.queries+pool.execs, "the pool runs no statement outside the transaction")
	require.Len(t, pool.begun, 1, "the walk runs in one transaction")
	assert.NotZero(t, pool.begun[0].queries, "the transaction reads the children")
	assert.Equal(t, 1, pool.begun[0].commits)
}

// An unsharded delete still refuses a shard key bound by the caller, before any statement.
func TestDeleteWorkflowsUnshardedRejectsShardKey(t *testing.T) {
	s, fake, _ := newDeleteSysDB()
	tx := &deleteConn{fake: fake}
	ctx := WithShardKeys(context.Background(), map[string]string{"parent": "k1"})

	err := s.DeleteWorkflows(ctx, DeleteWorkflowsDBInput{WorkflowIDs: []string{"parent"}, DeleteChildren: true, Tx: tx})
	require.ErrorIs(t, err, errShardKeyUnsharded)
	assert.Empty(t, fake.log, "no statement runs")
}
