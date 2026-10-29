package dbos

// Shard keys, on a sharded system database (Config.Sharding, a Transact Enterprise
// feature).
//
// Every workflow lives under a shard key, and every statement that names a workflow
// carries it, so a router such as PgDog, or a Citus coordinator, sends the statement to
// one shard. A workflow ID is unique only within its key.
//
// Starting or enqueueing a workflow names its key explicitly: WithShardKey,
// WithEnqueueShardKey, or ScheduleSpec.ShardKey. A workflow never inherits its caller's
// key, a child workflow included.
//
// Addressing an existing workflow names its key on the context, with BindShardKey or
// BindShardKeys: every operation run on the returned context (CancelWorkflow,
// GetEvent, ListWorkflows by ID, RetrieveWorkflow, ...) addresses the bound workflows
// under their keys. A handle carries its workflow's key, so its GetResult and GetStatus
// need no binding, and a workflow addresses its own rows by its own key.
//
// On an unsharded system database a shard key is an error.

import (
	"errors"

	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/sysdb"
)

func (c *dbosContext) BindShardKeys(_ Client, keys map[string]string) Client {
	return c.clone(sysdb.WithShardKeys(c.ctx, keys))
}

// BindShardKeys returns a copy of ctx in which each workflow ID in keys lives under its
// shard key, for the operations run on it that address those workflows. Bindings add
// to the ones already on ctx, and never change ctx itself. An empty key binds nothing.
//
//	keyed := dbos.BindShardKeys(ctx, map[string]string{"order-1": "tenant-a", "order-2": "tenant-b"})
//	err := dbos.CancelWorkflow(keyed, "order-1")
func BindShardKeys[C Client](ctx C, keys map[string]string) C {
	if any(ctx) == nil {
		return ctx
	}
	// The copy is of ctx's own kind, so a Context stays a Context.
	if bound, ok := ctx.BindShardKeys(ctx, keys).(C); ok {
		return bound
	}
	return ctx
}

func (c *dbosContext) BindShardKey(_ Client, workflowID, key string) Client {
	return c.clone(sysdb.WithShardKey(c.ctx, workflowID, key))
}

// BindShardKey binds one workflow ID to its shard key; see BindShardKeys.
//
//	value, err := dbos.GetEvent[string](dbos.BindShardKey(ctx, workflowID, "tenant-a"), workflowID, "status", time.Minute)
func BindShardKey[C Client](ctx C, workflowID, key string) C {
	if any(ctx) == nil {
		return ctx
	}
	// The copy is of ctx's own kind, so a Context stays a Context.
	if bound, ok := ctx.BindShardKey(ctx, workflowID, key).(C); ok {
		return bound
	}
	return ctx
}

func (c *dbosContext) GetShardKey() (string, error) {
	wfState, ok := c.Value(workflowStateKey).(*workflowState)
	if !ok || wfState == nil {
		return "", errors.New("not within a DBOS workflow context")
	}
	return wfState.shardKey, nil
}

// GetShardKey retrieves the shard key of the workflow running on ctx: empty on an
// unsharded system database. Returns an error if not called from within a workflow
// context.
//
// Example:
//
//	key, err := dbos.GetShardKey(ctx)
func GetShardKey(ctx Context) (string, error) {
	if ctx == nil {
		return "", errors.New("ctx cannot be nil")
	}
	return ctx.GetShardKey()
}
