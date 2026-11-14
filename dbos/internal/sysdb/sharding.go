package sysdb

// sharding.go: what a sharded system database needs beyond core's keyed statements.
//
// Core writes the shard key into every statement (see shard_keys.go). The commercial
// module supplies the rest through a ShardingBackend: the sharded schema and its
// migrations, router- and coordinator-specific behaviour, and the discovery of which
// shard keys hold a queue's work. Without the backend there is no sharded schema, so
// none of core's keyed statements can run.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ShardingBackendInput is what a sharded system database's backend is built from.
type ShardingBackendInput struct {
	Pool    *pgxpool.Pool // The system database pool, connected through the router or coordinator
	Schema  string        // The DBOS system schema
	AppName string        // The owning application; "" for a handle acting for every application
	Logger  *slog.Logger
}

// ShardingBackend is the commercial half of a sharded system database.
type ShardingBackend interface {
	// Migrate brings the sharded schema to its latest version.
	Migrate(ctx context.Context) error
	// VerifyMigrations errors unless the sharded schema is at the version this build requires.
	VerifyMigrations(ctx context.Context) error
	// IsSerializationConflict reports a conflict this backend reports in its own way,
	// such as Citus' commit-time conflict, beyond Postgres' serialization failures.
	IsSerializationConflict(err error) bool
	// ClaimSlotsPerKey is how many claims of one shard key's queue work may run at once.
	ClaimSlotsPerKey() int
	// DequeueScopes returns the shard keys with enqueued work on queueName, as last discovered.
	DequeueScopes(ctx context.Context, queueName string) ([]string, error)
	// NoteClaim records whether a claim from one of queueName's keys found work, or a
	// limit that left work behind, so drained keys are rediscovered early.
	NoteClaim(queueName string, found bool)
	// BackgroundJobLock tries the lock letting one of this application's processes run a
	// background job at a time. When acquired, release must be called to free it.
	BackgroundJobLock(ctx context.Context, name string) (acquired bool, release func(), err error)
}

// ShardingBackendFactory builds the backend of a sharded system database.
type ShardingBackendFactory func(ctx context.Context, input ShardingBackendInput) (ShardingBackend, error)

// ErrShardedSchema reports an unsharded handle pointed at a sharded schema: its
// statements name no shard key, so they would read and write every tenant's rows.
var ErrShardedSchema = errors.New("sharded system schema")

func shardedSchemaError(schema string) error {
	return fmt.Errorf("%w: the DBOS system schema %s is sharded. Enable sharding to use it: set Sharding in "+
		"dbos.Config or dbos.ClientConfig, or pass dbos.WithMigrateSharding to dbos.Migrate", ErrShardedSchema, schema)
}

// holdsShardedSchema reports whether schema's workflow_status has a shard_key column.
func holdsShardedSchema(ctx context.Context, pool *pgxpool.Pool, schema string) (bool, error) {
	var found bool
	err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = 'workflow_status' AND column_name = 'shard_key')`, schema).Scan(&found)
	if err != nil {
		return false, fmt.Errorf("failed to check whether schema %s is sharded: %w", schema, err)
	}
	return found, nil
}

// unsupportedWhenSharded refuses a feature a sharded system database does not support.
func unsupportedWhenSharded(what string) error {
	return fmt.Errorf("%s is not supported on a sharded system database", what)
}

// BackgroundJobLock tries the lock letting one of this application's processes run a
// background job at a time. Unsharded, every process runs every job, so it is always taken.
func (s *SysDB) BackgroundJobLock(ctx context.Context, name string) (bool, func(), error) {
	if s.sharding == nil {
		return true, func() {}, nil
	}
	return s.sharding.BackgroundJobLock(ctx, name)
}

// DequeueScopes returns the scopes a queue's workers dequeue from in turn: one keyless
// scope unless sharded, else the shard keys holding the queue's enqueued work.
func (s *SysDB) DequeueScopes(ctx context.Context, queueName string) ([]string, error) {
	if s.sharding == nil {
		return []string{""}, nil
	}
	return s.sharding.DequeueScopes(ctx, queueName)
}

// noteClaim records a scoped claim's outcome for the backend's discovery.
func (s *SysDB) noteClaim(queueName string, found bool) {
	if s.sharding != nil {
		s.sharding.NoteClaim(queueName, found)
	}
}

// claimGate returns the predicate admitting a few claimers per key at a time, binding
// its arguments next; the rest skip the key until their next poll. Concurrent claims of
// one key's oldest rows wait on one another despite SKIP LOCKED: a claim whose snapshot
// predates a row's claim locks its old version, and Postgres then locks the newer
// PENDING version as well, waiting on any lock there, until it commits. Hundreds of
// processes racing one key's backlog chained such waits for a minute at a time.
// Transaction-scoped, so it ends with the claim, and uncorrelated, so Postgres decides
// it once, before the claim scans. On Citus it runs on the key's worker.
func (s *SysDB) claimGate(queueName, key, partitionKey string, args *[]any) string {
	slots := 1
	if s.sharding != nil {
		slots = max(1, s.sharding.ClaimSlotsPerKey())
	}
	*args = append(*args, fmt.Sprintf("dbos-claim:%s:%s:%s", queueName, key, partitionKey), slots)
	return fmt.Sprintf(" AND (SELECT %s.claim_gate($%d, $%d))", pgx.Identifier{s.schema}.Sanitize(), len(*args)-1, len(*args))
}

// idsByKey runs query, whose rows lead with (workflow_uuid, shard_key), and groups the
// IDs by key, the keys in the order first seen: a sharded write acts under one key.
// Further columns scan into extra, overwritten row by row.
func (s *SysDB) idsByKey(ctx context.Context, query string, args []any, extra ...any) ([]string, map[string][]string, error) {
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	var keys []string
	byKey := make(map[string][]string)
	for rows.Next() {
		var id, key string
		if err := rows.Scan(append([]any{&id, &key}, extra...)...); err != nil {
			return nil, nil, err
		}
		if _, ok := byKey[key]; !ok {
			keys = append(keys, key)
		}
		byKey[key] = append(byKey[key], id)
	}
	return keys, byKey, rows.Err()
}
