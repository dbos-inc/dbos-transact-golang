package sysdb

// shard_keys.go: the shard keys a caller supplies for the workflows it addresses.
//
// A sharded system database stores every workflow under a shard key, and every
// statement that names a workflow carries that key so a router (PgDog) or a
// coordinator (Citus) sends it to one shard. Core only records keys; on an
// unsharded database nothing here is consulted, and a key that is given anyway
// is an error.
//
// Keys travel in the context:
//   - bindings map workflow IDs to keys (the public API, handles, recovery, Conductor);
//   - the scope is one key for statements that name no single workflow (the queue loop);
//   - the own key is the running workflow's, set when it starts.
//
// Resolution, for a workflow a statement addresses: bound, then own, then scope.
// A workflow writing its own rows (steps, events, streams) resolves own first, so a
// same-ID workflow bound as a target under another key cannot capture its writes.

import (
	"context"
	"fmt"
	"maps"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/models"
)

// GlobalShardKey is the shard key of every row in a global table: queues,
// schedules, application versions, and the migration version row.
const GlobalShardKey = "__dbos_global__"

type shardBindingsCtxKey struct{}
type shardScopeCtxKey struct{}
type shardOwnCtxKey struct{}

type ownShardKey struct {
	workflowID string
	key        string
}

// WithShardKeys returns a context in which each workflow ID in keys lives under its
// shard key. Empty keys are ignored. Bindings merge over the parent's into a fresh
// map, so a binding never changes the context it was derived from.
func WithShardKeys(ctx context.Context, keys map[string]string) context.Context {
	var given map[string]string
	for id, key := range keys {
		if key == "" {
			continue
		}
		if given == nil {
			given = make(map[string]string, len(keys))
		}
		given[id] = key
	}
	if len(given) == 0 {
		return ctx
	}
	merged := make(map[string]string, len(given))
	if outer, ok := ctx.Value(shardBindingsCtxKey{}).(map[string]string); ok {
		maps.Copy(merged, outer)
	}
	maps.Copy(merged, given)
	return context.WithValue(ctx, shardBindingsCtxKey{}, merged)
}

// WithShardKey binds one workflow ID to its shard key; an empty key binds nothing.
func WithShardKey(ctx context.Context, workflowID, key string) context.Context {
	if key == "" {
		return ctx
	}
	return WithShardKeys(ctx, map[string]string{workflowID: key})
}

// BoundShardKey is the shard key bound for workflowID, or "".
func BoundShardKey(ctx context.Context, workflowID string) string {
	if ctx == nil {
		return ""
	}
	bindings, _ := ctx.Value(shardBindingsCtxKey{}).(map[string]string)
	return bindings[workflowID]
}

// WithShardKeyScope returns a context in which statements that name no single
// workflow run under key.
func WithShardKeyScope(ctx context.Context, key string) context.Context {
	return context.WithValue(ctx, shardScopeCtxKey{}, key)
}

// ScopedShardKey is the enclosing scope's shard key, or "".
func ScopedShardKey(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	key, _ := ctx.Value(shardScopeCtxKey{}).(string)
	return key
}

// WithWorkflowShardKey returns the context a workflow body runs in: no binding or
// scope of its caller's, and its own key. A workflow never runs under its caller's keys.
func WithWorkflowShardKey(ctx context.Context, workflowID, key string) context.Context {
	if _, ok := ctx.Value(shardBindingsCtxKey{}).(map[string]string); ok {
		ctx = context.WithValue(ctx, shardBindingsCtxKey{}, map[string]string(nil))
	}
	if ScopedShardKey(ctx) != "" {
		ctx = context.WithValue(ctx, shardScopeCtxKey{}, "")
	}
	return context.WithValue(ctx, shardOwnCtxKey{}, ownShardKey{workflowID: workflowID, key: key})
}

// OwnShardKey is the running workflow's shard key, if it is workflowID; else "".
func OwnShardKey(ctx context.Context, workflowID string) string {
	if ctx == nil {
		return ""
	}
	own, _ := ctx.Value(shardOwnCtxKey{}).(ownShardKey)
	if own.workflowID != workflowID {
		return ""
	}
	return own.key
}

// KeyedID prefixes id with key, length first, so two (key, id) pairs never collide:
// "<len(key)>:<key>:<id>". The length counts code points, as every DBOS SDK does, so
// a key with non-ASCII characters derives the same ID in each language. An empty key
// leaves id unchanged.
func KeyedID(key, id string) string {
	if key == "" {
		return id
	}
	return fmt.Sprintf("%d:%s:%s", utf8.RuneCountInString(key), key, id)
}

// ChildWorkflowID is a child's default ID: its parent's, suffixed with the step that
// started it. A parent ID is unique only within its shard key, so a child under
// another key carries its parent's key as well.
func ChildWorkflowID(parentID string, stepID int, parentKey, childKey string) string {
	base := fmt.Sprintf("%s-%d", parentID, stepID)
	if parentKey == "" || childKey == "" || childKey == parentKey {
		return base
	}
	return KeyedID(parentKey, base)
}

// MessageIdempotencyKey is the idempotency key a workflow's send derives for its i-th
// message when none is given, so a replay is a no-op and a same-ID sender under another
// key does not collide.
func MessageIdempotencyKey(senderKey, senderID string, stepID, i int) string {
	return fmt.Sprintf("%s::%d::%d", KeyedID(senderKey, senderID), stepID, i)
}

func errNoShardKeyFor(workflowID string) error {
	return models.NewInvalidOptionError(fmt.Sprintf(
		"No shard key for workflow %s. On a sharded system database, pass the workflow's shard key "+
			"(dbos.WithShardKey or dbos.BindShardKeys), or use a handle that carries it", workflowID))
}

var errNoShardKeyScope = models.NewInvalidOptionError(
	"No shard key for this operation. On a sharded system database, it runs under one shard key")

var errShardKeyUnsharded = models.NewInvalidOptionError(
	"A shard key was given, but this system database is not sharded")

// Sharded reports whether this system database routes every statement by shard key.
func (s *SysDB) Sharded() bool { return s.sharded }

// rejectShardKey errors when an unsharded database is given a key for workflowID.
func (s *SysDB) rejectShardKey(ctx context.Context, workflowID, explicit string) error {
	if explicit != "" || BoundShardKey(ctx, workflowID) != "" {
		return errShardKeyUnsharded
	}
	return nil
}

// keyOf is the shard key a statement addresses workflowID under: explicit, bound by
// the caller, the running workflow's own, or the scope's. "" on an unsharded database.
func (s *SysDB) keyOf(ctx context.Context, workflowID string, explicit ...string) (string, error) {
	given := ""
	if len(explicit) > 0 {
		given = explicit[0]
	}
	if !s.sharded {
		return "", s.rejectShardKey(ctx, workflowID, given)
	}
	if given != "" {
		return given, nil
	}
	if key := BoundShardKey(ctx, workflowID); key != "" {
		return key, nil
	}
	if key := OwnShardKey(ctx, workflowID); key != "" {
		return key, nil
	}
	if key := ScopedShardKey(ctx); key != "" {
		return key, nil
	}
	return "", errNoShardKeyFor(workflowID)
}

// ownKeyOf is the key of a workflow writing its own rows: its running key first, even
// where a same-ID workflow is bound as a target under another key.
func (s *SysDB) ownKeyOf(ctx context.Context, workflowID string) (string, error) {
	if s.sharded {
		if key := OwnShardKey(ctx, workflowID); key != "" {
			return key, nil
		}
	}
	return s.keyOf(ctx, workflowID)
}

// resolvedKey is keyOf without the error: "" when nothing names the workflow's key.
func (s *SysDB) resolvedKey(ctx context.Context, workflowID string) string {
	if !s.sharded {
		return ""
	}
	key, _ := s.keyOf(ctx, workflowID)
	return key
}

// KeyHint is workflowID's shard key, resolved now for a statement or waiter that
// needs it later. "" on an unsharded database.
func (s *SysDB) KeyHint(ctx context.Context, workflowID string) (string, error) {
	return s.keyOf(ctx, workflowID)
}

// scopeKey is the key of a statement confined to the current shard, such as a queue's claim.
func (s *SysDB) scopeKey(ctx context.Context) (string, error) {
	if !s.sharded {
		return "", nil
	}
	if key := ScopedShardKey(ctx); key != "" {
		return key, nil
	}
	return "", errNoShardKeyScope
}

// globalKey is the key of a global table's rows: GlobalShardKey, or "" unsharded.
func (s *SysDB) globalKey() string {
	if !s.sharded {
		return ""
	}
	return GlobalShardKey
}

// andKey returns " AND shard_key = $n" binding key as the next argument, or "" for no key.
func andKey(key string, args *[]any) string {
	if key == "" {
		return ""
	}
	*args = append(*args, key)
	return fmt.Sprintf(" AND shard_key = $%d", len(*args))
}

// KeyClause is andKey for the commercial module.
func KeyClause(key string, args *[]any) string { return andKey(key, args) }

// keyColumn returns the shard key column for an INSERT's column list, or "".
func keyColumn(key string) string {
	if key == "" {
		return ""
	}
	return "shard_key, "
}

// keyValue returns the placeholder for an INSERT's shard key value, binding key as the
// next argument, or "".
func keyValue(key string, args *[]any) string {
	if key == "" {
		return ""
	}
	*args = append(*args, key)
	return fmt.Sprintf("$%d, ", len(*args))
}

// keyedColumns prefixes the columns identifying a row with shard_key when sharded, for a
// conflict target or a join.
func (s *SysDB) keyedColumns(columns string) string {
	if !s.sharded {
		return columns
	}
	return "shard_key, " + columns
}

// waiterPayload is the waiter-map entry for a workflow's topic or key, keyed by shard
// key when sharded, since one workflow ID can live under several keys.
func waiterPayload(key, workflowID, name string) string {
	return KeyedID(key, workflowID+"::"+name)
}

// parseKeyedID splits a KeyedID back into its key and ID. ok is false for an ID with
// no well-formed key prefix.
func parseKeyedID(keyed string) (key, id string, ok bool) {
	colon := strings.IndexByte(keyed, ':')
	if colon <= 0 {
		return "", keyed, false
	}
	n, err := strconv.Atoi(keyed[:colon])
	if err != nil || n <= 0 {
		return "", keyed, false
	}
	rest := keyed[colon+1:]
	// Walk n code points, the length KeyedID wrote.
	end := 0
	for i := 0; i < n; i++ {
		if end >= len(rest) {
			return "", keyed, false
		}
		_, size := utf8.DecodeRuneInString(rest[end:])
		end += size
	}
	if end >= len(rest) || rest[end] != ':' {
		return "", keyed, false
	}
	return rest[:end], rest[end+1:], true
}

// splitWaiterPayload recovers (key, workflowID, name) from a waiter payload.
func (s *SysDB) splitWaiterPayload(payload string) (key, workflowID, name string, ok bool) {
	rest := payload
	if s.sharded {
		if key, rest, ok = parseKeyedID(payload); !ok {
			return "", "", "", false
		}
	}
	parts := strings.SplitN(rest, "::", 2)
	if len(parts) != 2 {
		return "", "", "", false
	}
	return key, parts[0], parts[1], true
}

// ownResolvedKey is ownKeyOf without the error: "" when nothing names the workflow's key.
func (s *SysDB) ownResolvedKey(ctx context.Context, workflowID string) string {
	if !s.sharded {
		return ""
	}
	key, _ := s.ownKeyOf(ctx, workflowID)
	return key
}
