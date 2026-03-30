package dbos

// PostgreSQL stored function tests for direct SQL client calls.
// Ports python/tests/test_pgsql_client.py (commit e2d420b).

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// callEnqueueWorkflow calls the enqueue_workflow plpgsql stored function directly.
// Uses positional parameters (not named) for CockroachDB compatibility.
// Signature: enqueue_workflow(workflow_name, queue_name, positional_args, named_args,
//
//	class_name, config_name, workflow_id, app_version, timeout_ms,
//	deadline_epoch_ms, deduplication_id, priority, queue_partition_key)
func callEnqueueWorkflow(ctx context.Context, pool *pgxpool.Pool, schema string, params map[string]any) (string, error) {
	sanitized := pgx.Identifier{schema}.Sanitize()
	query := fmt.Sprintf(`SELECT %s.enqueue_workflow($1, $2, $3::json[], $4::json, $5, $6, $7, $8, $9, $10, $11, $12, $13)`, sanitized)

	get := func(key string) any { return params[key] }

	var wfID string
	err := pool.QueryRow(ctx, query,
		get("workflow_name"),       // $1
		get("queue_name"),          // $2
		get("positional_args"),     // $3
		get("named_args"),          // $4
		get("class_name"),          // $5
		get("config_name"),         // $6
		get("workflow_id"),         // $7
		get("app_version"),         // $8
		get("timeout_ms"),          // $9
		get("deadline_epoch_ms"),   // $10
		get("deduplication_id"),    // $11
		get("priority"),            // $12
		get("queue_partition_key"), // $13
	).Scan(&wfID)
	return wfID, err
}

// callSendMessage calls the send_message plpgsql stored function directly.
// Uses positional parameters (not named) for CockroachDB compatibility.
// Signature: send_message(destination_id, message, topic, message_id)
func callSendMessage(ctx context.Context, pool *pgxpool.Pool, schema string, destinationID string, message any, topic *string, messageID *string) error {
	sanitized := pgx.Identifier{schema}.Sanitize()
	query := fmt.Sprintf(`SELECT %s.send_message($1, $2::json, $3, $4)`, sanitized)

	msgJSON, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	_, err = pool.Exec(ctx, query, destinationID, string(msgJSON), topic, messageID)
	return err
}

// jsonArray encodes values as a []string of JSON-encoded elements, suitable for casting to json[] in Postgres.
func jsonArray(values ...any) ([]string, error) {
	out := make([]string, len(values))
	for i, v := range values {
		b, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		out[i] = string(b)
	}
	return out, nil
}

func TestPgsqlClient(t *testing.T) {
	serverCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	pool := serverCtx.(*dbosContext).systemDB.(*sysDB).pool
	schema := serverCtx.(*dbosContext).systemDB.(*sysDB).schema

	queue := NewWorkflowQueue(serverCtx, "pgsql-test-queue")

	// A simple workflow that joins its positional args into a string.
	type enqueueArgs struct {
		Num    int
		Str    string
		Person struct {
			First string
			Last  string
			Age   int
		}
	}
	enqueueWorkflow := func(ctx DBOSContext, args enqueueArgs) (string, error) {
		personJSON, err := json.Marshal(args.Person)
		if err != nil {
			return "", fmt.Errorf("failed to marshal person: %w", err)
		}
		return fmt.Sprintf("%d-%s-%s", args.Num, args.Str, string(personJSON)), nil
	}
	RegisterWorkflow(serverCtx, enqueueWorkflow, WithWorkflowName("pgsql_enqueue_test"))

	// A workflow that blocks until cancelled.
	blockedWorkflow := func(ctx DBOSContext, _ string) (string, error) {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(30 * time.Second):
			return "should-never-complete", nil
		}
	}
	RegisterWorkflow(serverCtx, blockedWorkflow, WithWorkflowName("pgsql_blocked_workflow"))

	// A workflow that just returns its input string.
	retrieveWorkflow := func(ctx DBOSContext, input string) (string, error) {
		return input, nil
	}
	RegisterWorkflow(serverCtx, retrieveWorkflow, WithWorkflowName("pgsql_retrieve_test"))

	// A workflow that waits for a message on a topic and returns it.
	recvWorkflow := func(ctx DBOSContext, topic string) (string, error) {
		return Recv[string](ctx, topic, 30*time.Second)
	}
	RegisterWorkflow(serverCtx, recvWorkflow, WithWorkflowName("pgsql_recv_test"))

	_ = queue

	err := Launch(serverCtx)
	require.NoError(t, err)

	t.Run("EnqueueAndGetResult", func(t *testing.T) {
		input := enqueueArgs{Num: 42, Str: "test"}
		input.Person.First = "John"
		input.Person.Last = "Doe"
		input.Person.Age = 30

		args, err := jsonArray(input, "extra-ignored-arg")
		require.NoError(t, err)

		wfID, err := callEnqueueWorkflow(context.Background(), pool, schema, map[string]any{
			"workflow_name":       "pgsql_enqueue_test",
			"queue_name":          queue.Name,
			"positional_args":     args,
			"named_args":          `{"ignored_key": "ignored_value"}`,
			"workflow_id":         nil,
			"app_version":         serverCtx.GetApplicationVersion(),
			"timeout_ms":          nil,
			"deadline_epoch_ms":   nil,
			"deduplication_id":    nil,
			"priority":            nil,
			"queue_partition_key": nil,
		})
		require.NoError(t, err)
		require.NotEmpty(t, wfID)

		handle, err := RetrieveWorkflow[string](serverCtx, wfID)
		require.NoError(t, err)
		result, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, `42-test-{"First":"John","Last":"Doe","Age":30}`, result)
	})

	t.Run("EnqueueWithTimeout", func(t *testing.T) {
		wfID, err := callEnqueueWorkflow(context.Background(), pool, schema, map[string]any{
			"workflow_name":       "pgsql_blocked_workflow",
			"queue_name":          queue.Name,
			"positional_args":     []string{`""`},
			"named_args":          `{}`,
			"workflow_id":         nil,
			"app_version":         serverCtx.GetApplicationVersion(),
			"timeout_ms":          int64(500),
			"deadline_epoch_ms":   nil,
			"deduplication_id":    nil,
			"priority":            nil,
			"queue_partition_key": nil,
		})
		require.NoError(t, err)

		handle, err := RetrieveWorkflow[string](serverCtx, wfID)
		require.NoError(t, err)
		_, err = handle.GetResult()
		require.Error(t, err, "expected timeout/cancellation error")
		dbosErr, ok := err.(*DBOSError)
		require.True(t, ok, "expected DBOSError, got %T: %v", err, err)
		assert.Equal(t, AwaitedWorkflowCancelled, dbosErr.Code)
	})

	t.Run("EnqueueIdempotent", func(t *testing.T) {
		wfID := fmt.Sprintf("pgsql-idempotent-%d", time.Now().UnixNano())
		params := map[string]any{
			"workflow_name":       "pgsql_retrieve_test",
			"queue_name":          queue.Name,
			"positional_args":     []string{`"idempotent-input"`},
			"named_args":          `{}`,
			"workflow_id":         wfID,
			"app_version":         serverCtx.GetApplicationVersion(),
			"timeout_ms":          nil,
			"deadline_epoch_ms":   nil,
			"deduplication_id":    nil,
			"priority":            nil,
			"queue_partition_key": nil,
		}

		id1, err := callEnqueueWorkflow(context.Background(), pool, schema, params)
		require.NoError(t, err)
		id2, err := callEnqueueWorkflow(context.Background(), pool, schema, params)
		require.NoError(t, err)
		assert.Equal(t, id1, id2)

		handle, err := RetrieveWorkflow[string](serverCtx, wfID)
		require.NoError(t, err)
		result, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, "idempotent-input", result)
	})

	t.Run("EnqueueWithDeduplication", func(t *testing.T) {
		wfID1 := fmt.Sprintf("pgsql-dedup-wf1-%d", time.Now().UnixNano())
		wfID2 := fmt.Sprintf("pgsql-dedup-wf2-%d", time.Now().UnixNano())
		dedupID := fmt.Sprintf("pgsql-dedup-%d", time.Now().UnixNano())

		// First enqueue succeeds.
		_, err := callEnqueueWorkflow(context.Background(), pool, schema, map[string]any{
			"workflow_name":       "pgsql_retrieve_test",
			"queue_name":          queue.Name,
			"positional_args":     []string{`"abc"`},
			"named_args":          `{}`,
			"workflow_id":         wfID1,
			"app_version":         serverCtx.GetApplicationVersion(),
			"timeout_ms":          nil,
			"deadline_epoch_ms":   nil,
			"deduplication_id":    dedupID,
			"priority":            nil,
			"queue_partition_key": nil,
		})
		require.NoError(t, err)

		// Same wfID again is idempotent.
		_, err = callEnqueueWorkflow(context.Background(), pool, schema, map[string]any{
			"workflow_name":       "pgsql_retrieve_test",
			"queue_name":          queue.Name,
			"positional_args":     []string{`"abc"`},
			"named_args":          `{}`,
			"workflow_id":         wfID1,
			"app_version":         serverCtx.GetApplicationVersion(),
			"timeout_ms":          nil,
			"deadline_epoch_ms":   nil,
			"deduplication_id":    dedupID,
			"priority":            nil,
			"queue_partition_key": nil,
		})
		require.NoError(t, err)

		// Different wfID with same dedup key must fail.
		_, err = callEnqueueWorkflow(context.Background(), pool, schema, map[string]any{
			"workflow_name":       "pgsql_retrieve_test",
			"queue_name":          queue.Name,
			"positional_args":     []string{`"def"`},
			"named_args":          `{}`,
			"workflow_id":         wfID2,
			"app_version":         serverCtx.GetApplicationVersion(),
			"timeout_ms":          nil,
			"deadline_epoch_ms":   nil,
			"deduplication_id":    dedupID,
			"priority":            nil,
			"queue_partition_key": nil,
		})
		require.Error(t, err)
		pgErr := &pgconn.PgError{}
		require.ErrorAs(t, err, &pgErr, "expected pgconn.PgError, got %T: %v", err, err)
		assert.Equal(t, _PG_ERROR_UNIQUE_VIOLATION, pgErr.Code)
		assert.Equal(t, "DBOS queue duplicated", pgErr.Message)
		assert.Contains(t, pgErr.Detail, fmt.Sprintf("Workflow %s with queue %s and deduplication ID %s already exists", wfID2, queue.Name, dedupID))

		handle, err := RetrieveWorkflow[string](serverCtx, wfID1)
		require.NoError(t, err)
		result, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, "abc", result)
	})

	t.Run("EnqueueWithPriority", func(t *testing.T) {
		wfID := fmt.Sprintf("pgsql-priority-%d", time.Now().UnixNano())

		_, err := callEnqueueWorkflow(context.Background(), pool, schema, map[string]any{
			"workflow_name":       "pgsql_retrieve_test",
			"queue_name":          queue.Name,
			"positional_args":     []string{`"priority-input"`},
			"named_args":          `{}`,
			"workflow_id":         wfID,
			"app_version":         serverCtx.GetApplicationVersion(),
			"timeout_ms":          nil,
			"deadline_epoch_ms":   nil,
			"deduplication_id":    nil,
			"priority":            int32(5),
			"queue_partition_key": nil,
		})
		require.NoError(t, err)

		// Verify priority is set correctly in the DB.
		sanitized := pgx.Identifier{schema}.Sanitize()
		var priority int
		err = pool.QueryRow(context.Background(),
			fmt.Sprintf(`SELECT priority FROM %s.workflow_status WHERE workflow_uuid = $1`, sanitized),
			wfID,
		).Scan(&priority)
		require.NoError(t, err)
		assert.Equal(t, 5, priority)

		handle, err := RetrieveWorkflow[string](serverCtx, wfID)
		require.NoError(t, err)
		result, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, "priority-input", result)
	})

	t.Run("SendWithTopic", func(t *testing.T) {
		topic := fmt.Sprintf("pgsql-topic-%d", time.Now().UnixNano())
		message := fmt.Sprintf("hello-pgsql-%d", time.Now().UnixNano())

		handle, err := RunWorkflow(serverCtx, recvWorkflow, topic, WithWorkflowID(fmt.Sprintf("pgsql-send-topic-%d", time.Now().UnixNano())))
		require.NoError(t, err)

		err = callSendMessage(context.Background(), pool, schema, handle.GetWorkflowID(), message, &topic, nil)
		require.NoError(t, err)

		result, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, message, result)
	})

	t.Run("SendNoTopic", func(t *testing.T) {
		message := fmt.Sprintf("hello-pgsql-notopic-%d", time.Now().UnixNano())
		defaultTopic := ""

		handle, err := RunWorkflow(serverCtx, recvWorkflow, defaultTopic, WithWorkflowID(fmt.Sprintf("pgsql-send-notopic-%d", time.Now().UnixNano())))
		require.NoError(t, err)

		err = callSendMessage(context.Background(), pool, schema, handle.GetWorkflowID(), message, nil, nil)
		require.NoError(t, err)

		result, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, message, result)
	})

	t.Run("SendIdempotent", func(t *testing.T) {
		topic := fmt.Sprintf("pgsql-idempotent-topic-%d", time.Now().UnixNano())
		message := fmt.Sprintf("hello-pgsql-idempotent-%d", time.Now().UnixNano())
		messageID := fmt.Sprintf("pgsql-msg-id-%d", time.Now().UnixNano())

		handle, err := RunWorkflow(serverCtx, recvWorkflow, topic, WithWorkflowID(fmt.Sprintf("pgsql-send-idempotent-%d", time.Now().UnixNano())))
		require.NoError(t, err)

		// Send the same message twice with the same message ID.
		err = callSendMessage(context.Background(), pool, schema, handle.GetWorkflowID(), message, &topic, &messageID)
		require.NoError(t, err)
		err = callSendMessage(context.Background(), pool, schema, handle.GetWorkflowID(), message, &topic, &messageID)
		require.NoError(t, err)

		// Verify only one notification row exists.
		sanitized := pgx.Identifier{schema}.Sanitize()
		var count int
		err = pool.QueryRow(context.Background(),
			fmt.Sprintf(`SELECT COUNT(*) FROM %s.notifications WHERE destination_uuid = $1`, sanitized),
			handle.GetWorkflowID(),
		).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 1, count)

		result, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, message, result)
	})
}
