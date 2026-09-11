package sysdb

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Rows a retention batch deletes per transaction.
const DefaultGCBatchSize = 50_000

// Keyed by workflow_uuid with no foreign key on workflow_status.
var payloadTables = []string{"workflow_input", "workflow_output", "operation_outputs"}

type GarbageCollectWorkflowsInput struct {
	CutoffEpochTimestampMs *int64
	RowsThreshold          *int
	BatchSize              *int // nil takes DefaultGCBatchSize
}

// GarbageCollectWorkflows runs one retention round over the entire system database.
func (s *SysDB) GarbageCollectWorkflows(ctx context.Context, input GarbageCollectWorkflowsInput) error {
	if input.RowsThreshold != nil && *input.RowsThreshold <= 0 {
		return fmt.Errorf("rowsThreshold must be greater than 0, got %d", *input.RowsThreshold)
	}
	batchSize := DefaultGCBatchSize
	if input.BatchSize != nil {
		if *input.BatchSize <= 0 {
			return fmt.Errorf("batchSize must be greater than 0, got %d", *input.BatchSize)
		}
		batchSize = *input.BatchSize
	}
	if input.CutoffEpochTimestampMs == nil && input.RowsThreshold == nil {
		return nil
	}

	release, acquired, err := s.acquireRetentionLock(ctx)
	if err != nil {
		return err
	}
	if !acquired {
		s.logger.Warn("Skipping retention: another round is already running against this system database.")
		return nil
	}
	defer release()

	cutoff, err := s.garbageCollectStatus(ctx, input.CutoffEpochTimestampMs, input.RowsThreshold, batchSize)
	if err != nil || cutoff == nil {
		return err
	}
	// Strictly after the status sweep: the payload sweep only takes orphans.
	return s.garbageCollectPayloads(ctx, *cutoff, batchSize)
}

// Every SDK derives the key this way, so rounds in different languages contend.
func retentionLockKey(schema string) int64 {
	sum := sha256.Sum256([]byte("dbos.retention." + schema))
	return int64(binary.BigEndian.Uint64(sum[:8])) // #nosec G115 -- the sign flip is the point: the key is a signed bigint
}

// Session-scoped advisory lock; engines without one always take it.
func (s *SysDB) acquireRetentionLock(ctx context.Context) (release func(), acquired bool, err error) {
	pool := PgxPool(s.pool)
	if pool == nil || s.isCockroachDB {
		return func() {}, true, nil
	}
	key := retentionLockKey(s.schema)
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("failed to acquire a connection for the retention lock: %w", err)
	}
	var locked bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", key).Scan(&locked); err != nil {
		conn.Release()
		return nil, false, fmt.Errorf("failed to take the retention lock: %w", err)
	}
	if !locked {
		conn.Release()
		return func() {}, false, nil
	}
	release = func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var released bool
		if err := conn.QueryRow(unlockCtx, "SELECT pg_advisory_unlock($1)", key).Scan(&released); err != nil {
			// The session may still hold the lock: keep it out of the pool.
			s.logger.Warn("Could not release the retention lock; closing its connection", "error", err)
			_ = conn.Hijack().Close(unlockCtx)
			return
		}
		if !released {
			s.logger.Warn("Could not release the retention lock: this session no longer holds it. " +
				"Retention will not proceed until the lock is released, which happens when the holding " +
				"backend closes. A transaction-pooling proxy in front of Postgres causes this; run DBOS " +
				"through a session-pooled or direct connection.")
		}
		conn.Release()
	}
	return release, true, nil
}

// Server notices, routed per connection.
var noticeSinks sync.Map

func forwardNotice(conn *pgconn.PgConn, notice *pgconn.Notice) {
	if sink, ok := noticeSinks.Load(conn); ok {
		sink.(func(*pgconn.Notice))(notice)
	}
}

func (s *SysDB) vacuumTables(ctx context.Context, tables []string) {
	pool := PgxPool(s.pool)
	if pool == nil || s.isCockroachDB {
		return
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		s.logger.Warn("Payload retention could not acquire a connection to vacuum", "error", err)
		return
	}
	defer conn.Release()

	// A refused VACUUM does not fail, it says so in a notice.
	var notices []string
	pgConn := conn.Conn().PgConn()
	noticeSinks.Store(pgConn, func(n *pgconn.Notice) { notices = append(notices, n.Message) })
	defer noticeSinks.Delete(pgConn)

	for _, table := range tables {
		notices = nil
		query := fmt.Sprintf("VACUUM (INDEX_CLEANUP ON, TRUNCATE OFF, ANALYZE) %s.%s", pgx.Identifier{s.schema}.Sanitize(), pgx.Identifier{table}.Sanitize())
		if _, err := conn.Exec(ctx, query, pgx.QueryExecModeSimpleProtocol); err != nil {
			s.logger.Warn("Payload retention could not vacuum table", "table", table, "error", err)
			continue
		}
		for _, notice := range notices {
			s.logger.Warn("Payload retention vacuuming table", "table", table, "notice", notice)
		}
	}
}

func (s *SysDB) retentionRetryOpts() []RetryOption {
	return []RetryOption{WithRetrierLogger(s.logger), WithRetryCondition(s.dialect.IsRetryableTransaction)}
}

// Returns the cutoff actually used, or nil when there is nothing to collect.
func (s *SysDB) garbageCollectStatus(ctx context.Context, cutoffEpochTimestampMs *int64, rowsThreshold *int, batchSize int) (*int64, error) {
	schemaPrefix := s.dialect.SchemaPrefix(s.schema)
	retryOpts := s.retentionRetryOpts()
	cutoff := cutoffEpochTimestampMs

	if rowsThreshold != nil {
		query := s.RenderSQL(`SELECT completed_at
				  FROM %sworkflow_status
				  WHERE completed_at IS NOT NULL
				  ORDER BY completed_at DESC
				  LIMIT 1 OFFSET $1`, schemaPrefix)
		rowsBasedCutoff, err := RetryWithResult(ctx, func() (*int64, error) {
			var found int64
			err := s.pool.QueryRow(ctx, query, *rowsThreshold-1).Scan(&found)
			if errors.Is(err, ErrNoRows) {
				return nil, nil
			}
			if err != nil {
				return nil, fmt.Errorf("failed to query cutoff timestamp by rows threshold: %w", err)
			}
			return &found, nil
		}, retryOpts...)
		if err != nil {
			return nil, err
		}
		if rowsBasedCutoff != nil && (cutoff == nil || *rowsBasedCutoff > *cutoff) {
			cutoff = rowsBasedCutoff
		}
	}
	if cutoff == nil {
		return nil, nil
	}

	seedQuery := s.RenderSQL(`SELECT completed_at FROM %sworkflow_status
			  WHERE completed_at < $1 ORDER BY completed_at LIMIT 1`, schemaPrefix)
	stepQuery := s.RenderSQL(`SELECT completed_at FROM %sworkflow_status
			  WHERE completed_at < $1 AND completed_at > $2
			  ORDER BY completed_at LIMIT 1 OFFSET $3`, schemaPrefix)
	batchQuery := s.RenderSQL(`DELETE FROM %sworkflow_status
			  WHERE completed_at < $1 AND completed_at > $2 AND completed_at <= $3`, schemaPrefix)
	// Unbounded: an import can land a completed_at below the watermark mid-pass.
	finalQuery := s.RenderSQL(`DELETE FROM %sworkflow_status WHERE completed_at < $1`, schemaPrefix)

	watermark, err := s.seedWatermark(ctx, seedQuery, *cutoff, retryOpts)
	if err != nil {
		return nil, err
	}
	var deletedCount int64
	for {
		step, err := RetryWithResult(ctx, func() (*int64, error) {
			tx, err := s.pool.BeginTx(ctx, TxOptions{})
			if err != nil {
				return nil, fmt.Errorf("failed to begin garbage collection batch: %w", err)
			}
			defer tx.Rollback(ctx)

			var step int64
			err = tx.QueryRow(ctx, stepQuery, *cutoff, watermark, batchSize-1).Scan(&step)
			final := errors.Is(err, ErrNoRows)
			if err != nil && !final {
				return nil, fmt.Errorf("failed to query garbage collection batch bound: %w", err)
			}
			query, args := batchQuery, []any{*cutoff, watermark, step}
			if final {
				query, args = finalQuery, []any{*cutoff}
			}
			result, err := tx.Exec(ctx, query, args...)
			if err != nil {
				return nil, fmt.Errorf("failed to garbage collect workflows: %w", err)
			}
			if err := tx.Commit(ctx); err != nil {
				return nil, fmt.Errorf("failed to commit garbage collection batch: %w", err)
			}
			affected, _ := result.RowsAffected()
			deletedCount += affected
			if final {
				return nil, nil
			}
			return &step, nil
		}, retryOpts...)
		if err != nil {
			return nil, err
		}
		if step == nil {
			break
		}
		watermark = *step
	}

	s.logger.Info("Garbage collected workflows", "cutoff_timestamp", *cutoff, "deleted_count", deletedCount)
	return cutoff, nil
}

func (s *SysDB) seedWatermark(ctx context.Context, seedQuery string, cutoff int64, retryOpts []RetryOption) (int64, error) {
	return RetryWithResult(ctx, func() (int64, error) {
		var oldest int64
		err := s.pool.QueryRow(ctx, seedQuery, cutoff).Scan(&oldest)
		if errors.Is(err, ErrNoRows) {
			return 0, nil
		}
		if err != nil {
			return 0, fmt.Errorf("failed to seed the garbage collection watermark: %w", err)
		}
		return oldest - 1, nil
	}, retryOpts...)
}

func (s *SysDB) garbageCollectPayloads(ctx context.Context, cutoff int64, batchSize int) error {
	s.vacuumTables(ctx, append([]string{"workflow_status"}, payloadTables...))

	deleted := make([]int64, len(payloadTables))
	failures := make([]error, len(payloadTables))
	var wg sync.WaitGroup
	for i, table := range payloadTables {
		sweep := func() {
			deleted[i], failures[i] = s.garbageCollectPayloadTable(ctx, table, cutoff, batchSize)
		}
		if PgxPool(s.pool) == nil {
			sweep()
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			sweep()
		}()
	}
	wg.Wait()
	if err := errors.Join(failures...); err != nil {
		return err
	}

	s.vacuumTables(ctx, payloadTables)
	s.logger.Debug("Payload retention deleted orphaned rows",
		"inputs", deleted[0], "outputs", deleted[1], "steps", deleted[2])
	return nil
}

func (s *SysDB) garbageCollectPayloadTable(ctx context.Context, table string, cutoff int64, batchSize int) (int64, error) {
	schemaPrefix := s.dialect.SchemaPrefix(s.schema)
	retryOpts := s.retentionRetryOpts()
	qualified := schemaPrefix + table

	orphaned := `NOT EXISTS (SELECT 1 FROM ` + schemaPrefix + `workflow_status ws
			  WHERE ws.workflow_uuid = ` + table + `.workflow_uuid AND ws.created_at < $1)`
	seedQuery := s.dialect.RewriteQuery(`SELECT retention_timestamp FROM ` + qualified + `
			  WHERE retention_timestamp < $1 ORDER BY retention_timestamp LIMIT 1`)
	stepQuery := s.dialect.RewriteQuery(`SELECT retention_timestamp FROM ` + qualified + `
			  WHERE retention_timestamp < $1 AND retention_timestamp > $2
			  ORDER BY retention_timestamp LIMIT 1 OFFSET $3`)
	batchQuery := s.dialect.RewriteQuery(`DELETE FROM ` + qualified + `
			  WHERE retention_timestamp < $1 AND retention_timestamp > $2 AND retention_timestamp <= $3 AND ` + orphaned)
	finalQuery := s.dialect.RewriteQuery(`DELETE FROM ` + qualified + `
			  WHERE retention_timestamp < $1 AND retention_timestamp > $2 AND ` + orphaned)

	var oldest int64
	err := s.pool.QueryRow(ctx, seedQuery, cutoff).Scan(&oldest)
	if errors.Is(err, ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("failed to seed the %s retention watermark: %w", table, err)
	}
	watermark := oldest - 1

	var total int64
	for {
		step, err := RetryWithResult(ctx, func() (*int64, error) {
			tx, err := s.pool.BeginTx(ctx, TxOptions{})
			if err != nil {
				return nil, fmt.Errorf("failed to begin %s retention batch: %w", table, err)
			}
			defer tx.Rollback(ctx)

			var step int64
			err = tx.QueryRow(ctx, stepQuery, cutoff, watermark, batchSize-1).Scan(&step)
			final := errors.Is(err, ErrNoRows)
			if err != nil && !final {
				return nil, fmt.Errorf("failed to query %s retention batch bound: %w", table, err)
			}
			query, args := batchQuery, []any{cutoff, watermark, step}
			if final {
				query, args = finalQuery, []any{cutoff, watermark}
			}
			result, err := tx.Exec(ctx, query, args...)
			if err != nil {
				return nil, fmt.Errorf("failed to garbage collect %s: %w", table, err)
			}
			if err := tx.Commit(ctx); err != nil {
				return nil, fmt.Errorf("failed to commit %s retention batch: %w", table, err)
			}
			affected, _ := result.RowsAffected()
			total += affected
			if final {
				return nil, nil
			}
			return &step, nil
		}, retryOpts...)
		if err != nil {
			return total, err
		}
		if step == nil {
			return total, nil
		}
		watermark = *step
	}
}
