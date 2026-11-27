package sysdb

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// How long a background job's lock may sit idle before Postgres ends its holder's session.
const backgroundJobLockIdleTimeoutMs = 5000

// HeldLock is a lock its holder keeps until Release.
type HeldLock interface {
	// Renew keeps the lock in place, erroring once it is lost.
	Renew(ctx context.Context) error
	Release()
}

type grantedLock struct{}

func (grantedLock) Renew(context.Context) error { return nil }
func (grantedLock) Release()                    {}

// BackgroundJobLock returns nil if another process holds the lock. CockroachDB and SQLite grant every try.
func (s *SysDB) BackgroundJobLock(ctx context.Context, jobName string) (HeldLock, error) {
	if s.sharding != nil {
		return s.sharding.BackgroundJobLock(ctx, jobName)
	}
	if s.dialect.Name() != DialectPostgres {
		return grantedLock{}, nil
	}
	key := AdvisoryLockKey(fmt.Sprintf("dbos.background_job.%s.%s.%s", s.schema, s.appName, jobName))
	tx, err := s.pool.BeginTx(ctx, TxOptions{IsoLevel: IsoLevelReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("failed to begin the %s lock's transaction: %w", jobName, err)
	}
	lock := &xactLock{s: s, tx: tx, jobName: jobName}
	// Unbound, in the simple protocol, so the idle session holds no snapshot; the local idle
	// timeout ends a frozen holder's session, releasing its lock.
	var acquired bool
	err = tx.QueryRow(ctx, fmt.Sprintf(
		"SELECT pg_try_advisory_xact_lock(%d) FROM set_config('idle_in_transaction_session_timeout', '%d', true)",
		key, backgroundJobLockIdleTimeoutMs), pgx.QueryExecModeSimpleProtocol).Scan(&acquired)
	if err != nil {
		lock.Release()
		return nil, fmt.Errorf("failed to try the %s lock: %w", jobName, err)
	}
	if !acquired {
		lock.Release()
		return nil, nil
	}
	return lock, nil
}

// xactLock is a transaction-scoped advisory lock, held while its transaction stays open.
type xactLock struct {
	s       *SysDB
	tx      Tx
	jobName string
}

func (l *xactLock) Renew(ctx context.Context) error {
	// Snapshot-free, so a concurrent index build never waits on it; fails if the session ended.
	_, err := l.tx.Exec(ctx, fmt.Sprintf("SET LOCAL idle_in_transaction_session_timeout = '%d'", backgroundJobLockIdleTimeoutMs),
		pgx.QueryExecModeSimpleProtocol)
	if err != nil {
		return fmt.Errorf("failed to renew the %s lock: %w", l.jobName, err)
	}
	return nil
}

func (l *xactLock) Release() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := l.tx.Rollback(ctx); err != nil {
		// A session the idle timeout ended has released the lock already.
		l.s.logger.Debug("Could not release the background job lock", "job", l.jobName, "error", err)
	}
}
