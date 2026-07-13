package dbos

import (
	"context"
	"database/sql"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// dbq.go: a thin driver-agnostic SQL surface used by sysDB.
//
// Each backend type can implement the same interfaces:
//   pgxPoolAdapter  → wraps *pgxpool.Pool / pgx.Tx (Postgres + CockroachDB)
//   sqlPoolAdapter  → wraps *sql.DB / *sql.Tx     (SQLite via modernc.org/sqlite)
//
// Higher-level code (sysDB methods, the migration runner, runAsTxn in
// workflow.go) talks to these interfaces, so it does not have to know which
// driver is in use. Per-dialect query fragments (placeholder style, schema
// prefix, FOR UPDATE clauses, etc.) live in Dialect (see dialect.go).

// Querier is the subset of SQL operations available on both a pool and a
// transaction.
type Querier interface {
	Exec(ctx context.Context, query string, args ...any) (Result, error)
	Query(ctx context.Context, query string, args ...any) (Rows, error)
	QueryRow(ctx context.Context, query string, args ...any) Row
}

// Tx is a transaction that can also execute queries.
type Tx interface {
	Querier
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// Pool is a connection pool that can begin transactions.
type Pool interface {
	Querier
	BeginTx(ctx context.Context, opts TxOptions) (Tx, error)
	Ping(ctx context.Context) error
	Close()
}

// TxOptions configures a new transaction.
type TxOptions struct {
	IsoLevel IsoLevel
	ReadOnly bool
}

// IsoLevel is a portable isolation level. Dialects map it to their native enum.
type IsoLevel int

const (
	IsoLevelDefault IsoLevel = iota
	IsoLevelReadCommitted
	IsoLevelRepeatableRead
	IsoLevelSerializable
)

// Rows is a cursor over a multi-row result set.
type Rows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close() error
}

// Row is a single-row result that has not yet been scanned. Scan returns
// ErrNoRows if no row matched.
type Row interface {
	Scan(dest ...any) error
}

// Result describes the effects of a write. Mirrors database/sql.Result. For
// pgx-backed results, the int64 returned by pgconn.CommandTag.RowsAffected()
// is wrapped with a nil error.
type Result interface {
	RowsAffected() (int64, error)
}

// ErrNoRows is returned by Row.Scan when no row matched. Aliased to
// pgx.ErrNoRows so existing callers can continue to compare against pgx's
// sentinel; the sql adapter maps sql.ErrNoRows to this value as well.
var ErrNoRows = pgx.ErrNoRows

/* ---------------------------------------------------------------------------
   pgx adapter
   ------------------------------------------------------------------------- */

// newPgxPool wraps a *pgxpool.Pool so it satisfies Pool.
func newPgxPool(p *pgxpool.Pool) Pool { return &pgxPoolAdapter{p: p} }

type pgxPoolAdapter struct{ p *pgxpool.Pool }

func (a *pgxPoolAdapter) Exec(ctx context.Context, q string, args ...any) (Result, error) {
	tag, err := a.p.Exec(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return pgxResult{rows: tag.RowsAffected()}, nil
}

func (a *pgxPoolAdapter) Query(ctx context.Context, q string, args ...any) (Rows, error) {
	rows, err := a.p.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return &pgxRows{r: rows}, nil
}

func (a *pgxPoolAdapter) QueryRow(ctx context.Context, q string, args ...any) Row {
	return &pgxRow{r: a.p.QueryRow(ctx, q, args...)}
}

func (a *pgxPoolAdapter) BeginTx(ctx context.Context, opts TxOptions) (Tx, error) {
	tx, err := a.p.BeginTx(ctx, pgxTxOpts(opts))
	if err != nil {
		return nil, err
	}
	return &pgxTxAdapter{tx: tx}, nil
}

func (a *pgxPoolAdapter) Ping(ctx context.Context) error { return a.p.Ping(ctx) }
func (a *pgxPoolAdapter) Close()                         { a.p.Close() }

// PgxPool unwraps the underlying *pgxpool.Pool. Returns nil if the Pool is not
// pgx-backed. Used by call sites that still need pgx-specific features
// (notably the LISTEN/NOTIFY listener).
func PgxPool(p Pool) *pgxpool.Pool {
	if a, ok := p.(*pgxPoolAdapter); ok {
		return a.p
	}
	return nil
}

type pgxTxAdapter struct{ tx pgx.Tx }

func (t *pgxTxAdapter) Exec(ctx context.Context, q string, args ...any) (Result, error) {
	tag, err := t.tx.Exec(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return pgxResult{rows: tag.RowsAffected()}, nil
}

func (t *pgxTxAdapter) Query(ctx context.Context, q string, args ...any) (Rows, error) {
	rows, err := t.tx.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return &pgxRows{r: rows}, nil
}

func (t *pgxTxAdapter) QueryRow(ctx context.Context, q string, args ...any) Row {
	return &pgxRow{r: t.tx.QueryRow(ctx, q, args...)}
}

func (t *pgxTxAdapter) Commit(ctx context.Context) error   { return t.tx.Commit(ctx) }
func (t *pgxTxAdapter) Rollback(ctx context.Context) error { return t.tx.Rollback(ctx) }

type pgxRows struct{ r pgx.Rows }

func (r *pgxRows) Next() bool             { return r.r.Next() }
func (r *pgxRows) Scan(dest ...any) error { return r.r.Scan(dest...) }
func (r *pgxRows) Err() error             { return r.r.Err() }
func (r *pgxRows) Close() error           { r.r.Close(); return nil }

type pgxRow struct{ r pgx.Row }

func (r *pgxRow) Scan(dest ...any) error {
	if err := r.r.Scan(dest...); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNoRows
		}
		return err
	}
	return nil
}

type pgxResult struct{ rows int64 }

func (r pgxResult) RowsAffected() (int64, error) { return r.rows, nil }

func pgxTxOpts(o TxOptions) pgx.TxOptions {
	var iso pgx.TxIsoLevel
	switch o.IsoLevel {
	case IsoLevelReadCommitted:
		iso = pgx.ReadCommitted
	case IsoLevelRepeatableRead:
		iso = pgx.RepeatableRead
	case IsoLevelSerializable:
		iso = pgx.Serializable
	default:
		iso = "" // pgx default (ReadCommitted for Postgres)
	}
	mode := pgx.ReadWrite
	if o.ReadOnly {
		mode = pgx.ReadOnly
	}
	return pgx.TxOptions{IsoLevel: iso, AccessMode: mode}
}

/* ---------------------------------------------------------------------------
   database/sql adapter (used by SQLite)
   ------------------------------------------------------------------------- */

// newSQLPool wraps a *sql.DB so it satisfies Pool.
func newSQLPool(db *sql.DB) Pool { return &sqlPoolAdapter{db: db} }

// ctxErr attaches the context error to a driver error so callers can
// errors.Is(err, context.Canceled/DeadlineExceeded). modernc/sqlite does this
// substitution for statements (stmt.exec) but not for begin/commit/rollback
// (tx.exec).
func ctxErr(ctx context.Context, err error) error {
	if err == nil || ctx.Err() == nil || errors.Is(err, ctx.Err()) {
		return err
	}
	return errors.Join(err, ctx.Err())
}

type sqlPoolAdapter struct{ db *sql.DB }

func (a *sqlPoolAdapter) Exec(ctx context.Context, q string, args ...any) (Result, error) {
	res, err := a.db.ExecContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (a *sqlPoolAdapter) Query(ctx context.Context, q string, args ...any) (Rows, error) {
	rows, err := a.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return &sqlRows{r: rows}, nil
}

func (a *sqlPoolAdapter) QueryRow(ctx context.Context, q string, args ...any) Row {
	return &sqlRow{r: a.db.QueryRowContext(ctx, q, args...)}
}

func (a *sqlPoolAdapter) BeginTx(ctx context.Context, opts TxOptions) (Tx, error) {
	tx, err := a.db.BeginTx(ctx, sqlTxOpts(opts))
	if err != nil {
		return nil, ctxErr(ctx, err)
	}
	return &sqlTxAdapter{tx: tx}, nil
}

func (a *sqlPoolAdapter) Ping(ctx context.Context) error { return a.db.PingContext(ctx) }
func (a *sqlPoolAdapter) Close()                         { _ = a.db.Close() }

// SQLDB unwraps the underlying *sql.DB, or returns nil if not sql-backed.
func SQLDB(p Pool) *sql.DB {
	if a, ok := p.(*sqlPoolAdapter); ok {
		return a.db
	}
	return nil
}

// sameEngine reports whether two portable pools wrap the same underlying engine
// handle — the identical *pgxpool.Pool or *sql.DB.
func sameEngine(a, b Pool) bool {
	if pa := PgxPool(a); pa != nil {
		return pa == PgxPool(b)
	}
	if sa := SQLDB(a); sa != nil {
		return sa == SQLDB(b)
	}
	return false
}

type sqlTxAdapter struct{ tx *sql.Tx }

func (t *sqlTxAdapter) Exec(ctx context.Context, q string, args ...any) (Result, error) {
	res, err := t.tx.ExecContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (t *sqlTxAdapter) Query(ctx context.Context, q string, args ...any) (Rows, error) {
	rows, err := t.tx.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return &sqlRows{r: rows}, nil
}

func (t *sqlTxAdapter) QueryRow(ctx context.Context, q string, args ...any) Row {
	return &sqlRow{r: t.tx.QueryRowContext(ctx, q, args...)}
}

func (t *sqlTxAdapter) Commit(ctx context.Context) error   { return ctxErr(ctx, t.tx.Commit()) }
func (t *sqlTxAdapter) Rollback(ctx context.Context) error { return ctxErr(ctx, t.tx.Rollback()) }

type sqlRows struct{ r *sql.Rows }

func (r *sqlRows) Next() bool             { return r.r.Next() }
func (r *sqlRows) Scan(dest ...any) error { return r.r.Scan(dest...) }
func (r *sqlRows) Err() error             { return r.r.Err() }
func (r *sqlRows) Close() error           { return r.r.Close() }

type sqlRow struct{ r *sql.Row }

func (r *sqlRow) Scan(dest ...any) error {
	if err := r.r.Scan(dest...); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNoRows
		}
		return err
	}
	return nil
}

func sqlTxOpts(o TxOptions) *sql.TxOptions {
	var iso sql.IsolationLevel
	switch o.IsoLevel {
	case IsoLevelReadCommitted:
		iso = sql.LevelReadCommitted
	case IsoLevelRepeatableRead:
		iso = sql.LevelRepeatableRead
	case IsoLevelSerializable:
		iso = sql.LevelSerializable
	default:
		iso = sql.LevelDefault
	}
	return &sql.TxOptions{Isolation: iso, ReadOnly: o.ReadOnly}
}
