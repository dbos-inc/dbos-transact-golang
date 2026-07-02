package dbos

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"regexp"
	"strings"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	sqlitelib "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// dialect.go: per-backend SQL fragments and behaviours.
//
// The same sysDB struct is reused across Postgres, CockroachDB, and SQLite.
// All per-backend differences (placeholder style, schema prefix, lock clauses,
// error classification, listen/notify support, migration set) live behind the
// Dialect interface so sysDB methods stay dialect-agnostic.
//
// Conventions
//   - Canonical query strings in sysDB are written in Postgres syntax with $N
//     placeholders and a "%s" schema-prefix slot rendered via fmt.Sprintf.
//     SQLite-flavor methods that diverge enough call dialect.RewriteQuery to
//     convert $N → ?
//   - SQL-level functions that don't exist in SQLite (gen_random_uuid,
//     now()-epoch math) are not used in canonical queries; Go callers supply
//     explicit values (uuid.NewString(), time.Now().UnixMilli()).

// DialectName identifies the backend. Stable string suitable for logging.
type DialectName string

const (
	DialectPostgres  DialectName = "postgres"
	DialectCockroach DialectName = "cockroach"
	DialectSQLite    DialectName = "sqlite"
)

// Dialect encapsulates per-backend SQL fragments and behaviours.
type Dialect interface {
	// Name returns a stable identifier for the dialect.
	Name() DialectName

	// SchemaPrefix returns the qualified-table prefix, e.g. `"dbos".` for
	// Postgres or "" for SQLite (no schemas). Includes the trailing dot.
	SchemaPrefix(schema string) string

	// RewriteQuery converts a canonical Postgres-style query (with $N
	// placeholders and a "%s" schema-prefix slot already rendered) into the
	// dialect's native form. For Postgres this is a no-op; for SQLite it
	// rewrites $N to ? in left-to-right order.
	RewriteQuery(query string) string

	// LockSkipLocked returns the "FOR UPDATE SKIP LOCKED" fragment, or "" for
	// dialects that don't support row-level locking (SQLite).
	LockSkipLocked() string

	// LockNoWait returns the "FOR UPDATE NOWAIT" fragment, or "".
	LockNoWait() string

	// SnapshotIsolation returns the IsoLevel to request when a transaction
	// needs snapshot-style semantics for queue dequeue. Postgres returns
	// RepeatableRead; SQLite returns Default (the IMMEDIATE BEGIN handles it).
	SnapshotIsolation() IsoLevel

	// QueueDequeueIsolation returns the IsoLevel for the queue dequeue
	// transaction. snapshot=true requests snapshot semantics
	// snapshot=false allows the lighter read-committed path.
	QueueDequeueIsolation(snapshot bool) IsoLevel

	// SupportsListenNotify reports whether the dialect supports
	// LISTEN/NOTIFY. False for CockroachDB and SQLite, which both fall back
	// to the polling-based notification loop.
	SupportsListenNotify() bool

	// SupportsArrayParameters reports whether the dialect can bind a Go slice
	// as a single array-typed parameter (e.g. pg's `= ANY($1)` with $1=[]string).
	// SQLite cannot — callers must expand the slice into multiple positional
	// binds and use `IN (?, ?, ...)`.
	SupportsArrayParameters() bool

	// SupportsDataModifyingCTE reports whether a CTE term may be an
	// INSERT/UPDATE/DELETE (pg yes, sqlite no).
	SupportsDataModifyingCTE() bool

	// SupportsAttributesContainment reports whether the dialect can filter
	// workflows by JSONB attribute containment (@>). SQLite's JSON functions
	// cannot faithfully reproduce it, so the filter is rejected there.
	SupportsAttributesContainment() bool

	// IsUniqueViolation reports whether err represents a unique-constraint
	// violation surfaced from the driver.
	IsUniqueViolation(err error) bool

	// IsForeignKeyViolation reports whether err represents a foreign-key
	// violation surfaced from the driver.
	IsForeignKeyViolation(err error) bool

	// IsRetryable reports whether err is a transient driver error that the
	// retry helper should re-attempt. The logger is optional; implementations
	// may emit a debug/warning describing why the retry was triggered. The
	// signature matches retryConfig.retryConditionChain so a method value can
	// be inserted into the chain directly.
	IsRetryable(err error, logger *slog.Logger) bool

	// IsRetryableTransaction reports whether err is a transaction-level conflict
	// (serialization failure / deadlock / write-lock contention) that must be
	// retried by restarting the ENTIRE transaction with a fresh tx. This is
	// distinct from IsRetryable (transient connection-level errors) and is opted
	// in per call site via withRetryCondition. Same signature as IsRetryable so a
	// method value drops into the retry condition chain directly.
	IsRetryableTransaction(err error, logger *slog.Logger) bool
}

// detectDialect identifies the backend from a DBOS database URL by parsing
// the scheme.
//
// Postgres and CockroachDB are wire-compatible and share the postgres:
// scheme — neither pgx nor CRDB itself recognises a cockroach:/cockroachdb:
// scheme. The CRDB-vs-PG split is decided at runtime by querying the server
// (see isCockroachDB).
//
// Recognised schemes:
//
//	sqlite:, sqlite3:        → DialectSQLite
//	postgres:, postgresql:   → DialectPostgres (CRDB detected at runtime)
//
// If a user wants to pass modernc's URI-mode flags they can embed them
// inside a sqlite: URL, e.g. "sqlite:file:/path/x.db?_pragma=foreign_keys(1)";
// sqliteDSN trims the outer "sqlite:" and hands the rest to the driver.
func detectDialect(rawURL string) (DialectName, error) {
	if rawURL == "" {
		return "", fmt.Errorf("database URL is empty")
	}
	// libpq key=value DSNs (e.g. "user='User Name#$%&!' host=localhost ...")
	// can contain characters that url.Parse rejects as invalid URL escapes.
	// Detect this form before falling through to url.Parse so DSNs with funny
	// characters in quoted values still route to Postgres.
	if looksLikePostgresKVDSN(rawURL) {
		return DialectPostgres, nil
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("invalid database URL: %v", err)
	}
	switch strings.ToLower(u.Scheme) {
	case "sqlite", "sqlite3":
		return DialectSQLite, nil
	case "postgres", "postgresql":
		return DialectPostgres, nil
	case "":
		return "", fmt.Errorf("database URL has no scheme: %q", rawURL)
	default:
		return "", fmt.Errorf("unsupported database scheme %q (want sqlite: or postgres:)", u.Scheme)
	}
}

// looksLikePostgresKVDSN matches the libpq key=value connection-string form by
// checking for a leading canonical keyword. It is intentionally permissive: we
// only need to distinguish kv-DSN from outright garbage so we can route it to
// Postgres; the actual parse still happens in pgxpool.ParseConfig.
func looksLikePostgresKVDSN(s string) bool {
	lower := strings.ToLower(strings.TrimSpace(s))
	for _, prefix := range []string{
		"user=", "host=", "hostaddr=", "port=", "dbname=", "database=",
		"password=", "sslmode=", "application_name=", "options=",
	} {
		if strings.HasPrefix(lower, prefix) {
			return true
		}
	}
	return false
}

/* ---------------------------------------------------------------------------
   Postgres
   ------------------------------------------------------------------------- */

type postgresDialect struct{}

func (postgresDialect) Name() DialectName { return DialectPostgres }
func (postgresDialect) SchemaPrefix(schema string) string {
	return pgx.Identifier{schema}.Sanitize() + "."
}
func (postgresDialect) RewriteQuery(q string) string { return q }
func (postgresDialect) LockSkipLocked() string       { return "FOR UPDATE SKIP LOCKED" }
func (postgresDialect) LockNoWait() string           { return "FOR UPDATE NOWAIT" }
func (postgresDialect) SnapshotIsolation() IsoLevel  { return IsoLevelRepeatableRead }
func (postgresDialect) QueueDequeueIsolation(snapshot bool) IsoLevel {
	if snapshot {
		return IsoLevelRepeatableRead
	}
	return IsoLevelReadCommitted
}
func (postgresDialect) SupportsListenNotify() bool          { return true }
func (postgresDialect) SupportsArrayParameters() bool       { return true }
func (postgresDialect) SupportsDataModifyingCTE() bool      { return true }
func (postgresDialect) SupportsAttributesContainment() bool { return true }

// pgErrCode extracts the SQLSTATE code from a pgconn.PgError, or "" if err is
// not a pg error.
func pgErrCode(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

func (postgresDialect) IsUniqueViolation(err error) bool {
	return pgErrCode(err) == pgerrcode.UniqueViolation
}
func (postgresDialect) IsForeignKeyViolation(err error) bool {
	return pgErrCode(err) == pgerrcode.ForeignKeyViolation
}

// IsRetryable matches transient pg/CRDB driver errors that can be safely
// retried: closed transaction handles, connection-level SQLSTATEs, pgx
// connect failures, EOF/closed-conn strings, and net.Error. Serialization /
// deadlock SQLSTATEs are intentionally excluded — those require retrying the
// entire transaction and are opted in per call site via IsRetryableTransaction.
func (postgresDialect) IsRetryable(err error, logger *slog.Logger) bool {
	if err == nil {
		return false
	}
	// pgx surfaces ErrTxClosed for ops on a tx that has already finalized
	// the caller must retry with a fresh tx.
	if errors.Is(err, pgx.ErrTxClosed) {
		if logger != nil {
			logger.Warn("Transaction is closed, retrying requires a new transaction object", "error", err)
		}
		return true
	}
	var pgerr *pgconn.PgError
	if errors.As(err, &pgerr) {
		switch pgerr.Code {
		case pgerrcode.ConnectionException,
			pgerrcode.ConnectionDoesNotExist,
			pgerrcode.ConnectionFailure,
			pgerrcode.SQLClientUnableToEstablishSQLConnection,
			pgerrcode.SQLServerRejectedEstablishmentOfSQLConnection,
			pgerrcode.AdminShutdown,
			pgerrcode.CrashShutdown,
			pgerrcode.CannotConnectNow:
			return true
		}
	}
	var cerr *pgconn.ConnectError
	if errors.As(err, &cerr) {
		return true
	}
	if errors.Is(err, io.EOF) || strings.Contains(err.Error(), "conn closed") {
		return true
	}
	var nerr net.Error
	return errors.As(err, &nerr)
}

// IsRetryableTransaction matches pg/CRDB transaction-level conflicts that
// require restarting the whole transaction with a fresh tx: 40001
// serialization_failure (MVCC conflict) and 40P01 deadlock_detected.
func (postgresDialect) IsRetryableTransaction(err error, logger *slog.Logger) bool {
	switch pgErrCode(err) {
	case pgerrcode.SerializationFailure, pgerrcode.DeadlockDetected:
		if logger != nil {
			logger.Warn("Retrying transaction-level conflict, requires a new transaction object", "error", err)
		}
		return true
	}
	return false
}

/* ---------------------------------------------------------------------------
   CockroachDB

   Wire-compatible with Postgres but:
     - no LISTEN/NOTIFY (poll-based notification loop)
     - migration 10 needs application-layer handling (see system_database.go)
   ------------------------------------------------------------------------- */

type cockroachDialect struct{ postgresDialect }

func (cockroachDialect) Name() DialectName          { return DialectCockroach }
func (cockroachDialect) SupportsListenNotify() bool { return false }

/* ---------------------------------------------------------------------------
   SQLite

   - "?" placeholders
   - no schemas (SchemaPrefix is "")
   - no row-level locking (SQLite serialises writers via IMMEDIATE BEGIN)
   - no LISTEN/NOTIFY (poll-based notification loop)
   - error classification by message substring (driver does not expose codes)
   ------------------------------------------------------------------------- */

type sqliteDialect struct{}

func (sqliteDialect) Name() DialectName            { return DialectSQLite }
func (sqliteDialect) SchemaPrefix(_ string) string { return "" }

// sqlitePlaceholderRe matches $N-style Postgres placeholders so they can be
// rewritten to sqlite's ?N form. Numbering is preserved so queries that
// reference the same argument multiple times (e.g. $26 in two CASE branches)
// still resolve to the same arg without the caller duplicating it.
//
// Caveat: the regex is not string-literal aware. A hard-coded SQL literal that
// embeds a $N-shaped substring (e.g. `WHERE note = 'see $1 for help'`) will be
// rewritten incorrectly. This is safe in practice because all canonical
// queries live in system_database.go and are vetted to avoid this pattern.
var sqlitePlaceholderRe = regexp.MustCompile(`\$(\d+)`)

func (sqliteDialect) RewriteQuery(q string) string {
	return sqlitePlaceholderRe.ReplaceAllString(q, "?$1")
}

func (sqliteDialect) LockSkipLocked() string                { return "" }
func (sqliteDialect) LockNoWait() string                    { return "" }
func (sqliteDialect) SnapshotIsolation() IsoLevel           { return IsoLevelDefault }
func (sqliteDialect) QueueDequeueIsolation(_ bool) IsoLevel { return IsoLevelDefault }
func (sqliteDialect) SupportsListenNotify() bool            { return false }
func (sqliteDialect) SupportsArrayParameters() bool         { return false }
func (sqliteDialect) SupportsDataModifyingCTE() bool        { return false }
func (sqliteDialect) SupportsAttributesContainment() bool   { return false }

// Classify sqlite errors via modernc's typed *sqlite.Error and the extended
// result code constants in modernc.org/sqlite/lib. The Code() return is the
// extended code (high byte = subcode, low byte = primary code); for retryable
// classification we mask down to the primary code so all SQLITE_BUSY_* /
// SQLITE_LOCKED_* variants match.

// sqliteCode extracts the extended result code from err, or -1 if err is not
// a modernc *sqlite.Error.
func sqliteCode(err error) int {
	var se *sqlitelib.Error
	if errors.As(err, &se) {
		return se.Code()
	}
	return -1
}

func (sqliteDialect) IsUniqueViolation(err error) bool {
	switch sqliteCode(err) {
	case sqlite3.SQLITE_CONSTRAINT_UNIQUE, sqlite3.SQLITE_CONSTRAINT_PRIMARYKEY:
		return true
	}
	return false
}

func (sqliteDialect) IsForeignKeyViolation(err error) bool {
	return sqliteCode(err) == sqlite3.SQLITE_CONSTRAINT_FOREIGNKEY
}

// IsRetryable matches SQLITE_BUSY / SQLITE_LOCKED and their extended variants
// (BUSY_RECOVERY, BUSY_SNAPSHOT, BUSY_TIMEOUT, LOCKED_SHAREDCACHE, ...) — all
// of which share the same primary result code in the low byte.
func (sqliteDialect) IsRetryable(err error, logger *slog.Logger) bool {
	code := sqliteCode(err)
	if code < 0 {
		return false
	}
	switch code & 0xFF {
	case sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED:
		if logger != nil {
			logger.Warn("Retrying transient sqlite error", "error", err)
		}
		return true
	}
	return false
}

// IsRetryableTransaction is identical to IsRetryable for SQLite: a busy/locked
// writer (SQLITE_BUSY / SQLITE_LOCKED) is both the transient and the
// transaction-conflict condition, and both are resolved the same way — retry
// with a fresh tx. The two methods stay distinct on the Dialect interface
// (pg/CRDB separate connection-level errors from serialization/deadlock
// conflicts) but collapse to the same check here.
func (d sqliteDialect) IsRetryableTransaction(err error, logger *slog.Logger) bool {
	return d.IsRetryable(err, logger)
}

/* ---------------------------------------------------------------------------
   helpers
   ------------------------------------------------------------------------- */

func dialectAnyClause(d Dialect, column string, placeholderIdx int) string {
	if d.SupportsArrayParameters() {
		return fmt.Sprintf("%s = ANY($%d)", column, placeholderIdx)
	}
	return fmt.Sprintf("%s IN (SELECT value FROM json_each($%d))", column, placeholderIdx)
}

func dialectLikeAnyClause(d Dialect, column string, placeholderIdx int) string {
	if d.SupportsArrayParameters() {
		return fmt.Sprintf("%s LIKE ANY($%d)", column, placeholderIdx)
	}
	return fmt.Sprintf("EXISTS (SELECT 1 FROM json_each($%d) WHERE %s LIKE value)", placeholderIdx, column)
}

// dialectNoLimitClause returns the LIMIT clause that must precede an OFFSET when
// no row limit was requested. SQLite rejects a bare OFFSET and uses -1 as its
// "no limit" sentinel; Postgres accepts a bare OFFSET and so needs nothing.
func dialectNoLimitClause(d Dialect) string {
	if d.Name() == DialectSQLite {
		return " LIMIT -1"
	}
	return ""
}

func encodeArrayParam(d Dialect, slice any) (any, error) {
	if d.SupportsArrayParameters() {
		return slice, nil
	}
	b, err := json.Marshal(slice)
	if err != nil {
		return nil, fmt.Errorf("encode array param: %w", err)
	}
	return string(b), nil
}
