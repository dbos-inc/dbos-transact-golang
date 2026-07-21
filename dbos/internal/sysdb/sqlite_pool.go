package sysdb

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// sqlite_pool.go: SQLite-specific setup
//
//   - URL parsing: turn "sqlite:///path/to.db" or "sqlite::memory:" into the
//     DSN modernc expects.
//   - Connection bootstrap: ensure parent directory exists; apply PRAGMAs
//     (foreign_keys, journal_mode=WAL, synchronous=NORMAL, busy_timeout) so
//     write-heavy workloads behave well.

// SqliteDSN extracts the modernc-compatible DSN from a DBOS-style sqlite URL.
//
// Examples (input → DSN):
//
//	sqlite:/tmp/x.db                                  → /tmp/x.db
//	sqlite:///tmp/x.db                                → /tmp/x.db   (triple-slash, SQLAlchemy-style)
//	sqlite::memory:                                   → :memory:
//	sqlite:relative.db                                → relative.db
//	sqlite3:relative.db                               → relative.db
//	sqlite:file:/abs/x.db?_pragma=foreign_keys(1)     → file:/abs/x.db?_pragma=foreign_keys(1)
//
// SQLite has no host concept, so a URL with a non-empty authority (e.g.
// sqlite://server/path) is rejected.
func SqliteDSN(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("invalid sqlite URL %q: %v", raw, err)
	}
	switch strings.ToLower(u.Scheme) {
	case "sqlite", "sqlite3":
	default:
		return "", fmt.Errorf("not a sqlite URL: %q", raw)
	}
	if u.Host != "" {
		return "", fmt.Errorf("sqlite URL must not have a host component: %q", raw)
	}

	dsn := u.Opaque
	if dsn == "" {
		dsn = u.Path
	}
	if dsn == "" {
		return "", fmt.Errorf("sqlite URL has no path: %q", raw)
	}
	if u.RawQuery != "" {
		dsn += "?" + u.RawQuery
	}
	if u.Fragment != "" {
		dsn += "#" + u.Fragment
	}
	return dsn, nil
}

// OpenSQLitePool opens a *sql.DB backed by the registered SQLite driver,
// ensures the parent directory of file-backed databases exists, applies
// recommended PRAGMAs, and returns the pool. The caller owns Close. It
// errors if no SQLite driver is registered (see RegisterSQLiteDriver).
func OpenSQLitePool(ctx context.Context, databaseURL string) (*sql.DB, error) {
	driver, err := registeredSQLiteDriver()
	if err != nil {
		return nil, err
	}
	dsn, err := SqliteDSN(databaseURL)
	if err != nil {
		return nil, err
	}

	// Ensure the parent directory exists for file-backed databases.
	if dsn != ":memory:" && !strings.HasPrefix(dsn, ":memory:") && !strings.HasPrefix(dsn, "file::memory:") {
		path := dsn
		if strings.HasPrefix(path, "file:") {
			// Extract the file path from a file:URI.
			if u, err := url.Parse(path); err == nil {
				path = u.Opaque
				if path == "" {
					path = u.Path
				}
			}
		}
		// Skip directory creation if the path is empty or only a name.
		if dir := filepath.Dir(path); dir != "" && dir != "." {
			if err := os.MkdirAll(dir, 0o750); err != nil {
				return nil, fmt.Errorf("failed to create sqlite parent dir %q: %v", dir, err)
			}
		}
	}

	// Immediately begin a transaction on every connection.
	// More conservative but we can always make this configurable.
	// (also the current API supports passing a custom *sql.DB)
	dsn = withSqliteTxlockImmediate(dsn)
	// Apply per-connection pragmas as DSN params so every connection in the
	// pool inherits them
	dsn = withSqlitePragma(dsn, "busy_timeout(5000)")
	dsn = withSqlitePragma(dsn, "foreign_keys(ON)")
	dsn = withSqlitePragma(dsn, "synchronous(NORMAL)")

	db, err := sql.Open(driver.DriverName, dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite database %q: %v", dsn, err)
	}

	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(4)
	db.SetConnMaxLifetime(time.Hour)

	if err := applySQLitePragmas(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

// When customDB is non-nil it is used as-is (the caller
// owns its lifecycle and PRAGMA configuration); otherwise the function opens
// a fresh pool from databaseURL and applies default PRAGMAs.
// Either way it runs SQLite migrations and returns a sysDB.
func newSqliteSystemDatabase(encodeScheduledInput func(context.Context, time.Time, json.RawMessage) (*string, string, error),

	ctx context.Context,
	databaseURL, databaseSchema string,
	customDB *sql.DB,
	logger *slog.Logger,
) (SystemDatabase, error) {
	// The driver is required even with a custom handle: error classification
	// (retryable/contention/constraint) needs its ErrorCode extractor.
	if _, err := registeredSQLiteDriver(); err != nil {
		return nil, err
	}
	var (
		db    *sql.DB
		owned bool
	)
	if customDB != nil {
		logger.Info("Using custom SQLite system database handle")
		db = customDB
	} else {
		logger.Info("Connecting to SQLite system database", "database_url", databaseURL)
		opened, err := OpenSQLitePool(ctx, databaseURL)
		if err != nil {
			return nil, err
		}
		db = opened
		owned = true
	}
	closeIfOwned := func() {
		if !owned {
			return
		}
		if err := db.Close(); err != nil {
			logger.Warn("Failed to close sqlite database during cleanup", "error", err)
		}
	}
	if err := Retry(ctx, func() error {
		return RunSqliteMigrations(ctx, db, logger)
	}, WithRetrierLogger(logger)); err != nil {
		closeIfOwned()
		return nil, fmt.Errorf("failed to run sqlite migrations: %v", err)
	}
	if err := db.PingContext(ctx); err != nil {
		closeIfOwned()
		return nil, fmt.Errorf("failed to ping sqlite database: %v", err)
	}
	return &SysDB{
		pool:                 NewSQLPool(db),
		dialect:              SqliteDialect{},
		RecvNotifier:         newNotifyRegistry(),
		EventNotifier:        newNotifyRegistry(),
		streamNotifier:       newNotifyRegistry(),
		notificationLoopDone: make(chan struct{}),
		logger:               logger.With("service", "system_database"),
		schema:               databaseSchema,
		encodeScheduledInput: encodeScheduledInput,
	}, nil
}

func withSqliteTxlockImmediate(dsn string) string {
	if strings.Contains(dsn, "_txlock=") {
		return dsn
	}
	return appendDSNParam(dsn, "_txlock=immediate")
}

// withSqlitePragma appends `_pragma=<pragma>` to the dsn so modernc.org/sqlite
// applies it at every new connection in the pool (database/sql has no
// connection-init hook). pragma is the body, e.g. `busy_timeout(5000)`. If a
// pragma with the same name is already present in the DSN it's left alone.
func withSqlitePragma(dsn, pragma string) string {
	name := pragma
	if before, _, ok := strings.Cut(pragma, "("); ok {
		name = before
	}
	if strings.Contains(dsn, "_pragma="+name+"(") {
		return dsn
	}
	return appendDSNParam(dsn, "_pragma="+pragma)
}

func appendDSNParam(dsn, param string) string {
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	if hash := strings.Index(dsn, "#"); hash >= 0 {
		return dsn[:hash] + sep + param + dsn[hash:]
	}
	return dsn + sep + param
}

// applySQLitePragmas configures the connection for write-heavy use:
//
//   - foreign_keys = ON   (FK constraints are declared in our schema)
//   - journal_mode = WAL  (concurrent reads + faster writes)
//   - synchronous = NORMAL (safe with WAL; ~3-5x faster than FULL)
//   - busy_timeout = 5000 (block writers for 5s on lock conflicts)
//
// PRAGMAs apply per-connection in SQLite. The pool may have multiple
// connections open, so we cycle a few times and let database/sql apply them
// to each retrieved connection. The WAL setting is database-wide and sticks.
func applySQLitePragmas(ctx context.Context, db *sql.DB) error {
	pragmas := []string{
		`PRAGMA foreign_keys = ON`,
		`PRAGMA journal_mode = WAL`,
		`PRAGMA synchronous = NORMAL`,
		`PRAGMA busy_timeout = 5000`,
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire sqlite connection: %v", err)
	}
	defer conn.Close()
	for _, p := range pragmas {
		if _, err := conn.ExecContext(ctx, p); err != nil {
			return fmt.Errorf("failed to apply %q: %v", p, err)
		}
	}
	return nil
}
