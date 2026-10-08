package sysdb

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Keyed by workflow_uuid with no foreign key on workflow_status.
var payloadTables = []string{"workflow_input", "workflow_output", "operation_outputs"}

// AdvisoryLockKey derives a pg advisory lock key from a name.
func AdvisoryLockKey(name string) int64 {
	sum := sha256.Sum256([]byte(name))
	return int64(binary.BigEndian.Uint64(sum[:8])) // #nosec G115 -- the sign flip is the point: the key is a signed bigint
}

// Server notices, routed per connection. forwardNotice is every pool
// connection's OnNotice hook; VacuumTables registers a sink for its connection.
var noticeSinks sync.Map

func forwardNotice(conn *pgconn.PgConn, notice *pgconn.Notice) {
	if sink, ok := noticeSinks.Load(conn); ok {
		sink.(func(*pgconn.Notice))(notice)
	}
}

// VacuumTables vacuums and analyzes the named system tables on Postgres. It is
// a no-op on other engines, and failures and refusals are logged, not returned.
func (s *SysDB) VacuumTables(ctx context.Context, tables []string) {
	pool := PgxPool(s.pool)
	if pool == nil || s.isCockroachDB {
		return
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		s.logger.Warn("Could not acquire a connection to vacuum", "error", err)
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
			s.logger.Warn("Could not vacuum table", "table", table, "error", err)
			continue
		}
		for _, notice := range notices {
			s.logger.Warn("Vacuuming table", "table", table, "notice", notice)
		}
	}
}
