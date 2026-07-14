// Package sysdb implements the DBOS system database: schema migrations,
// workflow/step state persistence, queues, schedules, notifications, streams,
// and application-version bookkeeping.
//
// SystemDatabase is the interface the dbos executor programs against; sysDB
// implements it for Postgres, CockroachDB (pgx), and SQLite (database/sql).
// Driver differences are isolated behind two seams that are also re-exported
// publicly for future driver registration:
//
//   - Pool/Tx/Querier (dbq.go): a thin driver-agnostic SQL surface.
//   - Dialect (dialect.go): per-backend query fragments and error
//     classification.
//
// The package must not know about workflow execution or serialization.
// Where the DB layer needs to serialize values (schedule backfill/trigger
// inputs), the encoder is injected via NewSystemDatabaseInput.
// It may import only internal/models.
package sysdb
