// Package adminserver implements the DBOS admin HTTP server: health checks,
// workflow listing/cancel/resume/fork, queue metadata, recovery, and
// deactivation endpoints used by the DBOS console and tooling.
//
// It depends on the DBOS runtime only through the Executor interface, which
// the dbos package implements with a thin adapter (dbos/executor_adapter.go).
// The interface is the complete inventory of runtime operations the admin
// server may perform.
package adminserver
