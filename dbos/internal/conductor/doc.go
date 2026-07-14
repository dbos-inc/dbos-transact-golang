// Package conductor maintains the WebSocket connection to DBOS Conductor and
// services its requests: workflow queries and management, schedules, queues,
// metrics, export/import, and alerts.
//
// The exported protocol types in protocol.go are the wire contract with the
// Conductor service. The package depends on the DBOS runtime only through
// the Executor interface (implemented by dbos/executor_adapter.go) plus
// sysdb.SystemDatabase for direct read/admin queries; serialization of
// stored values is injected via Executor.DecodeStoredValue.
package conductor
