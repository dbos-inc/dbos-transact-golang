// Package models holds the shared, dependency-free data types of the DBOS
// runtime: workflow statuses, schedules, queue configuration, list/filter
// inputs and their functional options, and the Error type with its
// error codes.
//
// It is the leaf package of the module — it may not import any other dbos
// package. Types here are re-exported from the public dbos package via type
// aliases (see dbos/aliases.go), so they are part of the public API surface;
// changing a field or constant here is a public API change.
package models
