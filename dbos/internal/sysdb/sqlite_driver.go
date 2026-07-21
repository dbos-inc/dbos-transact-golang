package sysdb

import (
	"fmt"
	"sync"
)

// sqlite_driver.go: database/sql-style SQLite driver registration.
//
// The heavyweight SQLite driver (modernc.org/sqlite) is not imported by this
// package. SQLite support is linked in only when the user blank-imports the
// public driver package, whose init() calls RegisterSQLiteDriver:
//
//	import _ "github.com/dbos-inc/dbos-transact-golang/dbos/driver/sqlite"
//
// PostgreSQL-only builds therefore never compile or link the SQLite driver.

// SQLiteDriver describes a registered SQLite driver implementation.
type SQLiteDriver struct {
	// DriverName is the name the driver registered with database/sql
	// (e.g. "sqlite" for modernc.org/sqlite).
	DriverName string
	// ErrorCode extracts the extended SQLite result code from a driver error,
	// or returns -1 if err does not originate from the driver.
	ErrorCode func(err error) int
}

var (
	sqliteDriverMu sync.RWMutex
	sqliteDriver   *SQLiteDriver
)

// RegisterSQLiteDriver makes a SQLite driver available to the system database.
// It is called from the init() of a driver package and panics on invalid input,
// mirroring database/sql.Register semantics.
func RegisterSQLiteDriver(d SQLiteDriver) {
	if d.DriverName == "" || d.ErrorCode == nil {
		panic("sysdb: RegisterSQLiteDriver requires a driver name and an ErrorCode function")
	}
	sqliteDriverMu.Lock()
	defer sqliteDriverMu.Unlock()
	sqliteDriver = &d
}

// registeredSQLiteDriver returns the registered driver, or an error telling
// the user which package to import.
func registeredSQLiteDriver() (*SQLiteDriver, error) {
	sqliteDriverMu.RLock()
	defer sqliteDriverMu.RUnlock()
	if sqliteDriver == nil {
		return nil, fmt.Errorf(`SQLite support is not linked into this binary: add import _ "github.com/dbos-inc/dbos-transact-golang/dbos/driver/sqlite" to register the SQLite driver`)
	}
	return sqliteDriver, nil
}
