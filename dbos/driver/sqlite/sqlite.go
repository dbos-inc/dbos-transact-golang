// Package sqlite registers SQLite support for DBOS, backed by the pure-Go
// modernc.org/sqlite driver.
//
// It is imported for its side effects only, like a database/sql driver:
//
//	import _ "github.com/dbos-inc/dbos-transact-golang/dbos/driver/sqlite"
//
// With this import in place, sqlite: database URLs (and the SQLiteSystemDB
// config field) work everywhere in dbos. Without it, SQLite is not compiled
// into the binary — PostgreSQL-only applications need nothing but the dbos
// package and stay free of the SQLite dependency.
package sqlite

import (
	"errors"

	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/sysdb"
	sqlitelib "modernc.org/sqlite"
)

func init() {
	sysdb.RegisterSQLiteDriver(sysdb.SQLiteDriver{
		// modernc.org/sqlite registers itself with database/sql as "sqlite".
		DriverName: "sqlite",
		ErrorCode: func(err error) int {
			var se *sqlitelib.Error
			if errors.As(err, &se) {
				return se.Code()
			}
			return -1
		},
	})
}
