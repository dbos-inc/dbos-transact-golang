// Package sqlite registers SQLite support for DBOS, backed by the pure-Go
// modernc.org/sqlite driver.
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
