package dbos

import (
	"errors"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/sysdb"

	"github.com/stretchr/testify/require"
)

func TestApplicationVersions(t *testing.T) {
	t.Run("LaunchRegistersCurrentVersion", func(t *testing.T) {
		dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true})
		require.NoError(t, dbosCtx.Launch())

		latest, err := GetLatestApplicationVersion(dbosCtx)
		require.NoError(t, err)
		require.NotNil(t, latest)
		require.Equal(t, dbosCtx.GetApplicationVersion(), latest.Name)

		versions, err := ListApplicationVersions(dbosCtx)
		require.NoError(t, err)
		require.Len(t, versions, 1)
		require.Equal(t, latest.Name, versions[0].Name)
		require.Equal(t, latest.ID, versions[0].ID)
	})

	t.Run("CreateIsIdempotent", func(t *testing.T) {
		dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true})
		require.NoError(t, dbosCtx.Launch())

		c := dbosCtx.(*dbosContext)
		// Re-registering the same version must not create a duplicate row.
		require.NoError(t, c.systemDB.CreateApplicationVersion(c, c.applicationVersion))
		require.NoError(t, c.systemDB.CreateApplicationVersion(c, c.applicationVersion))

		versions, err := ListApplicationVersions(dbosCtx)
		require.NoError(t, err)
		require.Len(t, versions, 1)
	})

	t.Run("SetLatestUpdatesTimestamp", func(t *testing.T) {
		dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true})
		require.NoError(t, dbosCtx.Launch())

		c := dbosCtx.(*dbosContext)
		// Insert an older version directly so it sorts before "current".
		require.NoError(t, c.systemDB.CreateApplicationVersion(c, "older-version"))
		require.NoError(t, c.systemDB.UpdateApplicationVersionTimestamp(c, "older-version", time.Now().Add(-time.Hour).UnixMilli()))

		latest, err := GetLatestApplicationVersion(dbosCtx)
		require.NoError(t, err)
		require.Equal(t, dbosCtx.GetApplicationVersion(), latest.Name)

		// Promoting older-version should make it the new latest.
		require.NoError(t, SetLatestApplicationVersion(dbosCtx, "older-version"))

		latest, err = GetLatestApplicationVersion(dbosCtx)
		require.NoError(t, err)
		require.Equal(t, "older-version", latest.Name)

		versions, err := ListApplicationVersions(dbosCtx)
		require.NoError(t, err)
		require.Len(t, versions, 2)
		require.Equal(t, "older-version", versions[0].Name)
	})

	t.Run("GetLatestReturnsErrWhenEmpty", func(t *testing.T) {
		dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true})
		// Launch registers the current version; clear the table to simulate empty state.
		require.NoError(t, dbosCtx.Launch())
		c := dbosCtx.(*dbosContext)
		s := c.systemDB.(*sysdb.SysDB)
		_, err := s.Pool().Exec(c, s.RenderSQL("DELETE FROM %sapplication_versions", s.Dialect().SchemaPrefix(s.Schema())))
		require.NoError(t, err)

		_, err = GetLatestApplicationVersion(dbosCtx)
		require.Error(t, err)
		var dbosErr *DBOSError
		require.True(t, errors.As(err, &dbosErr), "expected *DBOSError, got %T: %v", err, err)
		require.Equal(t, NoApplicationVersions, dbosErr.Code)
	})

	t.Run("SetLatestRequiresVersionName", func(t *testing.T) {
		dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true})
		require.NoError(t, dbosCtx.Launch())

		err := SetLatestApplicationVersion(dbosCtx, "")
		require.Error(t, err)
	})
}
