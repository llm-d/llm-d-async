package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoad(t *testing.T) {
	migrations, err := load()
	require.NoError(t, err)
	require.NotEmpty(t, migrations, "no migrations embedded")
	for i, m := range migrations {
		assert.Equal(t, i+1, m.version)
		assert.NotEmpty(t, strings.TrimSpace(m.sql), "migration %04d_%s is empty", m.version, m.name)
	}
}

func latestVersion(t *testing.T) int {
	t.Helper()
	migrations, err := load()
	require.NoError(t, err)
	return len(migrations)
}

func TestFileNamePattern(t *testing.T) {
	for _, tt := range []struct {
		name  string
		file  string
		match bool
	}{
		{name: "valid", file: "0001_initial_schema.sql", match: true},
		{name: "missing padding", file: "1_initial.sql", match: false},
		{name: "uppercase", file: "0002_AddColumn.sql", match: false},
		{name: "wrong extension", file: "0002_add_column.txt", match: false},
		{name: "no name", file: "0002_.sql", match: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.match, fileNamePattern.MatchString(tt.file))
		})
	}
}

// The tests below run against a real Postgres given by TEST_POSTGRES_URL.
// Each test works in its own schema so runs are isolated and leave nothing behind.

func isolatedURL(t *testing.T) string {
	t.Helper()
	pgURL := os.Getenv("TEST_POSTGRES_URL")
	if pgURL == "" {
		t.Skip("TEST_POSTGRES_URL not set")
	}
	admin, err := sql.Open("pgx", pgURL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = admin.Close() })
	schema := fmt.Sprintf("migrate_test_%d", time.Now().UnixNano())
	_, err = admin.Exec("CREATE SCHEMA " + schema)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = admin.Exec("DROP SCHEMA " + schema + " CASCADE") })

	u, err := url.Parse(pgURL)
	require.NoError(t, err)
	query := u.Query()
	query.Set("search_path", schema)
	u.RawQuery = query.Encode()
	return u.String()
}

func newTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", isolatedURL(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func appliedVersions(t *testing.T, db *sql.DB) []int {
	t.Helper()
	rows, err := db.Query("SELECT version FROM " + versionTable + " ORDER BY version")
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	var versions []int
	for rows.Next() {
		var v int
		require.NoError(t, rows.Scan(&v))
		versions = append(versions, v)
	}
	require.NoError(t, rows.Err())
	return versions
}

type logLines struct {
	mu    sync.Mutex
	lines []string
}

func (l *logLines) get() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.lines...)
}

func captureLogs(ctx context.Context) (context.Context, *logLines) {
	l := &logLines{}
	logger := funcr.New(func(_, args string) {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.lines = append(l.lines, args)
	}, funcr.Options{})
	return logr.NewContext(ctx, logger), l
}

func tableExists(t *testing.T, db *sql.DB, table string) bool {
	t.Helper()
	var exists bool
	require.NoError(t, db.QueryRow("SELECT to_regclass($1) IS NOT NULL", table).Scan(&exists))
	return exists
}

func TestRun(t *testing.T) {
	ctx := context.Background()

	t.Run("fresh database", func(t *testing.T) {
		db := newTestDB(t)
		require.ErrorIs(t, Check(ctx, db), ErrSchemaMissing)

		logCtx, logs := captureLogs(ctx)
		require.NoError(t, Run(logCtx, db))
		latest := latestVersion(t)
		assert.Len(t, appliedVersions(t, db), latest)
		require.Len(t, logs.get(), latest)
		assert.Contains(t, logs.get()[0], `"msg"="applied sql transport schema migration" "version"=1 "name"="initial_schema"`)
		for _, table := range []string{
			"async_requests", "async_partitions", "async_dispatchers", "async_results",
			"async_quota_keys", "async_quota_holders", "async_quota_slots", "async_quota_windows", "async_quota_admits",
		} {
			assert.True(t, tableExists(t, db, table), "table %s missing after migrate", table)
		}
		require.NoError(t, Check(ctx, db))
	})

	t.Run("second run is a no-op", func(t *testing.T) {
		db := newTestDB(t)
		require.NoError(t, Run(ctx, db))
		before := appliedVersions(t, db)

		logCtx, logs := captureLogs(ctx)
		require.NoError(t, Run(logCtx, db))
		assert.Empty(t, logs.get())
		assert.Equal(t, before, appliedVersions(t, db))
	})

	t.Run("a current schema takes no lock", func(t *testing.T) {
		db := newTestDB(t)
		require.NoError(t, Run(ctx, db))
		holder := holdLock(t, db)

		quick, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		require.NoError(t, Run(quick, db))
		require.NoError(t, Check(quick, db))
		require.NoError(t, holder.Rollback())
	})

	t.Run("a newer database is left alone", func(t *testing.T) {
		db := newTestDB(t)
		require.NoError(t, Run(ctx, db))
		_, err := db.Exec(insertVersion, 9999, "from_the_future")
		require.NoError(t, err)
		before := appliedVersions(t, db)

		logCtx, logs := captureLogs(ctx)
		require.NoError(t, Run(logCtx, db))
		assert.Equal(t, before, appliedVersions(t, db))
		require.Len(t, logs.get(), 1)
		assert.Contains(t, logs.get()[0], `"databaseVersion"=9999`)
	})

	t.Run("a failed migration rolls back completely", func(t *testing.T) {
		db := newTestDB(t)
		_, err := db.Exec(`CREATE TABLE async_schema (version TEXT NOT NULL); CREATE TABLE async_quota_admits (key TEXT NOT NULL)`)
		require.NoError(t, err)

		require.ErrorContains(t, Run(ctx, db), "apply migration 0001_initial_schema")
		require.ErrorIs(t, Check(ctx, db), ErrSchemaMissing)
		assert.True(t, tableExists(t, db, "async_schema"), "the legacy version table survives")
		assert.False(t, tableExists(t, db, "async_requests"), "tables created before the failure are rolled back")
	})

	t.Run("adopts a schema created before migrations existed", func(t *testing.T) {
		db := newTestDB(t)
		migrations, err := load()
		require.NoError(t, err)
		_, err = db.Exec(`CREATE TABLE async_schema (version TEXT NOT NULL); INSERT INTO async_schema VALUES ('legacy')`)
		require.NoError(t, err)
		_, err = db.Exec(migrations[0].sql)
		require.NoError(t, err)
		_, err = db.Exec(`INSERT INTO async_quota_keys (key) VALUES ('kept')`)
		require.NoError(t, err)
		require.ErrorIs(t, Check(ctx, db), ErrSchemaMissing)

		require.NoError(t, Run(ctx, db))
		assert.Equal(t, 1, appliedVersions(t, db)[0])
		require.NoError(t, Check(ctx, db))
		assert.False(t, tableExists(t, db, "async_schema"), "the legacy version table is dropped")
		var kept int
		require.NoError(t, db.QueryRow(`SELECT count(*) FROM async_quota_keys WHERE key = 'kept'`).Scan(&kept))
		assert.Equal(t, 1, kept, "adopting the schema keeps its rows")
	})

	t.Run("concurrent runs apply each migration once", func(t *testing.T) {
		db := newTestDB(t)
		const runners = 8

		logCtx, logs := captureLogs(ctx)
		var wg sync.WaitGroup
		errs := make(chan error, runners)
		for range runners {
			wg.Go(func() { errs <- Run(logCtx, db) })
		}
		wg.Wait()
		close(errs)

		for err := range errs {
			assert.NoError(t, err, "concurrent Run")
		}
		latest := latestVersion(t)
		assert.Len(t, logs.get(), latest, "each migration is applied by one runner")
		assert.Len(t, appliedVersions(t, db), latest)
	})
}

func TestCheck(t *testing.T) {
	ctx := context.Background()

	t.Run("behind", func(t *testing.T) {
		db := newTestDB(t)
		require.NoError(t, Run(ctx, db))
		_, err := db.Exec("DELETE FROM " + versionTable)
		require.NoError(t, err)
		require.ErrorIs(t, Check(ctx, db), ErrSchemaBehind)
	})

	t.Run("ahead is accepted", func(t *testing.T) {
		db := newTestDB(t)
		require.NoError(t, Run(ctx, db))
		_, err := db.Exec(insertVersion, 9999, "from_the_future")
		require.NoError(t, err)
		require.NoError(t, Check(ctx, db))
	})

	t.Run("waits for a migration in progress", func(t *testing.T) {
		db := newTestDB(t)
		migrations, err := load()
		require.NoError(t, err)
		migrating := holdLock(t, db)
		_, err = migrating.Exec(createVersionTable)
		require.NoError(t, err)
		_, err = migrating.Exec(migrations[0].sql)
		require.NoError(t, err)
		for _, m := range migrations {
			_, err = migrating.Exec(insertVersion, m.version, m.name)
			require.NoError(t, err)
		}

		checked := make(chan error, 1)
		go func() { checked <- Check(ctx, db) }()
		select {
		case err := <-checked:
			t.Fatalf("Check returned %v before the migration committed", err)
		case <-time.After(200 * time.Millisecond):
		}
		require.NoError(t, migrating.Commit())
		select {
		case err := <-checked:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("Check did not return after the migration committed")
		}
	})
}

// holdLock takes the migration lock in a transaction the test ends.
func holdLock(t *testing.T, db *sql.DB) *sql.Tx {
	t.Helper()
	tx, err := db.Begin()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	_, err = tx.Exec("SELECT pg_advisory_xact_lock($1)", lockKey)
	require.NoError(t, err)
	return tx
}
