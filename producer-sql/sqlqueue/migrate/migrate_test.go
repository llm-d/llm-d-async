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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoad(t *testing.T) {
	migrations, err := Load()
	require.NoError(t, err)
	require.NotEmpty(t, migrations, "no migrations embedded")
	for i, m := range migrations {
		assert.Equal(t, i+1, m.Version)
		assert.NotEmpty(t, strings.TrimSpace(m.SQL), "migration %04d_%s is empty", m.Version, m.Name)
	}

	latest, err := LatestVersion()
	require.NoError(t, err)
	assert.Equal(t, len(migrations), latest)
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

		applied, err := Run(ctx, db)
		require.NoError(t, err)
		latest, err := LatestVersion()
		require.NoError(t, err)
		assert.Len(t, applied, latest)
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
		_, err := Run(ctx, db)
		require.NoError(t, err)
		before := appliedVersions(t, db)

		applied, err := Run(ctx, db)
		require.NoError(t, err)
		assert.Empty(t, applied)
		assert.Equal(t, before, appliedVersions(t, db))
	})

	t.Run("adopts a schema created before migrations existed", func(t *testing.T) {
		db := newTestDB(t)
		migrations, err := Load()
		require.NoError(t, err)
		_, err = db.Exec(`CREATE TABLE async_schema (version TEXT NOT NULL); INSERT INTO async_schema VALUES ('legacy')`)
		require.NoError(t, err)
		_, err = db.Exec(migrations[0].SQL)
		require.NoError(t, err)
		_, err = db.Exec(`INSERT INTO async_quota_keys (key) VALUES ('kept')`)
		require.NoError(t, err)
		require.ErrorIs(t, Check(ctx, db), ErrSchemaMissing)

		_, err = Run(ctx, db)
		require.NoError(t, err)
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

		var wg sync.WaitGroup
		results := make(chan int, runners)
		errs := make(chan error, runners)
		for range runners {
			wg.Add(1)
			go func() {
				defer wg.Done()
				applied, err := Run(ctx, db)
				if err != nil {
					errs <- err
					return
				}
				results <- len(applied)
			}()
		}
		wg.Wait()
		close(results)
		close(errs)

		for err := range errs {
			t.Errorf("concurrent Run: %v", err)
		}
		total := 0
		for n := range results {
			total += n
		}
		latest, err := LatestVersion()
		require.NoError(t, err)
		assert.Equal(t, latest, total)
		assert.Len(t, appliedVersions(t, db), latest)
	})
}

func TestCheck(t *testing.T) {
	ctx := context.Background()

	t.Run("behind", func(t *testing.T) {
		db := newTestDB(t)
		_, err := Run(ctx, db)
		require.NoError(t, err)
		_, err = db.Exec("DELETE FROM " + versionTable)
		require.NoError(t, err)
		require.ErrorIs(t, Check(ctx, db), ErrSchemaBehind)
	})

	t.Run("ahead is accepted", func(t *testing.T) {
		db := newTestDB(t)
		_, err := Run(ctx, db)
		require.NoError(t, err)
		_, err = db.Exec(insertVersion, 9999, "from_the_future")
		require.NoError(t, err)
		require.NoError(t, Check(ctx, db))
	})
}

func TestRunCommand(t *testing.T) {
	ctx := context.Background()

	t.Run("flag", func(t *testing.T) {
		u := isolatedURL(t)
		require.NoError(t, RunCommand(ctx, []string{"--postgresql-url", u}))
		db, err := sql.Open("pgx", u)
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		require.NoError(t, Check(ctx, db))
	})

	t.Run("environment", func(t *testing.T) {
		u := isolatedURL(t)
		t.Setenv(urlEnv, u)
		require.NoError(t, RunCommand(ctx, nil))
		db, err := sql.Open("pgx", u)
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		require.NoError(t, Check(ctx, db))
	})

	t.Run("no url", func(t *testing.T) {
		t.Setenv(urlEnv, "")
		err := RunCommand(ctx, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "--postgresql-url")
	})
}
