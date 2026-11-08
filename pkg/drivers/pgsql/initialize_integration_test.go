package pgsql

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/drivers/generic"
	"github.com/k3s-io/kine/pkg/server"
)

// Use only a disposable PostgreSQL instance. Each test owns an isolated schema;
// interruption tests terminate only the backend PID obtained from their own tx.
func integrationDatabase(t *testing.T) (*sql.DB, *pgx.ConnConfig) {
	t.Helper()
	dsn := os.Getenv("KINE_PG_TEST_DSN")
	if dsn == "" {
		t.Skip("set KINE_PG_TEST_DSN to a disposable PostgreSQL instance")
	}
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	admin := sql.OpenDB(newConnector(config))
	schema := "kine_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	quoted := pgx.Identifier{schema}.Sanitize()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := admin.ExecContext(ctx, "CREATE SCHEMA "+quoted); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		defer admin.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := admin.ExecContext(ctx, "DROP SCHEMA "+quoted+" CASCADE"); err != nil {
			t.Error(err)
		}
	})
	config = config.Copy()
	config.RuntimeParams["search_path"] = schema
	db := sql.OpenDB(newConnector(config))
	t.Cleanup(func() { db.Close() })
	return db, config
}

func TestPostgreSQLInitializationConnectionLoss(t *testing.T) {
	t.Setenv("KINE_SCHEMA_MIGRATION", "2")
	db, _ := integrationDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	const key int64 = 928374123
	pidReady := make(chan int, 1)
	resume := make(chan struct{})
	var release sync.Once
	defer release.Do(func() { close(resume) })
	first := make(chan error, 1)
	go func() {
		first <- withSchemaLock(ctx, db, key, func(tx *sql.Tx) error {
			if err := setup(ctx, tx); err != nil {
				return err
			}
			if err := initializeCompactRevision(ctx, tx); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, "CREATE TABLE interrupted_marker (id integer)"); err != nil {
				return err
			}
			var pid int
			if err := tx.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&pid); err != nil {
				return err
			}
			pidReady <- pid
			select {
			case <-resume:
			case <-ctx.Done():
				return ctx.Err()
			}
			// This must fail on the killed transaction, never use another pool conn.
			_, err := tx.ExecContext(ctx, "CREATE TABLE stale_marker (id integer)")
			return err
		})
	}()
	var pid int
	select {
	case pid = <-pidReady:
	case err := <-first:
		t.Fatalf("initializer failed before interruption: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	second := make(chan error, 1)
	go func() {
		second <- withSchemaLock(ctx, db, key, func(tx *sql.Tx) error {
			if err := setup(ctx, tx); err != nil {
				return err
			}
			return initializeCompactRevision(ctx, tx)
		})
	}()
	// Observe the second initializer actually waiting, rather than depending on
	// a scheduler delay to claim concurrent initialization coverage.
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		var waiting bool
		if err := db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM pg_locks
			WHERE locktype = 'advisory' AND classid = 0 AND objid = $1 AND NOT granted)`, key).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case <-tick.C:
		case err := <-second:
			t.Fatalf("initializer did not wait for the lock: %v", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	var terminated bool
	if err := db.QueryRowContext(ctx, "SELECT pg_terminate_backend($1)", pid).Scan(&terminated); err != nil || !terminated {
		t.Fatalf("could not terminate test backend: terminated=%v, err=%v", terminated, err)
	}
	// The replacement must finish even while the stale Go initializer remains
	// paused. Its schema and compact row must not conflict with partial old DDL.
	select {
	case err := <-second:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	release.Do(func() { close(resume) })
	select {
	case err := <-first:
		if err == nil {
			t.Fatal("interrupted initializer reported success")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	var clean bool
	if err := db.QueryRowContext(ctx, `SELECT to_regclass('interrupted_marker') IS NULL
		AND to_regclass('stale_marker') IS NULL
		AND (SELECT count(*) FROM kine WHERE name = 'compact_rev_key') = 1`).Scan(&clean); err != nil || !clean {
		t.Fatalf("interrupted initialization leaked DDL or compact rows: clean=%v, err=%v", clean, err)
	}
}

func TestPostgreSQLConcurrentBackendInitialization(t *testing.T) {
	t.Setenv("KINE_SCHEMA_MIGRATION", "2")
	db, config := integrationDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	var backends sync.WaitGroup
	var workers sync.WaitGroup
	defer func() { cancel(); workers.Wait(); backends.Wait() }()
	const key int64 = 928374124
	// Reject any compact-revision startup mutation made on an unlocked pool
	// connection. The first initializer starts with no compact-revision row.
	if err := withSchemaLock(ctx, db, key, func(tx *sql.Tx) error {
		if err := setup(ctx, tx); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `CREATE FUNCTION guard_compact_start() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
		  IF NEW.name = 'compact_rev_key' AND NOT EXISTS
		    (SELECT 1 FROM pg_locks WHERE pid = pg_backend_pid() AND locktype = 'advisory' AND granted)
		  THEN RAISE EXCEPTION 'compact startup mutation outside advisory lock'; END IF;
		  RETURN NEW;
		END $$;
		CREATE TRIGGER guard_compact_start BEFORE INSERT OR UPDATE ON kine
		FOR EACH ROW EXECUTE FUNCTION guard_compact_start()`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 3)
	for range 3 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			_, backend, err := New(ctx, &backends, &drivers.Config{
				PostgreSQL:           &drivers.PostgreSQLConfig{ConnConfig: config, SkipCreateDatabase: true, SchemaLockKey: ptrInt64(key)},
				ConnectionPoolConfig: generic.ConnectionPoolConfig{MaxOpen: 4},
				CompactBatchSize:     100, CompactInterval: time.Hour, CompactTimeout: time.Second,
				PollBatchSize: 100, PollInterval: time.Millisecond,
			})
			if err == nil {
				err = backend.Start(ctx)
			}
			results <- err
		}()
	}
	for range 3 {
		select {
		case err := <-results:
			if err != nil {
				t.Error(err)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	for _, key := range []string{"compact_rev_key", server.HealthKey} {
		var count int
		if err := db.QueryRowContext(ctx, "SELECT count(*) FROM kine WHERE name = $1", key).Scan(&count); err != nil || count != 1 {
			t.Fatalf("startup key %q: count=%d, err=%v", key, count, err)
		}
	}
}

func ptrInt64(n int64) *int64 { return &n }

func TestPostgreSQLRepairsCompactRevisionInTransaction(t *testing.T) {
	t.Setenv("KINE_SCHEMA_MIGRATION", "2")
	db, _ := integrationDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := withSchemaLock(ctx, db, 928374125, func(tx *sql.Tx) error {
		if err := setup(ctx, tx); err != nil {
			return err
		}
		for _, revision := range []int{0, 7, 11} {
			if _, err := tx.ExecContext(ctx, `INSERT INTO kine (name, created, deleted, prev_revision)
				VALUES ('compact_rev_key', 1, 0, $1)`, revision); err != nil {
				return err
			}
		}
		return initializeCompactRevision(ctx, tx)
	}); err != nil {
		t.Fatal(err)
	}
	var count, revision int
	if err := db.QueryRowContext(ctx, "SELECT count(*), max(prev_revision) FROM kine WHERE name = 'compact_rev_key'").Scan(&count, &revision); err != nil {
		t.Fatal(err)
	}
	if count != 1 || revision != 11 {
		t.Fatalf("compact revision repair: count=%d, revision=%d", count, revision)
	}
}
