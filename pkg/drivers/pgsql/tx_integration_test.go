package pgsql

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	"github.com/k3s-io/kine/pkg/drivers/generic"
)

func TestPostgreSQLTransactionInsertSingleConnection(t *testing.T) {
	t.Setenv("KINE_SCHEMA_MIGRATION", "2")
	db, config := integrationDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	var wg sync.WaitGroup
	defer func() { cancel(); wg.Wait() }()
	if err := setup(ctx, db); err != nil {
		t.Fatal(err)
	}
	d, err := generic.OpenDB(ctx, &wg, "pgx", newConnector(config), generic.ConnectionPoolConfig{MaxOpen: 1}, "$", true, nil)
	if err != nil {
		t.Fatal(err)
	}
	d.ErrCode = func(error) string { return "" }
	for _, commit := range []bool{false, true} {
		tx, err := d.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { tx.Rollback() })
		// Bound nested-checkout regressions without cancelling the transaction's
		// lifetime context, so cleanup remains straightforward on failure.
		insertCtx, insertCancel := context.WithTimeout(ctx, time.Second)
		id, err := tx.Insert(insertCtx, "/transaction-test", true, false, 0, 0, []byte("value"))
		insertCancel()
		if err != nil || id == 0 {
			tx.Rollback()
			t.Fatalf("insert with one connection: id=%d, err=%v", id, err)
		}
		if rev, err := tx.CurrentRevision(ctx); err != nil || rev != id {
			t.Fatalf("transaction cannot see its insert: rev=%d, err=%v", rev, err)
		}
		if commit {
			err = tx.Commit()
		} else {
			err = tx.Rollback()
		}
		if err != nil {
			t.Fatal(err)
		}
		var count int
		if err := d.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM kine WHERE name = '/transaction-test'").Scan(&count); err != nil {
			t.Fatal(err)
		}
		if (commit && count != 1) || (!commit && count != 0) {
			t.Fatalf("commit=%v left %d records", commit, count)
		}
	}
	if waits := d.DB.Stats().WaitCount; waits != 0 {
		t.Fatalf("transaction required %d nested pool checkouts", waits)
	}
}
