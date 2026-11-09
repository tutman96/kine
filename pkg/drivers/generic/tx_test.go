package generic

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/k3s-io/kine/pkg/query"
	_ "modernc.org/sqlite"
)

func transactionDatabase(t *testing.T) *Generic {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	db.SetMaxOpenConns(1)
	_, err = db.Exec(`CREATE TABLE kine (
		id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, created INTEGER, deleted INTEGER,
		create_revision INTEGER, prev_revision INTEGER, lease INTEGER, value BLOB, old_value BLOB)`)
	if err != nil {
		t.Fatal(err)
	}
	return &Generic{
		DB: db, ErrCode: func(error) string { return "" },
		InsertSQL:          query.New(insertSQL, "?", false, "Insert"),
		InsertReturningSQL: query.New(insertSQL+" RETURNING id", "?", false, "InsertReturning"),
		CurrentRevSQL:      query.New("SELECT COALESCE(MAX(id), 0) FROM kine", "?", false, "CurrentRevision"),
		CompactRevSQL:      query.New("SELECT COALESCE(MAX(prev_revision), 0) FROM kine", "?", false, "CompactRevision"),
		UpdateCompactSQL:   query.New("UPDATE kine SET prev_revision = ?", "?", false, "SetCompactRevision"),
		DeleteSQL:          query.New("DELETE FROM kine WHERE id = ?", "?", false, "DeleteRevision"),
		CompactSQL:         query.New("DELETE FROM kine WHERE id > ? AND id <= ? AND id > ? AND id <= ?", "?", false, "Compact"),
	}
}

// A nested pool checkout cannot complete while this transaction holds the only
// connection. Exercise both insert strategies, and verify real rollback semantics.
func TestTransactionInsertUsesHeldConnection(t *testing.T) {
	for _, lastInsertID := range []bool{false, true} {
		name := "returning"
		if lastInsertID {
			name = "last-insert-id"
		}
		t.Run(name, func(t *testing.T) {
			d := transactionDatabase(t)
			d.LastInsertID = lastInsertID
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			for _, commit := range []bool{false, true} {
				tx, err := d.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { tx.Rollback() })
				id, err := tx.Insert(ctx, "/test", true, false, 0, 0, []byte("value"))
				if err != nil || id == 0 {
					t.Fatalf("insert id=%d, err=%v", id, err)
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
				if err := d.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM kine").Scan(&count); err != nil {
					t.Fatal(err)
				}
				if (commit && count != 1) || (!commit && count != 0) {
					t.Fatalf("commit=%v left %d rows", commit, count)
				}
			}
			if waits := d.DB.Stats().WaitCount; waits != 0 {
				t.Fatalf("transaction required %d nested pool checkouts", waits)
			}
		})
	}
}

func TestTransactionRevisionMethodsUseHeldConnection(t *testing.T) {
	d := transactionDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	tx, err := d.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	id, err := tx.Insert(ctx, "/test", true, false, 0, 0, []byte("value"))
	if err != nil {
		t.Fatal(err)
	}
	if rev, err := tx.CurrentRevision(ctx); err != nil || rev != id {
		t.Fatalf("current revision=%d, err=%v", rev, err)
	}
	if err := tx.SetCompactRevision(ctx, id); err != nil {
		t.Fatal(err)
	}
	if rev, err := tx.GetCompactRevision(ctx); err != nil || rev != id {
		t.Fatalf("compact revision=%d, err=%v", rev, err)
	}
	if err := tx.DeleteRevision(ctx, id+1); err != nil {
		t.Fatal(err)
	}
	if rows, err := tx.Compact(ctx, 0, id); err != nil || rows != 1 {
		t.Fatalf("compacted rows=%d, err=%v", rows, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if waits := d.DB.Stats().WaitCount; waits != 0 {
		t.Fatalf("transaction required %d nested pool checkouts", waits)
	}
}
