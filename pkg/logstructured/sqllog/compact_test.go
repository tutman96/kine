package sqllog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/k3s-io/kine/pkg/server"
	_ "modernc.org/sqlite"
)

type compactTransaction struct {
	server.Transaction
	failAt                      string
	failure, rollbackErr        error
	commits, rollbacks, deletes int
	done                        bool
}

func (t *compactTransaction) fail(stage string) error {
	if t.failAt == stage {
		return t.failure
	}
	return nil
}
func (t *compactTransaction) CurrentRevision(context.Context) (int64, error) {
	return 100, t.fail("current")
}
func (t *compactTransaction) GetCompactRevision(context.Context) (int64, error) {
	return 1, t.fail("revision")
}
func (t *compactTransaction) SetCompactRevision(context.Context, int64) error {
	return t.fail("set")
}
func (t *compactTransaction) Compact(context.Context, int64, int64) (int64, error) {
	return 5, t.fail("compact")
}
func (t *compactTransaction) DeleteRevision(context.Context, int64) error {
	t.deletes++
	return t.fail("delete")
}
func (t *compactTransaction) Commit() error {
	t.commits++
	t.done = true // Match database/sql, including failed Commit.
	return t.fail("commit")
}
func (t *compactTransaction) Rollback() error {
	t.rollbacks++
	if t.rollbackErr != nil {
		return t.rollbackErr
	}
	if t.done {
		return sql.ErrTxDone
	}
	t.done = true
	return nil
}
func (*compactTransaction) MustCommit()   { panic("compaction must not call fatal commit helper") }
func (*compactTransaction) MustRollback() { panic("compaction must not call fatal rollback helper") }

type compactDialect struct {
	server.Dialect
	tx                       *compactTransaction
	db                       *sql.DB
	beginErr, currentErr     error
	revisionErr              error
	postCompacts, txAttempts int
	retry                    bool
}

func (d *compactDialect) BeginTx(context.Context, *sql.TxOptions) (server.Transaction, error) {
	d.txAttempts++
	return d.tx, d.beginErr
}
func (d *compactDialect) CurrentRevision(context.Context) (int64, error) {
	return 100, d.currentErr
}
func (d *compactDialect) GetCompactRevision(context.Context) (int64, error) {
	return 1, d.revisionErr
}
func (d *compactDialect) IsTxRetry(error) bool {
	if d.retry {
		d.retry = false
		return true
	}
	return false
}
func (d *compactDialect) PostCompact(context.Context) error {
	d.postCompacts++
	return nil
}
func (d *compactDialect) After(ctx context.Context, _, _ string, _, _ int64) (*sql.Rows, error) {
	return d.db.QueryContext(ctx, `SELECT 100, 1, 2, 'compact_rev_key', 0, 0, 1, 1, 0, NULL, NULL
		UNION ALL SELECT 100, 1, 3, 'compact_rev_key', 0, 0, 1, 2, 0, NULL, NULL`)
}

func compactLog(d *compactDialect) *SQLLog {
	return &SQLLog{d: d, ctx: context.Background(), compactTimeout: time.Second, compactBatchSize: 100}
}

func TestCompactPropagatesTransactionErrors(t *testing.T) {
	for _, stage := range []string{"begin", "current", "revision", "set", "compact", "commit"} {
		for _, failRollback := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/rollback-error=%v", stage, failRollback), func(t *testing.T) {
				failure := errors.New("transaction failed")
				rollbackErr := errors.New("rollback failed")
				tx := &compactTransaction{failAt: stage, failure: failure}
				if failRollback {
					tx.rollbackErr = rollbackErr
				}
				d := &compactDialect{tx: tx}
				if stage == "begin" {
					d.beginErr = failure
				}
				rev, err := compactLog(d).Compact(context.Background(), 50)
				if rev != 0 || !errors.Is(err, failure) {
					t.Fatalf("rev=%d, err=%v; expected original failure", rev, err)
				}
				if failRollback && stage != "begin" && !errors.Is(err, rollbackErr) {
					t.Fatalf("lost rollback failure: %v", err)
				}
				wantRollbacks := 1
				if stage == "begin" {
					wantRollbacks = 0
				}
				if tx.rollbacks != wantRollbacks || d.postCompacts != 0 {
					t.Fatalf("rollbacks=%d, post-compacts=%d", tx.rollbacks, d.postCompacts)
				}
			})
		}
	}
}

func TestCompactSuccessfulCommitIgnoresErrTxDone(t *testing.T) {
	tx := &compactTransaction{}
	d := &compactDialect{tx: tx}
	rev, err := compactLog(d).Compact(context.Background(), 50)
	if err != nil || rev != 100 || tx.commits != 1 || tx.rollbacks != 1 || d.postCompacts != 1 {
		t.Fatalf("rev=%d, err=%v, tx=%+v, post-compacts=%d", rev, err, tx, d.postCompacts)
	}
}

func TestCompactRollbackFailureInvalidatesRevisions(t *testing.T) {
	failure := errors.New("rollback failed")
	tx := &compactTransaction{rollbackErr: failure}
	s := compactLog(&compactDialect{tx: tx})
	// This would normally return valid revisions with ErrCompacted.
	compacted, current, err := s.compact(2, 50)
	if compacted != 0 || current != 0 || !errors.Is(err, failure) || !errors.Is(err, server.ErrCompacted) {
		t.Fatalf("compacted=%d, current=%d, err=%v", compacted, current, err)
	}
}

func TestCompactRetryReceivesCommitFailure(t *testing.T) {
	failure := errors.New("retryable commit failure")
	tx := &compactTransaction{failAt: "commit", failure: failure}
	d := &compactDialect{tx: tx, retry: true}
	compacted, current, err := compactLog(d).compactRetry(1, 50)
	if compacted != 0 || current != 0 || !errors.Is(err, failure) || d.txAttempts != 2 || tx.rollbacks != 2 {
		t.Fatalf("compacted=%d, current=%d, err=%v, attempts=%d, rollbacks=%d", compacted, current, err, d.txAttempts, tx.rollbacks)
	}
}

func TestCompactPropagatesRevisionReadErrors(t *testing.T) {
	failure := errors.New("revision query failed")
	for _, current := range []bool{false, true} {
		d := &compactDialect{}
		if current {
			d.currentErr = failure
		} else {
			d.revisionErr = failure
		}
		if _, err := compactLog(d).Compact(context.Background(), 50); !errors.Is(err, failure) {
			t.Fatalf("lost revision query failure: %v", err)
		}
		if d.txAttempts != 0 {
			t.Fatal("started compaction despite failed revision query")
		}
	}
}

func TestCompactStartTransactionCleanup(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, stage := range []string{"success", "delete", "commit"} {
		for _, failRollback := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/rollback-error=%v", stage, failRollback), func(t *testing.T) {
				failure := errors.New("cleanup operation failed")
				rollbackErr := errors.New("cleanup rollback failed")
				tx := &compactTransaction{failAt: stage, failure: failure}
				if failRollback {
					tx.rollbackErr = rollbackErr
				}
				s := compactLog(&compactDialect{tx: tx, db: db})
				err := s.compactStart(context.Background())
				if stage != "success" && !errors.Is(err, failure) {
					t.Fatalf("lost operation failure: %v", err)
				}
				if failRollback && !errors.Is(err, rollbackErr) {
					t.Fatalf("lost rollback failure: %v", err)
				}
				if stage == "success" && !failRollback && err != nil {
					t.Fatal(err)
				}
				if tx.rollbacks != 1 || tx.deletes != 1 {
					t.Fatalf("rollbacks=%d, deletes=%d", tx.rollbacks, tx.deletes)
				}
			})
		}
	}
}
