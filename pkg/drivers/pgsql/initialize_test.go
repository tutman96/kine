package pgsql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k3s-io/kine/pkg/server"
)

// A transaction-aware fake tests lock lifetime and release without PostgreSQL.
// Real advisory-lock support/concurrent DDL requires the opt-in integration suite.
type lockConnector struct {
	gate       chan struct{}
	commits    atomic.Int32
	rollbacks  atomic.Int32
	failLock   bool
	allowSetup bool
	setupCalls atomic.Int32
}
type lockConn struct {
	c    *lockConnector
	held bool
}
type lockTx struct{ conn *lockConn }

func (c *lockConnector) Connect(context.Context) (driver.Conn, error) { return &lockConn{c: c}, nil }
func (c *lockConnector) Driver() driver.Driver                        { return nil }
func (c *lockConn) Prepare(string) (driver.Stmt, error)               { return nil, errors.New("unexpected prepare") }
func (c *lockConn) Close() error                                      { return nil }
func (c *lockConn) Begin() (driver.Tx, error)                         { return &lockTx{c}, nil }
func (c *lockConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if c.c.allowSetup && query != "SELECT pg_advisory_xact_lock($1)" {
		if len(c.c.gate) != 1 {
			return nil, errors.New("schema setup ran outside lock")
		}
		c.c.setupCalls.Add(1)
		return driver.RowsAffected(1), nil
	}
	if query != "SELECT pg_advisory_xact_lock($1)" || len(args) != 1 || args[0].Value != int64(123) {
		return nil, errors.New("unexpected lock SQL or key")
	}
	if c.c.failLock {
		return nil, errors.New("lock unsupported")
	}
	select {
	case c.c.gate <- struct{}{}:
		c.held = true
		return driver.RowsAffected(1), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func (t *lockTx) release() {
	if t.conn.held {
		<-t.conn.c.gate
		t.conn.held = false
	}
}
func (t *lockTx) Commit() error   { t.conn.c.commits.Add(1); t.release(); return nil }
func (t *lockTx) Rollback() error { t.conn.c.rollbacks.Add(1); t.release(); return nil }

func TestSchemaLockRelease(t *testing.T) {
	for _, mode := range []string{"success", "initialize failure", "lock failure"} {
		t.Run(mode, func(t *testing.T) {
			c := &lockConnector{gate: make(chan struct{}, 1), failLock: mode == "lock failure"}
			db := sql.OpenDB(c)
			defer db.Close()
			called := false
			err := withSchemaLock(context.Background(), db, 123, func() error {
				called = true
				if len(c.gate) != 1 {
					t.Fatal("initializer ran outside lock")
				}
				if mode == "initialize failure" {
					return errors.New("interrupted initializer")
				}
				return nil
			})
			if (err == nil) != (mode == "success") || called != (mode != "lock failure") {
				t.Fatalf("called=%v, err=%v", called, err)
			}
			if len(c.gate) != 0 {
				t.Fatal("lock retained after startup")
			}
			if mode == "success" && c.commits.Load() != 1 {
				t.Fatal("startup did not commit lock transaction")
			}
			if mode != "success" && c.rollbacks.Load() != 1 {
				t.Fatal("failed startup did not roll back lock transaction")
			}
		})
	}
}

func TestSchemaLockSerializesInitializersAndRetries(t *testing.T) {
	c := &lockConnector{gate: make(chan struct{}, 1)}
	db := sql.OpenDB(c)
	defer db.Close()
	db.SetMaxOpenConns(4)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var active, completed atomic.Int32
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := withSchemaLock(ctx, db, 123, func() error {
				if active.Add(1) != 1 {
					t.Error("concurrent initializers")
				}
				defer active.Add(-1)
				completed.Add(1)
				return nil
			}); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if completed.Load() != 8 {
		t.Fatal("not all initializers completed")
	}
	// An interrupted attempt must not prevent a later initializer from running.
	_ = withSchemaLock(ctx, db, 123, func() error { return errors.New("interrupted") })
	if err := withSchemaLock(ctx, db, 123, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if db.Stats().InUse != 0 {
		t.Fatal("lock connection leaked")
	}
}

func TestSchemaLockWaitIsCancelable(t *testing.T) {
	c := &lockConnector{gate: make(chan struct{}, 1)}
	c.gate <- struct{}{}
	db := sql.OpenDB(c)
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err := withSchemaLock(ctx, db, 123, func() error { t.Error("initializer ran without acquiring lock"); return nil })
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("lock wait error = %v", err)
	}
}

type startupBackend struct {
	server.Backend
	start func(context.Context) error
}

func (b startupBackend) Start(ctx context.Context) error { return b.start(ctx) }

func TestInitializingBackendHoldsLockThroughStart(t *testing.T) {
	for _, failStart := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "backend failure"}[failStart], func(t *testing.T) {
			c := &lockConnector{gate: make(chan struct{}, 1), allowSetup: true}
			db := sql.OpenDB(c)
			defer db.Close()
			db.SetMaxOpenConns(2)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			startErr := errors.New("backend startup failed")
			called := false
			b := &initializingBackend{db: db, key: 123, Backend: startupBackend{start: func(got context.Context) error {
				called = true
				if got != ctx || len(c.gate) != 1 || c.setupCalls.Load() == 0 {
					t.Error("backend startup must follow schema setup while holding the lock")
				}
				if failStart {
					return startErr
				}
				return nil
			}}}
			err := b.Start(ctx)
			if !called || (failStart && !errors.Is(err, startErr)) || (!failStart && err != nil) {
				t.Fatalf("called=%v, err=%v", called, err)
			}
			if len(c.gate) != 0 || db.Stats().InUse != 0 {
				t.Fatal("startup retained the lock or its connection")
			}
			if (!failStart && c.commits.Load() != 1) || (failStart && c.rollbacks.Load() != 1) {
				t.Fatal("lock transaction did not finish with backend startup")
			}
		})
	}
}
