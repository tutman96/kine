package sqllog

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/drivers/generic"
	"github.com/k3s-io/kine/pkg/query"
	"github.com/k3s-io/kine/pkg/server"
)

type pollConnector struct {
	mode     string
	prepares atomic.Int32
	queries  atomic.Int32
	closed   atomic.Int32
}
type pollConn struct{ c *pollConnector }
type pollStmt struct{ c *pollConnector }
type pollRows struct{ fail, sent bool }

func (c *pollConnector) Connect(context.Context) (driver.Conn, error) { return &pollConn{c}, nil }
func (c *pollConnector) Driver() driver.Driver                        { return nil }
func (c *pollConn) Close() error                                      { return nil }
func (c *pollConn) Begin() (driver.Tx, error)                         { return nil, errors.New("not implemented") }
func (c *pollConn) Prepare(string) (driver.Stmt, error) {
	c.c.prepares.Add(1)
	return &pollStmt{c.c}, nil
}
func (s *pollStmt) Close() error  { s.c.closed.Add(1); return nil }
func (s *pollStmt) NumInput() int { return 1 }
func (s *pollStmt) Exec([]driver.Value) (driver.Result, error) {
	return nil, errors.New("not implemented")
}
func (s *pollStmt) Query(args []driver.Value) (driver.Rows, error) {
	first := s.c.queries.Add(1) == 1
	if args[0] != int64(41) {
		return nil, errors.New("poll advanced revision before publishing a complete batch")
	}
	if first && s.c.mode == "query" {
		return nil, errors.New("query disconnected")
	}
	return &pollRows{fail: first && s.c.mode == "rows"}, nil
}
func (r *pollRows) Columns() []string { return make([]string, 11) }
func (r *pollRows) Close() error      { return nil }
func (r *pollRows) Next(dest []driver.Value) error {
	if r.sent {
		if r.fail {
			return errors.New("stream disconnected after a partial batch")
		}
		return io.EOF
	}
	r.sent = true
	copy(dest, []driver.Value{int64(42), int64(0), int64(42), "/test", true, false, int64(42), int64(0), int64(0), []byte("value"), nil})
	return nil
}

func TestPollRepreparesAfterQueryAndRowsFailures(t *testing.T) {
	for _, mode := range []string{"query", "rows"} {
		t.Run(mode, func(t *testing.T) {
			connector := &pollConnector{mode: mode}
			db := sql.OpenDB(connector)
			defer db.Close()
			db.SetMaxOpenConns(1)
			d := &generic.Generic{DB: db, AfterAllOldValSQL: query.New("SELECT fake", "$", true, "test"), ErrCode: func(error) string { return "" }}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			wg := &sync.WaitGroup{}
			log := New(d, wg, &drivers.Config{PollInterval: time.Millisecond, PollBatchSize: 100})
			log.ctx = ctx
			result := make(chan server.EventBatch)
			wg.Add(1)
			go log.poll(result, 41)
			select {
			case batch := <-result:
				if len(batch.Events) != 1 || batch.CurrentRev != 42 {
					t.Fatalf("bad recovered batch: %+v", batch)
				}
				if connector.prepares.Load() < 2 || connector.closed.Load() < 1 {
					t.Fatal("failed statement was not closed and reprepared")
				}
			case <-ctx.Done():
				t.Fatal("poll did not recover")
			}
			cancel()
			wg.Wait()
			if db.Stats().InUse != 0 {
				t.Fatal("poll retained a connection after shutdown")
			}
		})
	}
}
