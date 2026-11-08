package generic

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/k3s-io/kine/pkg/query"
)

type prepareFailureConnector struct{}
type prepareFailureConn struct{}

func (prepareFailureConnector) Connect(context.Context) (driver.Conn, error) {
	return prepareFailureConn{}, nil
}
func (prepareFailureConnector) Driver() driver.Driver { return nil }
func (prepareFailureConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare failed")
}
func (prepareFailureConn) Close() error              { return nil }
func (prepareFailureConn) Begin() (driver.Tx, error) { return nil, errors.New("not implemented") }

func TestFailedPrepareReleasesConnection(t *testing.T) {
	db := sql.OpenDB(prepareFailureConnector{})
	defer db.Close()
	db.SetMaxOpenConns(1)
	d := &Generic{DB: db}
	for range 3 {
		if _, err := d.prepare(context.Background(), query.New("SELECT 1", "$", true, "test")); err == nil {
			t.Fatal("expected prepare failure")
		}
		if got := db.Stats().InUse; got != 0 {
			t.Fatalf("leaked %d checked-out connections", got)
		}
	}
}
