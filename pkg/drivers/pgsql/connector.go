package pgsql

import (
	"context"
	"database/sql/driver"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

var errConnect = errors.New("PostgreSQL connection failed")

// Bound credential-bearing authentication/callback errors at the connector,
// before they can reach generic retries, SQL error logs, metrics or clients.
// Preserve cancellation and database/sql's bad-connection control signal.
type redactedConnector struct{ driver.Connector }

func newConnector(config *pgx.ConnConfig, options ...stdlib.OptionOpenDB) driver.Connector {
	return redactedConnector{stdlib.GetConnector(*config, options...)}
}

func (c redactedConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err == nil {
		return conn, nil
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if errors.Is(err, context.Canceled) {
		return nil, context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return nil, context.DeadlineExceeded
	}
	if errors.Is(err, driver.ErrBadConn) {
		return nil, driver.ErrBadConn
	}
	return nil, errConnect
}
