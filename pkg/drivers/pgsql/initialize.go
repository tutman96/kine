package pgsql

import (
	"context"
	"database/sql"

	"github.com/k3s-io/kine/pkg/server"
)

// initializingBackend keeps all startup mutations under one transaction-scoped
// advisory lock. DDL and Backend.Start use the ordinary pool, not the lock
// transaction: backend startup must see committed DDL on its own connections.
// An interrupted initializer can leave partial DDL; upstream IF NOT EXISTS and
// repeatable migrations allow the next lock holder to finish it.
type initializingBackend struct {
	server.Backend
	db  *sql.DB
	key int64
}

func (b *initializingBackend) Start(ctx context.Context) error {
	return withSchemaLock(ctx, b.db, b.key, func() error {
		if err := setup(ctx, b.db); err != nil {
			return err
		}
		return b.Backend.Start(ctx)
	})
}

func withSchemaLock(ctx context.Context, db *sql.DB, key int64, initialize func() error) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock($1)", key); err != nil {
		return err
	}
	if err := initialize(); err != nil {
		return err
	}
	// Do not retry on an ambiguous commit. The caller owns startup retry policy.
	return tx.Commit()
}
