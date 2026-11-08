package pgsql

import (
	"context"
	"database/sql"

	"github.com/k3s-io/kine/pkg/server"
)

// initializingBackend commits schema and compact-revision initialization on the
// connection holding the advisory lock, before starting the ordinary backend.
// Losing that connection aborts the mutations as well as releasing the lock.
type initializingBackend struct {
	server.Backend
	db  *sql.DB
	key int64
}

func (b *initializingBackend) Start(ctx context.Context) error {
	if err := withSchemaLock(ctx, b.db, b.key, func(tx *sql.Tx) error {
		if err := setup(ctx, tx); err != nil {
			return err
		}
		return initializeCompactRevision(ctx, tx)
	}); err != nil {
		return err
	}
	// compactStart now observes exactly one committed compact revision, so it
	// does not need to insert or repair it on other connections. Health-key
	// creation is independently idempotent via the unique (name, prev_revision)
	// index, and does not require the schema lock. Runtime gap filling is unchanged.
	return b.Backend.Start(ctx)
}

func withSchemaLock(ctx context.Context, db *sql.DB, key int64, initialize func(*sql.Tx) error) error {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock($1)", key); err != nil {
		return err
	}
	if err := initialize(tx); err != nil {
		return err
	}
	// Do not retry on an ambiguous commit. The caller owns startup retry policy.
	return tx.Commit()
}

// Pre-initialize the compact revision on the lock transaction. The ordinary
// SQLLog startup path cannot safely repair it while using a separate connection.
// Preserve the most advanced compact revision when repairing historical duplicates.
func initializeCompactRevision(ctx context.Context, tx *sql.Tx) error {
	rows, err := tx.QueryContext(ctx, `SELECT id FROM kine WHERE name = 'compact_rev_key'
		ORDER BY prev_revision DESC NULLS LAST, id DESC FOR UPDATE`)
	if err != nil {
		return err
	}
	var keep int64
	count := 0
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		if count == 0 {
			keep = id
		}
		count++
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	if count == 0 {
		_, err = tx.ExecContext(ctx, `INSERT INTO kine
			(name, created, deleted, create_revision, prev_revision, lease, value, old_value)
			VALUES ('compact_rev_key', 1, 0, NULL, 0, 0, $1, NULL)`, []byte{})
	} else if count > 1 {
		_, err = tx.ExecContext(ctx, `DELETE FROM kine WHERE name = 'compact_rev_key' AND id <> $1`, keep)
	}
	return err
}
