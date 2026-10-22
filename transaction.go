package pgxrepmgr

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"log/slog"
)

// InTransaction acquires a connection from pool, and calls `proc` within a transaction on it.
// If `proc` returns an error, then it rolls back the transaction, otherwise commits.
// It returns the error returned by `proc`, or the error of the commit, or an error of its own.
func InTransaction(ctx context.Context, pool *pgxpool.Pool, proc func(*pgxpool.Conn, pgx.Tx) error) error {
	// Connect to primary db
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	// Setup database transaction
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	// Roll back if proc fails or panics. After a commit, Rollback does nothing and returns ErrTxClosed.
	defer func() {
		if err := tx.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
			slog.Default().Error(err.Error())
		}
	}()
	if err = proc(conn, tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
