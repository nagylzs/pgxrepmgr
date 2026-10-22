package pgxrepmgr

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestIsNodeError(t *testing.T) {
	_, connectErr := pgx.Connect(context.Background(), closedPort+" connect_timeout=1")
	if connectErr == nil {
		t.Fatal("connecting to a closed port succeeded")
	}
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"application error", errors.New("invalid input"), false},
		{"canceled by the caller", fmt.Errorf("query: %w", context.Canceled), false},
		{"timeout", fmt.Errorf("query: %w", context.DeadlineExceeded), true},
		{"cannot connect", connectErr, true},
		{"connection lost", fmt.Errorf("commit: %w", io.ErrUnexpectedEOF), true},
		{"read-only transaction", &pgconn.PgError{Code: "25006"}, true},
		{"server shutting down", fmt.Errorf("insert: %w", &pgconn.PgError{Code: "57P01"}), true},
		{"connection failure", &pgconn.PgError{Code: "08006"}, true},
		{"unique violation", &pgconn.PgError{Code: "23505"}, false},
		{"no primary", fmt.Errorf("insert: %w", ErrNoPrimary), true},
		{"no node", ErrNoNode, true},
		{"closed", ErrClosed, false},
	} {
		if got := IsNodeError(tc.err); got != tc.want {
			t.Errorf("%s: IsNodeError(%v) = %v, want %v", tc.name, tc.err, got, tc.want)
		}
	}
}
