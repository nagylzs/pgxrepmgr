package pgxrepmgr

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

// ErrClosed is returned by a Connector or a Pool after Close.
var ErrClosed = errors.New("pgxrepmgr: closed")

// ErrNoPrimary is the LastError of Connector.Primary while there is no primary, for example during a
// failover, or before the first check has finished.
var ErrNoPrimary = errors.New("pgxrepmgr: no primary is available")

// ErrNoNode is the LastError of Connector.Nearest while no node is available.
var ErrNoNode = errors.New("pgxrepmgr: no node is available")

// IsNodeError reports whether err suggests that a node is gone, or is no longer what the connector
// says it is: connecting failed, the connection was lost or timed out, the server is shutting down or
// starting up, or it refused a write because it is in recovery. ErrNoPrimary and ErrNoNode count too,
// because they last only until a check finds a node. After such an error, call Connector.Refresh
// before retrying, so that Primary and Nearest are up to date. Pool does that by itself.
func IsNodeError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, ErrNoPrimary) || errors.Is(err, ErrNoNode) {
		return true
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "25006", // read_only_sql_transaction: the node is in recovery
			"57P01", "57P02", "57P03": // admin_shutdown, crash_shutdown, cannot_connect_now
			return true
		}
		return strings.HasPrefix(pgErr.Code, "08") // connection_exception
	}
	var connectErr *pgconn.ConnectError
	var netErr net.Error
	return errors.As(err, &connectErr) || errors.As(err, &netErr) || pgconn.Timeout(err) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}
