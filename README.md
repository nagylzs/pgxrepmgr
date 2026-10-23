# pgxrepmgr

pgxrepmgr is a Go library that discovers and monitors a [PostgreSQL repmgr](https://www.repmgr.org/) cluster
in the background. It tells your program which node is the current primary and which node is the nearest (the
one with the lowest connect latency, which may be a primary or a hot standby), and it gives you
[pgx](https://github.com/jackc/pgx) connection pools for them that follow failovers.

```sh
go get github.com/nagylzs/pgxrepmgr
```

Requirements: Go 1.25 or later, PostgreSQL 10 or later, and repmgr 4 or later. It is tested with PostgreSQL 17
and repmgr 5.5.

## How it works

* The connector periodically checks all known `repmgr` nodes in the background, until you call `Close()`. It
  starts a check every `CheckInterval` (defaults to 30 seconds). While there is no primary, for example during a
  failover, it starts one every `RetryInterval` (defaults to 1 second) instead, so it finds the new primary soon
  after it was promoted.
* When a check occurs, it connects to all known nodes (in parallel), and fetches data from `repmgr.nodes`.
  You only give it one or more seed connection strings, and it discovers the other nodes from `repmgr.nodes`.
  Nodes that the primary's `repmgr.nodes` no longer lists were removed from the cluster, so the connector stops
  checking them.
* Between checks, `Primary()` and `Nearest()` return what the last check found. When using a node fails in a
  way that suggests that the cluster has changed (`IsNodeError(err)` is true: the node can't be reached, it is
  shutting down, it refused a write because it is a standby now, or there is no primary at the moment), call
  `Refresh(ctx)`. It checks the cluster right away and waits for the result, so a retry goes to the right node.
  `Pool` does this by itself.
* A node is available (`NodeInfo.Avail`) only if the connector could connect to it and read `repmgr.nodes`
  from it, and that copy of `repmgr.nodes` has the node's own row with `active=true`.
* The primary is a node whose own row in `repmgr.nodes` says it is the primary, and which is not in recovery.
  After a failover, the old primary may still be running and still claim to be the primary (split brain), for
  example when it was cut off from the other nodes. A promotion always moves a node to a new, higher timeline, so
  the node on the highest timeline wins. The other nodes that claim to be primary get `Avail=false`, so they are
  not used for reading either, and their `LastError` explains why.
* The nearest node can be a standby that is behind the primary. Each check measures how far behind each node is,
  in bytes of WAL that it has yet to replay (`NodeInfo.LagBytes`). With `Options.WithMaxLagBytes(n)`, `Nearest()`
  skips the standbys that are more than `n` bytes behind, for example because their replication has stopped. The
  primary is never behind, so `Nearest()` falls back to it. How many seconds a number of bytes means depends on
  how much the primary writes, so choose `n` well above what your standbys are normally behind. The lag is as old
  as the last check, and while there is no primary it is unknown, so no node is skipped.

## How to use

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nagylzs/pgxrepmgr"
)

func main() {
	// The connector checks the nodes as the repmgr user, in the repmgr
	// database. Any user that can read repmgr.nodes will do.
	opts := pgxrepmgr.NewOptions("repmgr", "repmgr_password").
		// Check overrides are applied to the conninfo from repmgr.nodes
		// when the connector checks a node
		WithCheckOverride("application_name", "myapp-pgxrepmgr").
		// App overrides are applied to the conninfo that Primary,
		// Nearest and Nodes return, and Pool uses
		WithAppOverride("user", "app").
		WithAppOverride("password", "app password").
		WithAppOverride("dbname", "app").
		// Optional
		WithCheckInterval(10 * time.Second).
		// Optional, Nearest skips the standbys further behind
		WithMaxLagBytes(64 << 20)

	// One or more seeds. The connector discovers the other nodes from
	// their repmgr.nodes.
	rc := pgxrepmgr.NewConnector(opts, "host=pg1.example.com", "host=pg2.example.com")
	defer rc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	primary := rc.WaitForPrimary(ctx)
	if !primary.Avail {
		log.Fatal(primary.LastError)
	}
	fmt.Println("primary:", primary.NodeName)

	pool := pgxrepmgr.NewPool(rc, pgxrepmgr.DbPoolConfig{MaxConns: 10})
	defer pool.Close()

	// Writes go to the primary
	err := pool.InTransaction(ctx, func(conn *pgxpool.Conn, tx pgx.Tx) error {
		_, err := tx.Exec(ctx, "insert into events (name) values ($1)", "started")
		return err
	})
	if err != nil {
		log.Fatal(err)
	}

	// Reads can go to the nearest node, which may be a standby
	conn, err := pool.ConnectNearest(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Release()
	var count int
	if err := conn.QueryRow(ctx, "select count(*) from events").Scan(&count); err != nil {
		log.Fatal(err)
	}
	fmt.Println("events:", count)
}
```

`NewConnector` panics if the options are invalid (see below), if no seed is given, or if a seed is not a valid
connection string.

### Options

`NewOptions(user, password)` returns the defaults below, and sets the check overrides `user`, `password` and
`dbname=repmgr`. The `With` methods return a changed copy.

| Method                           | Default | Meaning                                                                                   |
|----------------------------------|---------|-------------------------------------------------------------------------------------------|
| `WithCheckInterval(d)`           | 30 s    | How often the cluster is checked. At least 1 s.                                           |
| `WithRetryInterval(d)`           | 1 s     | How often it is checked while there is no primary, and the least time between checks that `Refresh` starts. Between 100 ms and `CheckInterval`. |
| `WithConnectTimeout(d)`          | 5 s     | Timeout for connecting to a node when checking it. At least 100 ms.                       |
| `WithFetchTimeout(d)`            | 5 s     | Timeout for reading a node's state and `repmgr.nodes`. At least 100 ms.                   |
| `WithMaxLagBytes(n)`             | 0       | `Nearest()` skips the standbys more than `n` bytes of WAL behind the primary. 0: no limit. |
| `WithCheckOverride(key, value)`  |         | Sets `key` in the connection strings that the connector checks the nodes with.            |
| `WithAppOverride(key, value)`    |         | Sets `key` in the connection strings that `Primary()`, `Nearest()` and `Nodes()` return.  |
| `WithLogger(l)`                  | nil     | The `*slog.Logger` to log to. nil means `slog.Default()`.                                 |

### Connector

* `Primary() NodeInfo` - the primary node. Without one, `Avail` is false and `LastError` is `ErrNoPrimary`.
* `Nearest() NodeInfo` - the nearest available node. Without one, `Avail` is false and `LastError` is `ErrNoNode`.
* `WaitForPrimary(ctx) NodeInfo` - wait until there is a primary. If `ctx` is done first, `LastError` is `ctx.Err()`.
* `WaitForNearest(ctx) NodeInfo` - wait until a node is available, and return the nearest.
* `Nodes() []NodeInfo` - all nodes that the last check found, including the unavailable ones, with the reason in
  their `LastError`.
* `Refresh(ctx) error` - check the cluster now, and wait for the result. Concurrent calls share one check, and
  checks are never started less than `RetryInterval` apart.
* `Logger() *slog.Logger` - the logger that the connector uses.
* `Close()` - stop checking. Afterwards, the methods above return `ErrClosed` (in `LastError` for a `NodeInfo`),
  and `Nodes()` returns nil.

All methods are safe for concurrent use.

### Pool

`NewPool(connector, cfg)` returns a `Pool`, which keeps a `pgxpool.Pool` for each node, and creates it with the
positive fields of `cfg` (`DbPoolConfig`) when the node is first used. The other settings keep pgxpool's
defaults, or the `pool_*` settings of the connection string. When a node's conninfo changes in `repmgr.nodes`,
its pool is replaced, and when a node is removed from the cluster, its pool is closed.

* `ConnectPrimary(ctx)` / `ConnectNearest(ctx)` - a `*pgxpool.Conn` to the primary or the nearest node. You must
  call `Release()` on it.
* `InTransaction(ctx, fn)` - run `fn` in a transaction on the primary. It commits if `fn` returns nil, and rolls
  back otherwise.
* `PrimaryPool()` / `NearestPool()` / `GetPool(node)` - the `*pgxpool.Pool` of a node.
* `Close()` - close the pools of all nodes. It waits until all connections are released, and it doesn't close
  the connector.

When `ConnectPrimary`, `ConnectNearest` or `InTransaction` fails with an error for which `IsNodeError(err)` is
true, it refreshes the connector before it returns the error. It doesn't retry by itself, because only you know
whether your transaction is safe to run again (after a lost connection, a commit may or may not have happened).
A retry loop could look like this:

```go
for attempt := 1; ; attempt++ {
	err = pool.InTransaction(ctx, insertEvent)
	if err == nil || attempt == 5 || ctx.Err() != nil || !pgxrepmgr.IsNodeError(err) {
		break
	}
}
```

`Refresh` waits for a new check, and checks are at least `RetryInterval` apart, so the attempts don't spin while
there is no primary.

The `InTransaction(ctx, pool, fn)` function does the same as `Pool.InTransaction` with any `*pgxpool.Pool`,
without the refresh.

### Errors

* `ErrClosed` - the connector or the pool is closed.
* `ErrNoPrimary` / `ErrNoNode` - there is no primary or no available node right now.
* `IsNodeError(err) bool` - whether `err` suggests that you should call `Refresh` before retrying.

### NodeInfo

```go
type NodeInfo struct {
	// The node's own row in repmgr.nodes
	NodeId         int
	UpstreamNodeId *int
	Active         bool
	NodeName       string
	Type           string
	Location       string
	Priority       int
	ConnInfo       string
	ReplUser       string
	SlotName       string
	ConfigFile     string

	// The node claims to be the primary: its own row says so, and it is
	// writable
	IsPrimary bool
	// The node can be used: it answered, and its own row in repmgr.nodes
	// is active
	Avail bool
	// The node is in recovery (a standby)
	InRecovery bool
	// The timeline the node writes WAL on (0 when InRecovery)
	Timeline int
	// Bytes of WAL the node had yet to replay, compared to the primary
	// (0 for the primary, -1 if unknown)
	LagBytes int64
	// How long connecting took at the last check (0 if it failed)
	Latency time.Duration
	// The time it took to fetch the node's state and repmgr.nodes
	FetchDuration time.Duration
	// Time of the last check
	LastChecked time.Time
	// Why the node is not available (nil when Avail)
	LastError error
}
```

`NodeId` is `InvalidNodeId` in the `NodeInfo` that `Primary()`, `Nearest()` and the `WaitFor` methods return when
there is no node.

## Connection strings

Only key=value connection strings are supported, not `postgres://` URLs. Values can be quoted with single quotes,
and a backslash escapes a quote or a backslash, for example `password='it\'s secret'`.

`ConnInfo` holds the node's conninfo from `repmgr.nodes`, with the app overrides applied. It is normalized: its
keys are sorted, and its values are quoted where needed, so the same settings always give the same string. You
can pass it to `pgx.Connect` or `pgxpool.New` yourself if you don't use `Pool`.

The package also has helpers for connection strings: `ParseConnString`, `BuildConnString`, `ReplaceInDsn`,
`DsnWithHostPort`, `DsnWithDatabase` and `RemoveSensitiveFromDsn`. The last one removes the user and the password,
for example to log a connection string.

## Caveats

* The connector must be able to connect to the `repmgr` database (or the one in the check overrides) and read
  `repmgr.nodes`. See [repmgr user and metadata](https://www.repmgr.org/docs/current/repmgr-user-metadata.html).
* The connector connects to the nodes with the conninfo in `repmgr.nodes`, so your program must be able to reach
  the nodes at those addresses. That is usually no problem inside a private network (or a docker overlay network
  etc.) where every node is available through its hostname. Overrides can change any setting, but they are the
  same for all nodes, so they can't map each node to a different address.
* `Primary()` and `Nearest()` are as old as the last check. A node can fail between two checks, so you still have
  to handle connection errors.

## Why use this?

There are other ways to reach the primary of a PostgreSQL cluster:

* **A proxy**, such as HAProxy with health checks, or pgpool-II. They know the primary and stop sending
  connections to dead nodes. But they are more servers to run and keep highly available themselves, they add a
  network hop, and the node list is in their configuration.
* **pgx's multi-host connection strings**, such as `host=a,b,c target_session_attrs=primary`. They need no extra
  library, but pgx tries the hosts in order at every connect, so a dead node can cost a connect timeout each time. The host
  list is static, there is no nearest node, and an old primary that was cut off from the other nodes is still
  writable, so pgx can connect to it.
* **General cluster libraries**, such as [hasql](https://github.com/yandex/go-hasql). It checks each node's role
  periodically, but it works with `database/sql` types, its node list comes from a discoverer you give it (the
  included one is static), and when several nodes report that they are primary, its default picker chooses one of
  them at random.

pgxrepmgr is for programs that use pgx and a repmgr cluster. It reads the node list from `repmgr.nodes`, so new
nodes are used and removed nodes are dropped without configuration. It knows which nodes are available before
your program needs one, uses the timeline to ignore an old primary after a failover, picks the nearest node
by latency and replication lag, and gives you pgx pools that follow the primary. It needs no extra servers, and
your connections go straight to the database.

## Testing

`go test ./...` runs the unit tests, which need no database. `testenv/` has a three-node PostgreSQL 17 cluster
with repmgr 5.5 and automatic failover in docker, and integration tests that use it (on Linux):

```sh
# Run the integration tests. They start the cluster if needed.
go test -tags integration -v ./testenv
# List the commands that control the cluster
testenv/cluster.sh
# Print every change of the primary and the nearest node
go run ./testenv/watch -write
```

## License

MIT, see [LICENSE](LICENSE).
