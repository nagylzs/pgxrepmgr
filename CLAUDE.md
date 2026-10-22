# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A Go library (module `github.com/nagylzs/pgxrepmgr`, single package `pgxrepmgr` at the repo root) that discovers and monitors a PostgreSQL repmgr cluster by reading `repmgr.nodes`. It gives callers the current primary node and the nearest node (lowest connect latency), plus cached pgx pools for them. Dependencies: `jackc/pgx/v5` and `teivah/broadcast`.

## Commands

```sh
go build ./...                         # build the library and the watch program
go vet ./...                           # vet everything
go test ./...                          # unit tests, no database needed

go test -tags integration -v ./testenv # integration tests; they start the docker test cluster if needed
testenv/cluster.sh up                  # start the docker test cluster (no arguments: list all commands)
go run ./testenv/watch -write          # run a Connector against it and print every primary/nearest change
testenv/cluster.sh down                # remove the cluster and its data
```

`go test ./...` runs only the unit tests, which need no database (`*_test.go` at the root). They use seeds where nothing listens (`closedPort`) or where a server accepts connections and never answers (`hangingServer`). The integration tests in `testenv/` need the `integration` build tag.

## Test environment (`testenv/`)

`testenv/compose.yaml` runs three PostgreSQL 17 nodes with repmgr 5.5 and repmgrd (automatic failover). They are node1 `172.28.0.11` (the initial primary), node2 `.12` and node3 `.13` (lower priority). The nodes register in `repmgr.nodes` by IP address, and on Linux the host reaches those addresses directly, so Go code under test runs on the host with no port mapping. Credentials are `repmgr`/`repmgr` (the `watch` default) and `app`/`app`. Database `app` has a `writes` table whose `node` column defaults to the name of the node that accepted the insert. A fresh `up` takes about 25 seconds.

`cluster.sh` is the interface to the cluster: `show` prints each node's own copy of `repmgr.nodes` (which is what the connector reads), plus its recovery state, timeline and write count. `isolate <node>` uses iptables inside the node to cut it off from the other nodes while the host can still reach it, and `heal` undoes that. It resets TCP connections instead of dropping them, because with DROP, open connections hung: a repmgrd stuck on one missed the failover, and after `heal` a standby received WAL from the old primary and could no longer follow the new one. `repair` keeps the node on the highest timeline as primary and rejoins every other node that doesn't stream from it within 15 seconds. `delay <node> <ms>` adds latency with netem. `stop`/`kill`/`start` stop a node cleanly, crash it, or start it again. `rejoin <node> <primary>` turns a former primary into a standby with pg_rewind.

`up` leaves a running cluster as it is. After you change the `Dockerfile` or `node-entrypoint.sh`, use `up --build`, which recreates the nodes. Each build gets a new image ID even when nothing changed, which is why `up` doesn't rebuild every time.

`testenv/watch` prints a line only when the primary, the nearest node or the write result changes. With `-write` it inserts through `Pool.InTransaction` at every poll and shows which node accepted the row.

`testenv/integration_test.go` drives the cluster through `cluster.sh`. Each test starts with `cluster.sh up`, which waits for a healthy cluster, and finds the current primary by asking the nodes directly, so it works whichever node is primary. Tests that cause a failover run `heal` and `repair` in cleanup, which leaves the cluster healthy for the next run with a different primary. `TestFailoverIgnoresOldPrimary` takes about 70 seconds and `TestWritesFollowFailoverQuickly` about 40. `TestNearestSkipsLaggingStandby` takes about 5: it uses `delay` to make one standby the nearest node, pauses replay on it with `pg_wal_replay_pause()`, and writes a few MB of WAL. It connects to the nodes with `execSQL`. `newConnector` closes the connector in `t.Cleanup` and logs through `t`, so the log shows up for failed tests or with `-v`.

## Architecture

**Background monitor (`connector.go`).** `NewConnector` validates the options, seeds `initial` with the given DSNs (each with `NodeId: InvalidNodeId`) and starts `run()` in a goroutine. `run` alternates `check` and `wait` until `Close` cancels `c.ctx`. That context also cancels a check in progress; `logCheckError` keeps the resulting errors out of the log. `run` closes `c.done` when it returns, and then `Primary`/`Nearest`/`WaitFor*`/`Refresh` all report `ErrClosed`. Each `check`:
1. Calls `checkNode` on every known node in parallel (`collectInfos`). It uses the seed DSNs until the initial scan is complete and the previous `nodes` after that. `checkNode` connects and measures connect latency. In one query it asks the node whether it is in recovery, which timeline it writes WAL on, and its WAL position as a byte count (the replay position for a standby). The position goes in the unexported `NodeInfo.lsn`, with 0 meaning unknown. Then it selects all rows from `repmgr.nodes` with `pgx.CollectRows`, and uses them only if it could read all of them, so a failed read never leaves a node `Avail` with a partial list. A node that doesn't end up `Avail` gets a `LastError` saying why.
2. Collects any node IDs found in those rows that were not checked yet, and checks them too. During the initial scan the results for the seed DSNs are thrown away and replaced by the results for the discovered nodes, which have real node IDs.
3. Picks `primaryIdx` with `selectPrimary` (see below). If there is a primary, `withoutRemoved` drops the nodes that the primary's own copy of `repmgr.nodes` doesn't list, because they were unregistered, and bumps `removals`. `setLag` sets each `Avail` node's `LagBytes`, which is the primary's position minus the node's, clamped at 0. It is -1 when unknown and for every node when there is no primary. Then `selectNearest` picks `nearestIdx`, the `Avail` result with the lowest `Latency`. With `Options.MaxLagBytes > 0` and a primary, it skips nodes whose lag is over the limit or unknown. The primary's lag is 0, so it is always a candidate. It swaps them into `c.nodes` (unexported; callers get copies from `Nodes()`) under `mu` together with `checksFinished++`, then calls `relay.Broadcast` to wake any `WaitForPrimary`/`WaitForNearest`/`Refresh` callers. `initialScanComplete` becomes true only once a nearest node exists. Without a primary or nearest node, `Primary()`/`Nearest()` return `NodeId: InvalidNodeId` with `LastError` set to `ErrNoPrimary`/`ErrNoNode` (`errors.go`), and `IsNodeError` counts both as node errors.

**Scheduling and `Refresh`.** `wait` waits until `CheckInterval` after the start of the previous check, or `RetryInterval` (default 1 s) after it while there is no primary. A token in the `refresh` channel cuts the wait short, but never to less than `RetryInterval` after the previous start. `Refresh` sets its target to `checksStarted+1`, because a running check may have started before the caller's error. It puts a token in the channel (both under `mu`) and waits until `checksFinished` reaches the target. `run` increments `checksStarted` and drains the token under `mu` at the start of each check, so all `Refresh` calls made before a check starts share it.

**Availability rule.** A node gets `Avail=true` only if its own row (matching `NodeId`) shows up in the `repmgr.nodes` it returned, with `active=true`. Connecting and querying successfully is not enough. A seed DSN has `InvalidNodeId`, so it can never be `Avail` itself; it only serves to discover the real nodes. `selectPrimary` can also turn `Avail` off again (see below).

**Primary selection (split brain).** A node has `IsPrimary=true` if its own row says `type=primary` and the node is not in recovery. After a failover, an old primary that is still running keeps saying that about itself. This happens when it is cut off from the other nodes, or when it can reach them again but hasn't been rejoined yet. Promotion always moves a node to a new, higher timeline, so `selectPrimary` picks the `Avail`+`IsPrimary` node on the highest timeline. The node ID only breaks a tie, which keeps the choice independent of the order in which the checks finished. Every other node that claims to be primary gets `Avail=false` and a "split brain" `LastError`, so it can't be the nearest node either, and the connector logs an error at every check. `TestFailoverIgnoresOldPrimary` in `testenv/integration_test.go` covers both cases.

**Two override maps (`options.go`).** The stored `nodes` keep the raw `conninfo` from `repmgr.nodes`. Overrides are applied only at use time. The `With*Override` methods copy the map before changing it, and `NewConnector` copies both maps, so copies of `Options` never share them.
- `CheckOverrides` are applied when the connector itself connects to check a node. `NewOptions` seeds them with the repmgr user, the password and `dbname=repmgr`.
- `AppOverrides` are applied only when a `NodeInfo` is returned from `Primary()`/`Nearest()`/`Nodes()` (by `forApp`), and so also from `WaitFor*` and `Pool`.

**DSN handling (`dsn.go`).** Only key=value connection strings are supported, not `postgres://` URLs. `ParseConnString` follows pgx's rules: values can be in single quotes, and a backslash escapes a quote or a backslash (other backslashes are kept). It returns an error for malformed input, and its errors never contain the string, because it may hold a password. `BuildConnString` sorts the keys and quotes values that are empty or contain a space, quote or backslash, so a DSN that has been through `ReplaceInDsn` is normalized and deterministic. `NewConnector` panics on a seed that doesn't parse; `checkNode` sets `LastError` for a conninfo that doesn't. `dsn_test.go` checks that pgx reads what `BuildConnString` writes.

**Pools (`pool.go`, `transaction.go`).** `Pool` wraps a `Connector` and lazily creates one `pgxpool.Pool` per `NodeId`, configured from `DbPoolConfig`. `DbPoolConfig.ApplyTo` copies only the positive fields. The others keep pgxpool's value, which is its default or the `pool_*` setting from the DSN, so `DbPoolConfig{}` is valid. `GetPool` returns the node's `LastError` for a node that isn't `Avail` (for example `ErrNoPrimary` from `PrimaryPool`). It replaces a node's pool when its `ConnInfo` changed. When the connector's `removals` count has changed, it also closes the pools of nodes the connector no longer knows. Either way the old pool is closed in a goroutine, because `pgxpool.Pool.Close` waits for acquired connections. Pools of nodes that are only temporarily unavailable are kept. `Pool.Close` closes all pools but not the connector. `ConnectPrimary`/`ConnectNearest` return a `*pgxpool.Conn` that the caller must `Release()`. `InTransaction` always runs on the primary: it commits if the callback returns nil, rolls back otherwise, and returns the commit error. When `ConnectPrimary`, `ConnectNearest` (both through `acquire`) or `InTransaction` fails with `IsNodeError(err)` (`errors.go`), they call `Refresh` with the caller's context before returning the error. That includes `GetPool` failing with `ErrNoPrimary`/`ErrNoNode`, so a caller's retry loop waits for the next check instead of spinning. They never retry.

**Logging.** The connector logs through `c.Logger()`, which returns `Options.Logger` (set it with `WithLogger`), or `slog.Default()` when that is nil. `NewOptions` leaves it nil. The fallback looks up `slog.Default()` each time it logs, so a later `slog.SetDefault` still applies. `InTransaction` in `transaction.go` has no connector, so it logs rollback errors through `slog.Default()`.

## README

`README.md` is the user-facing documentation. Update it when you change the public API.

## Known issues

These came out of a review done before the first public release. Remove an item when it's fixed.

None open.
