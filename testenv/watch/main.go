// Command watch runs a Connector against the test cluster (see testenv/cluster.sh) and prints a
// line whenever the primary or the nearest node changes.
//
// With -write it also inserts a row into app.writes through Pool.InTransaction at every poll, and
// shows which node accepted it. That makes it visible when writes go to the wrong node.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nagylzs/pgxrepmgr"
)

func main() {
	interval := flag.Duration("interval", 2*time.Second, "connector check interval")
	poll := flag.Duration("poll", 200*time.Millisecond, "how often to read Primary() and Nearest()")
	write := flag.Bool("write", false, "insert a row into app.writes on the primary at every poll")
	debug := flag.Bool("debug", false, "show the connector's debug log messages")
	maxLag := flag.Int64("max-lag", 0, "Nearest skips standbys more bytes of WAL behind the primary (0: no limit)")
	flag.Usage = func() {
		fmt.Fprintln(flag.CommandLine.Output(), "Usage: go run ./testenv/watch [flags] [seed DSN...]")
		flag.PrintDefaults()
	}
	flag.Parse()

	seeds := flag.Args()
	if len(seeds) == 0 {
		seeds = []string{"host=172.28.0.11", "host=172.28.0.12", "host=172.28.0.13"}
	}
	password := os.Getenv("REPMGR_PASSWORD")
	if password == "" {
		password = "repmgr"
	}
	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}

	opts := pgxrepmgr.NewOptions("repmgr", password).
		WithCheckOverride("sslmode", "disable").
		WithCheckInterval(*interval).
		WithConnectTimeout(time.Second).
		WithFetchTimeout(time.Second).
		WithMaxLagBytes(*maxLag).
		WithAppOverride("user", "app").
		WithAppOverride("password", "app").
		WithAppOverride("dbname", "app").
		WithAppOverride("sslmode", "disable").
		WithLogger(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})))
	rc := pgxrepmgr.NewConnector(opts, seeds...)
	pool := pgxrepmgr.NewPool(rc, pgxrepmgr.DbPoolConfig{MaxConns: 2})

	last := ""
	for {
		state := fmt.Sprintf("primary=%s nearest=%s", describe(rc.Primary()), describe(rc.Nearest()))
		if *write {
			state += " write=" + writeRow(pool)
		}
		if state != last {
			fmt.Println(time.Now().Format("15:04:05.000"), state)
			last = state
		}
		time.Sleep(*poll)
	}
}

func describe(n pgxrepmgr.NodeInfo) string {
	if !n.Avail {
		return "none"
	}
	return n.NodeName
}

// writeRow inserts a row on the current primary and returns the name of the node that stored it.
func writeRow(pool *pgxrepmgr.Pool) string {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var node string
	err := pool.InTransaction(ctx, func(_ *pgxpool.Conn, tx pgx.Tx) error {
		return tx.QueryRow(ctx, "insert into writes default values returning node").Scan(&node)
	})
	if err != nil {
		return "error(" + err.Error() + ")"
	}
	return "ok@" + node
}
