//go:build integration

// Integration tests against the docker cluster in this directory. They start the cluster if it isn't
// running, and leave it running and healthy, possibly with a different primary:
//
//	go test -tags integration -v ./testenv
package testenv

import (
	"context"
	"fmt"
	"log/slog"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nagylzs/pgxrepmgr"
)

type node struct {
	name string
	host string
}

var nodes = []node{
	{"node1", "172.28.0.11"},
	{"node2", "172.28.0.12"},
	{"node3", "172.28.0.13"},
}

const checkInterval = time.Second

// After a failover, the old primary still lists itself as the active primary in its own copy of
// repmgr.nodes. The connector must follow the promoted node and never use the old primary, both
// while the old primary is cut off from the other nodes (but not from the client), and after it can
// reach them again but hasn't been rejoined yet.
func TestFailoverIgnoresOldPrimary(t *testing.T) {
	cluster(t, "up")
	old := currentPrimary(t)
	rc := newConnector(t, testOptions(t, checkInterval))
	waitForPrimary(t, rc, old.name)

	cluster(t, "isolate", old.name)
	t.Cleanup(func() {
		cluster(t, "heal")
		cluster(t, "repair")
	})
	promoted := waitForPromotion(t, old, time.Minute)
	t.Logf("%s is isolated, %s was promoted", old.name, promoted.name)
	// The connector notices a failover only at its next check.
	time.Sleep(2*checkInterval + time.Second)
	expectPrimary(t, rc, promoted.name, old.name, 15*time.Second, "while the old primary is isolated")

	cluster(t, "heal")
	expectPrimary(t, rc, promoted.name, old.name, 15*time.Second, "after the old primary is reachable again")
}

// When the primary stops, the first failed write makes the connector check the cluster right away
// (Pool refreshes it after node errors), and while there is no primary, it checks every RetryInterval.
// So writes reach the promoted node soon after the promotion, although CheckInterval is 30 seconds.
func TestWritesFollowFailoverQuickly(t *testing.T) {
	cluster(t, "up")
	old := currentPrimary(t)
	rc := newConnector(t, testOptions(t, 30*time.Second))
	pool := pgxrepmgr.NewPool(rc, pgxrepmgr.DbPoolConfig{})
	t.Cleanup(pool.Close)
	waitForPrimary(t, rc, old.name)

	// Write until a row lands on another node than the old primary
	written := make(chan string, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		for ctx.Err() == nil {
			node, err := insert(ctx, pool)
			if err == nil && node != old.name {
				written <- node
				return
			}
			if err != nil {
				time.Sleep(50 * time.Millisecond)
			}
		}
	}()

	cluster(t, "stop", old.name)
	t.Cleanup(func() {
		cluster(t, "start", old.name)
		cluster(t, "repair")
	})
	promoted := waitForPromotion(t, old, 2*time.Minute)
	promotedAt := time.Now()
	select {
	case node := <-written:
		delay := time.Since(promotedAt)
		t.Logf("%s stopped, %s promoted, the first write reached %s %v after the promotion",
			old.name, promoted.name, node, delay.Round(10*time.Millisecond))
		if node != promoted.name {
			t.Errorf("a write reached %s, but %s was promoted", node, promoted.name)
		}
		if delay > 5*time.Second {
			t.Errorf("the first write reached the promoted node %v after the promotion, want at most 5s", delay)
		}
	case <-time.After(30 * time.Second):
		t.Fatalf("no write reached %s within 30s of its promotion", promoted.name)
	}
}

// With MaxLagBytes, Nearest skips a standby that has fallen behind, here because its replay is paused,
// and uses it again once it has caught up.
func TestNearestSkipsLaggingStandby(t *testing.T) {
	cluster(t, "up")
	primary := currentPrimary(t)
	var standbys []node
	for _, n := range nodes {
		if n != primary {
			standbys = append(standbys, n)
		}
	}
	lagging, other := standbys[0], standbys[1]
	// Slow down the other nodes, so that the lagging standby is the nearest
	for _, n := range []node{primary, other} {
		cluster(t, "delay", n.name, "20")
	}
	t.Cleanup(func() {
		for _, n := range []node{primary, other} {
			cluster(t, "delay", n.name, "0")
		}
	})
	rc := newConnector(t, testOptions(t, checkInterval).WithMaxLagBytes(1<<20))
	waitForNearest(t, rc, lagging.name, true)

	execSQL(t, lagging.host, "repmgr", "select pg_wal_replay_pause()")
	t.Cleanup(func() { execSQL(t, lagging.host, "repmgr", "select pg_wal_replay_resume()") })
	// Several MB of WAL
	execSQL(t, primary.host, "app", "insert into writes select from generate_series(1, 50000)")
	waitForNearest(t, rc, lagging.name, false)
	t.Logf("%s paused replay, the nearest node is %s now", lagging.name, rc.Nearest().NodeName)

	execSQL(t, lagging.host, "repmgr", "select pg_wal_replay_resume()")
	waitForNearest(t, rc, lagging.name, true)
}

// testOptions returns options for a connector of the test cluster that logs through t. Pools made with
// it connect to the app database.
func testOptions(t *testing.T, checkInterval time.Duration) pgxrepmgr.Options {
	return pgxrepmgr.NewOptions("repmgr", "repmgr").
		WithCheckOverride("sslmode", "disable").
		WithCheckInterval(checkInterval).
		WithConnectTimeout(time.Second).
		WithFetchTimeout(time.Second).
		WithAppOverride("user", "app").
		WithAppOverride("password", "app").
		WithAppOverride("dbname", "app").
		WithAppOverride("sslmode", "disable").
		WithLogger(slog.New(slog.NewTextHandler(testWriter{t}, nil)))
}

// newConnector returns a connector for the test cluster that is closed when the test ends.
func newConnector(t *testing.T, opts pgxrepmgr.Options) *pgxrepmgr.Connector {
	seeds := make([]string, 0, len(nodes))
	for _, n := range nodes {
		seeds = append(seeds, "host="+n.host)
	}
	rc := pgxrepmgr.NewConnector(opts, seeds...)
	t.Cleanup(rc.Close)
	return rc
}

// waitForNearest waits up to 10 seconds until the connector's nearest node is name, or with is=false,
// until it is another node.
func waitForNearest(t *testing.T, rc *pgxrepmgr.Connector, name string, is bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for (rc.Nearest().NodeName == name) != is {
		if time.Now().After(deadline) {
			not := map[bool]string{true: "", false: "not "}[is]
			t.Fatalf("after 10s, the nearest node is %q, want %s%s", rc.Nearest().NodeName, not, name)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// execSQL runs sql in the database db on the node at host. The users repmgr and app own the databases
// of the same name, and their passwords are their names.
func execSQL(t *testing.T, host, db, sql string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, fmt.Sprintf("host=%s user=%s password=%s dbname=%s sslmode=disable", host, db, db, db))
	if err != nil {
		t.Fatalf("%s: %v", host, err)
	}
	defer conn.Close(context.Background())
	if _, err := conn.Exec(ctx, sql); err != nil {
		t.Fatalf("%s: %s: %v", host, sql, err)
	}
}

// testWriter writes log lines through t.Log, so that they show up for failed tests, or with -v.
type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(strings.TrimSuffix(string(p), "\n"))
	return len(p), nil
}

// waitForPrimary waits until the connector has a primary, and checks that it is want.
func waitForPrimary(t *testing.T, rc *pgxrepmgr.Connector, want string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if p := rc.WaitForPrimary(ctx); p.NodeName != want {
		t.Fatalf("the connector's primary is %q, the cluster's primary is %s", p.NodeName, want)
	}
}

// insert writes a row through pool, and returns the name of the node that stored it.
func insert(ctx context.Context, pool *pgxrepmgr.Pool) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var node string
	err := pool.InTransaction(ctx, func(_ *pgxpool.Conn, tx pgx.Tx) error {
		return tx.QueryRow(ctx, "insert into writes default values returning node").Scan(&node)
	})
	return node, err
}

// expectPrimary reads rc.Primary() and rc.Nearest() for the duration d. It reports an error if the
// primary was ever anything other than want, or if the nearest node was ever rogue.
func expectPrimary(t *testing.T, rc *pgxrepmgr.Connector, want, rogue string, d time.Duration, when string) {
	t.Helper()
	start := time.Now()
	last, wrongPrimary, rogueNearest, total := want, 0, 0, 0
	var changes []string
	for time.Since(start) < d {
		name := "none"
		if p := rc.Primary(); p.Avail {
			name = p.NodeName
		}
		total++
		if name != want {
			wrongPrimary++
		}
		if name != last {
			changes = append(changes, fmt.Sprintf("+%.1fs %s", time.Since(start).Seconds(), name))
			last = name
		}
		if n := rc.Nearest(); n.Avail && n.NodeName == rogue {
			rogueNearest++
		}
		time.Sleep(50 * time.Millisecond)
	}
	if wrongPrimary > 0 {
		t.Errorf("%s: the primary was not %s in %d of %d samples, changes: %s",
			when, want, wrongPrimary, total, strings.Join(changes, ", "))
	}
	if rogueNearest > 0 {
		t.Errorf("%s: the nearest node was %s in %d of %d samples", when, rogue, rogueNearest, total)
	}
}

// currentPrimary returns the only node that is not in recovery.
func currentPrimary(t *testing.T) node {
	t.Helper()
	var primaries []node
	for _, n := range nodes {
		recovery, err := inRecovery(n.host)
		if err != nil {
			t.Fatalf("%s: %v", n.name, err)
		}
		if !recovery {
			primaries = append(primaries, n)
		}
	}
	if len(primaries) != 1 {
		t.Fatalf("expected exactly one node out of recovery, got %v", primaries)
	}
	return primaries[0]
}

// waitForPromotion waits until a node other than old leaves recovery, and returns it.
func waitForPromotion(t *testing.T, old node, timeout time.Duration) *node {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n == old {
				continue
			}
			if recovery, err := inRecovery(n.host); err == nil && !recovery {
				return &n
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("no standby was promoted within %v", timeout)
	return nil
}

func inRecovery(host string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, "host="+host+" user=repmgr password=repmgr dbname=repmgr sslmode=disable")
	if err != nil {
		return false, err
	}
	defer conn.Close(context.Background())
	var recovery bool
	err = conn.QueryRow(ctx, "select pg_is_in_recovery()").Scan(&recovery)
	return recovery, err
}

func cluster(t *testing.T, args ...string) {
	t.Helper()
	out, err := exec.Command("./cluster.sh", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("cluster.sh %s: %v\n%s", strings.Join(args, " "), err, out)
	}
}
