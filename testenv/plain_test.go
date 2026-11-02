//go:build integration

package testenv

import (
	"context"
	"errors"
	"log/slog"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/nagylzs/pgxrepmgr"
)

// The nodes of the plain cluster (plain.yaml), which has no repmgr.
var plainNodes = []pgxrepmgr.Node{
	{ID: 1, Name: "plain1", ConnInfo: "host=172.28.1.11 user=app dbname=app sslmode=disable"},
	{ID: 2, Name: "plain2", ConnInfo: "host=172.28.1.12 user=app dbname=app sslmode=disable"},
	{ID: 3, Name: "plain3", ConnInfo: "host=172.28.1.13 user=app dbname=app sslmode=disable"},
}

// The single node of plain.yaml that has its own initdb, and so another system identifier.
var otherNode = pgxrepmgr.Node{
	ID: 4, Name: "other", ConnInfo: "host=172.28.1.21 user=app dbname=app sslmode=disable"}

// Without repmgr, the connector takes the nodes from a NodeSource, and finds the primary from their
// state. It follows the source's list as it changes. When a standby is promoted while the old primary
// still runs, the promoted node is on a higher timeline, so the connector switches to it, and doesn't
// use the old primary at all, not even for reading.
func TestSourceWithoutRepmgr(t *testing.T) {
	plain(t, "up")
	src := &listSource{nodes: plainNodes[:2]}
	rc := pgxrepmgr.NewConnectorWithSource(plainOptions(t), src)
	t.Cleanup(rc.Close)
	pool := pgxrepmgr.NewPool(rc, pgxrepmgr.DBPoolConfig{})
	t.Cleanup(pool.Close)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	waitForPrimary(t, rc, "plain1")
	expectAvail(t, rc, "plain1", "plain2")
	if node, err := insert(ctx, pool); err != nil || node != "plain1" {
		t.Fatalf("the write went to %q (error %v), want plain1", node, err)
	}

	src.set(plainNodes)
	refreshNow(t, rc)
	expectAvail(t, rc, "plain1", "plain2", "plain3")

	plain(t, "promote", "plain2")
	refreshNow(t, rc)
	expectPrimary(t, rc, "plain2", "plain1", 3*time.Second, "after plain2 was promoted")
	for _, n := range rc.Nodes() {
		if n.NodeName == "plain1" && (n.Avail || n.LastError == nil || !strings.Contains(n.LastError.Error(), "split brain")) {
			t.Errorf("plain1: Avail=%v LastError=%v, want a split brain error", n.Avail, n.LastError)
		}
	}
	if node, err := insert(ctx, pool); err != nil || node != "plain2" {
		t.Errorf("after the promotion, the write went to %q (error %v), want plain2", node, err)
	}

	src.set(plainNodes[1:])
	refreshNow(t, rc)
	expectAvail(t, rc, "plain2", "plain3")
}

// After pg_upgrade, the upgraded cluster starts on timeline 1, so a primary of the old cluster that
// runs on a higher timeline wins by the timeline rule. With Node.SystemID, the connector uses only the
// nodes of the right cluster. Here the plain cluster is the old one, with plain2 promoted to timeline
// 2 while plain1 still runs, and other is the new one, on timeline 1.
func TestSourceSystemID(t *testing.T) {
	plain(t, "up")
	plain(t, "promote", "plain2")
	nodes := []pgxrepmgr.Node{plainNodes[0], plainNodes[1], otherNode}
	src := &listSource{nodes: nodes}
	rc := pgxrepmgr.NewConnectorWithSource(plainOptions(t), src)
	t.Cleanup(rc.Close)
	pool := pgxrepmgr.NewPool(rc, pgxrepmgr.DBPoolConfig{})
	t.Cleanup(pool.Close)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	waitForPrimary(t, rc, "plain2")

	id := systemID(ctx, t, otherNode.ConnInfo)
	nodes = slices.Clone(nodes)
	for i := range nodes {
		nodes[i].SystemID = id
	}
	src.set(nodes)
	refreshNow(t, rc)
	expectPrimary(t, rc, "other", "plain2", 3*time.Second, "with the system identifiers")
	for _, n := range rc.Nodes() {
		if n.NodeName != "other" && (n.Avail || !errors.Is(n.LastError, pgxrepmgr.ErrSystemIDMismatch)) {
			t.Errorf("%s: Avail=%v LastError=%v, want ErrSystemIDMismatch", n.NodeName, n.Avail, n.LastError)
		}
	}
	if node, err := insert(ctx, pool); err != nil || node != "other" {
		t.Errorf("the write went to %q (error %v), want other", node, err)
	}
}

// systemID returns the system identifier of the node at connInfo.
func systemID(ctx context.Context, t *testing.T, connInfo string) int64 {
	t.Helper()
	conn, err := pgx.Connect(ctx, connInfo+" password=app")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	var id int64
	if err := conn.QueryRow(ctx, "select system_identifier from pg_control_system()").Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

// plainOptions returns options for a connector of the plain cluster that logs through t. It checks the
// nodes in the app database of their connection strings, because the plain cluster has no repmgr
// database.
func plainOptions(t *testing.T) pgxrepmgr.Options {
	return pgxrepmgr.NewOptions("app", "app").
		WithoutCheckOverride("dbname").
		WithCheckInterval(checkInterval).
		WithConnectTimeout(time.Second).
		WithFetchTimeout(time.Second).
		WithAppOverride("password", "app").
		WithLogger(slog.New(slog.NewTextHandler(testWriter{t}, nil)))
}

// listSource is a NodeSource whose list the test can change.
type listSource struct {
	mu    sync.Mutex
	nodes []pgxrepmgr.Node
}

func (s *listSource) Nodes(context.Context) ([]pgxrepmgr.Node, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.nodes), nil
}

func (s *listSource) set(nodes []pgxrepmgr.Node) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nodes = nodes
}

// refreshNow makes the connector check the cluster, and waits for the result.
func refreshNow(t *testing.T, rc *pgxrepmgr.Connector) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := rc.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
}

// expectAvail checks that the connector knows exactly the nodes names, in this order, and that they
// are all available.
func expectAvail(t *testing.T, rc *pgxrepmgr.Connector, names ...string) {
	t.Helper()
	var got []string
	for _, n := range rc.Nodes() {
		got = append(got, n.NodeName)
		if !n.Avail {
			t.Errorf("%s is not available: %v", n.NodeName, n.LastError)
		}
	}
	if !slices.Equal(got, names) {
		t.Errorf("the connector knows the nodes %v, want %v", got, names)
	}
}

func plain(t *testing.T, args ...string) {
	t.Helper()
	out, err := exec.Command("./plain.sh", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("plain.sh %s: %v\n%s", strings.Join(args, " "), err, out)
	}
}
