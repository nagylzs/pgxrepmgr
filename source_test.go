package pgxrepmgr

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// testSource is a NodeSource whose list and error the test can change.
type testSource struct {
	mu    sync.Mutex
	nodes []Node
	err   error
}

func (s *testSource) Nodes(context.Context) ([]Node, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.nodes), s.err
}

func (s *testSource) set(nodes []Node, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nodes, s.err = nodes, err
}

// listed returns a Node at a port where nothing listens.
func listed(id int, port string) Node {
	return Node{ID: id, Name: fmt.Sprintf("node%d", id), ConnInfo: "host=127.0.0.1 port=" + port}
}

// newSourceConnector returns a connector with src that is closed when the test ends. Its checks are
// at least 100 ms apart, so that Refresh doesn't wait long.
func newSourceConnector(t *testing.T, opts Options, src NodeSource) *Connector {
	c := NewConnectorWithSource(opts.WithRetryInterval(100*time.Millisecond), src)
	t.Cleanup(c.Close)
	return c
}

// refresh calls c.Refresh, and fails the test if it doesn't return nil within 5 seconds.
func refresh(t *testing.T, c *Connector) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
}

func nodeIDs(nodes []NodeInfo) []int {
	ids := make([]int, 0, len(nodes))
	for _, n := range nodes {
		ids = append(ids, n.NodeID)
	}
	return ids
}

// Nodes returns the listed nodes in their order, with what the source gave and why they are not
// available, even though none of them can be reached.
func TestSourceNodesWithoutServers(t *testing.T) {
	a, b := listed(1, "1"), listed(2, "2")
	b.Location = "dc2"
	c := newSourceConnector(t, quietOptions().WithAppOverride("application_name", "my app"),
		NodeSourceFunc(func(context.Context) ([]Node, error) { return []Node{a, b}, nil }))
	refresh(t, c)
	nodes := c.Nodes()
	if !slices.Equal(nodeIDs(nodes), []int{1, 2}) {
		t.Fatalf("Nodes returned the IDs %v, want [1 2]", nodeIDs(nodes))
	}
	for i, want := range []Node{a, b} {
		n := nodes[i]
		if n.NodeName != want.Name || n.Location != want.Location || !n.Active {
			t.Errorf("node %d: NodeName=%q Location=%q Active=%v, want %q, %q and true",
				n.NodeID, n.NodeName, n.Location, n.Active, want.Name, want.Location)
		}
		if n.Avail || n.LastError == nil {
			t.Errorf("node %d: Avail=%v LastError=%v, want the connect error", n.NodeID, n.Avail, n.LastError)
		}
		if !strings.Contains(n.ConnInfo, "application_name='my app'") {
			t.Errorf("node %d: ConnInfo=%q, want the app override", n.NodeID, n.ConnInfo)
		}
	}
	if n := c.Primary(); n.Avail || !errors.Is(n.LastError, ErrNoPrimary) {
		t.Errorf("Primary: Avail=%v LastError=%v, want ErrNoPrimary", n.Avail, n.LastError)
	}
	if n := c.Nearest(); n.Avail || !errors.Is(n.LastError, ErrNoNode) {
		t.Errorf("Nearest: Avail=%v LastError=%v, want ErrNoNode", n.Avail, n.LastError)
	}
}

// After Refresh, the connector uses the source's new list: it adds the new nodes, removes the ones
// that are no longer listed, and takes the new connection strings.
func TestSourceListChanges(t *testing.T) {
	src := &testSource{nodes: []Node{listed(1, "1"), listed(2, "2")}}
	c := newSourceConnector(t, quietOptions(), src)
	refresh(t, c)
	if ids := nodeIDs(c.Nodes()); !slices.Equal(ids, []int{1, 2}) {
		t.Fatalf("got the IDs %v, want [1 2]", ids)
	}
	removals := c.removalCount()

	src.set([]Node{listed(2, "3"), listed(3, "1")}, nil)
	refresh(t, c)
	nodes := c.Nodes()
	if ids := nodeIDs(nodes); !slices.Equal(ids, []int{2, 3}) {
		t.Fatalf("after the list changed, got the IDs %v, want [2 3]", ids)
	}
	if !strings.Contains(nodes[0].ConnInfo, "port=3") {
		t.Errorf("node 2 has ConnInfo=%q, want its new port 3", nodes[0].ConnInfo)
	}
	if c.removalCount() == removals {
		t.Errorf("the removal of node 1 didn't count as a removal, so Pool would keep its pool")
	}
}

// When the source fails, or returns an invalid list, the connector logs it, and keeps the last
// valid list.
func TestSourceErrorKeepsList(t *testing.T) {
	var log syncBuffer
	src := &testSource{nodes: []Node{listed(1, "1"), listed(2, "2")}}
	c := newSourceConnector(t, quietOptions().WithLogger(slog.New(slog.NewTextHandler(&log, nil))), src)
	refresh(t, c)
	for _, tc := range []struct {
		name  string
		nodes []Node
		err   error
		log   string
	}{
		{"error", nil, errors.New("source is down"), "source is down"},
		{"duplicate ID", []Node{listed(1, "1"), listed(1, "2")}, nil, "listed more than once"},
		{"invalid ID", []Node{listed(InvalidNodeID, "1")}, nil, "InvalidNodeID"},
	} {
		src.set(tc.nodes, tc.err)
		refresh(t, c)
		if ids := nodeIDs(c.Nodes()); !slices.Equal(ids, []int{1, 2}) {
			t.Errorf("%s: got the IDs %v, want the previous list [1 2]", tc.name, ids)
		}
		if !strings.Contains(log.String(), tc.log) {
			t.Errorf("%s: the log doesn't contain %q", tc.name, tc.log)
		}
	}
}

// Until the source returns a list, there are no nodes.
func TestSourceFailsFromStart(t *testing.T) {
	src := &testSource{err: errors.New("source is down")}
	c := newSourceConnector(t, quietOptions(), src)
	refresh(t, c)
	if nodes := c.Nodes(); len(nodes) != 0 {
		t.Errorf("got the IDs %v, want no nodes", nodeIDs(nodes))
	}
	if n := c.Primary(); !errors.Is(n.LastError, ErrNoPrimary) {
		t.Errorf("Primary: LastError=%v, want ErrNoPrimary", n.LastError)
	}
	src.set([]Node{listed(1, "1")}, nil)
	refresh(t, c)
	if ids := nodeIDs(c.Nodes()); !slices.Equal(ids, []int{1}) {
		t.Errorf("after the source recovered, got the IDs %v, want [1]", ids)
	}
}

// The source gets a context with FetchTimeout, so a source that hangs doesn't stop the checks.
func TestSourceHasFetchTimeout(t *testing.T) {
	src := NodeSourceFunc(func(ctx context.Context) ([]Node, error) {
		if _, ok := ctx.Deadline(); !ok {
			return nil, errors.New("the context has no deadline")
		}
		<-ctx.Done()
		return nil, ctx.Err()
	})
	c := newSourceConnector(t, quietOptions().WithFetchTimeout(100*time.Millisecond), src)
	start := time.Now()
	refresh(t, c)
	refresh(t, c)
	if d := time.Since(start); d > 3*time.Second {
		t.Errorf("two checks with a hanging source took %v", d)
	}
}

// When the source no longer lists a node, Pool closes its pool.
func TestSourcePoolClosesRemovedNode(t *testing.T) {
	src := &testSource{nodes: []Node{listed(1, "1"), listed(2, "2")}}
	c := newSourceConnector(t, quietOptions(), src)
	refresh(t, c)
	p := newTestPool(t, c, DBPoolConfig{})
	// GetPool doesn't connect, so the nodes don't have to be available
	for _, n := range []NodeInfo{node(1, "1"), node(2, "2")} {
		if _, err := p.GetPool(n); err != nil {
			t.Fatal(err)
		}
	}
	src.set([]Node{listed(1, "1")}, nil)
	refresh(t, c)
	if _, err := p.GetPool(node(1, "1")); err != nil {
		t.Fatal(err)
	}
	if _, ok := p.pools[2]; ok {
		t.Errorf("the pool of node 2 is still open, although the source no longer lists it")
	}
	if _, ok := p.pools[1]; !ok {
		t.Errorf("the pool of node 1 was closed")
	}
}

func TestNewConnectorWithSourcePanicsOnNil(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Errorf("NewConnectorWithSource(opts, nil) didn't panic")
		}
	}()
	NewConnectorWithSource(quietOptions(), nil)
}

func TestValidateNodes(t *testing.T) {
	for _, tc := range []struct {
		name  string
		nodes []Node
		ok    bool
	}{
		{"empty", nil, true},
		{"unique", []Node{{ID: 1}, {ID: 2}, {ID: 0}}, true},
		{"duplicate", []Node{{ID: 1}, {ID: 2}, {ID: 1}}, false},
		{"invalid", []Node{{ID: InvalidNodeID}}, false},
	} {
		if err := validateNodes(tc.nodes); (err == nil) != tc.ok {
			t.Errorf("%s: got %v", tc.name, err)
		}
	}
}

// The node on the highest timeline is the primary, the lower ID breaks a tie, and the other nodes
// that claim to be primary are not available.
func TestSelectPrimary(t *testing.T) {
	c := newSourceConnector(t, quietOptions(), &testSource{})
	for _, tc := range []struct {
		name  string
		nodes []NodeInfo
		want  int // node ID, or InvalidNodeID for none
	}{
		{"one primary", []NodeInfo{
			{NodeID: 1, Avail: true, InRecovery: true},
			{NodeID: 2, Avail: true, IsPrimary: true, Timeline: 1},
		}, 2},
		{"highest timeline", []NodeInfo{
			{NodeID: 1, Avail: true, IsPrimary: true, Timeline: 1},
			{NodeID: 2, Avail: true, IsPrimary: true, Timeline: 3},
			{NodeID: 3, Avail: true, IsPrimary: true, Timeline: 2},
		}, 2},
		{"tie", []NodeInfo{
			{NodeID: 3, Avail: true, IsPrimary: true, Timeline: 2},
			{NodeID: 2, Avail: true, IsPrimary: true, Timeline: 2},
		}, 2},
		{"unavailable", []NodeInfo{
			{NodeID: 1, Avail: false, IsPrimary: true, Timeline: 5},
			{NodeID: 2, Avail: true, IsPrimary: true, Timeline: 1},
		}, 2},
		{"none", []NodeInfo{{NodeID: 1, Avail: true, InRecovery: true}}, InvalidNodeID},
	} {
		// Only the available nodes that claim to be primary can lose to the primary
		var claimed []bool
		for _, n := range tc.nodes {
			claimed = append(claimed, n.Avail && n.IsPrimary)
		}
		idx := c.selectPrimary(tc.nodes)
		got := InvalidNodeID
		if idx >= 0 {
			got = tc.nodes[idx].NodeID
		}
		if got != tc.want {
			t.Errorf("%s: got node %d, want node %d", tc.name, got, tc.want)
			continue
		}
		for i, n := range tc.nodes {
			if i != idx && claimed[i] && (n.Avail || n.LastError == nil || !strings.Contains(n.LastError.Error(), "split brain")) {
				t.Errorf("%s: node %d claims to be primary, but has Avail=%v LastError=%v, want a split brain error",
					tc.name, n.NodeID, n.Avail, n.LastError)
			}
		}
	}
}
