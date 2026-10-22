package pgxrepmgr

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// closedPort is a seed where nothing listens, so that checks fail right away.
const closedPort = "host=127.0.0.1 port=1"

// syncBuffer is a bytes.Buffer that the connector's goroutine and the test can use at the same time.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func quietOptions() Options {
	return NewOptions("repmgr", "secret").WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// newTestConnector returns a connector that is closed when the test ends.
func newTestConnector(t *testing.T, opts Options, seed string) *Connector {
	c := NewConnector(opts, seed)
	t.Cleanup(c.Close)
	return c
}

// checkCounts returns how many checks have started and finished.
func (c *Connector) checkCounts() (started, finished int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.checksStarted, c.checksFinished
}

// hangingServer accepts connections but never answers, so that a check connecting to it hangs until
// ConnectTimeout. It returns a seed for it, and a channel that receives a value for each accepted
// connection.
func hangingServer(t *testing.T) (string, <-chan struct{}) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	accepted := make(chan struct{}, 100)
	go func() {
		var conns []net.Conn
		defer func() {
			for _, conn := range conns {
				conn.Close()
			}
		}()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			conns = append(conns, conn)
			accepted <- struct{}{}
		}
	}()
	host, port, _ := net.SplitHostPort(ln.Addr().String())
	return "host=" + host + " port=" + port, accepted
}

func waitForAccept(t *testing.T, accepted <-chan struct{}) {
	t.Helper()
	select {
	case <-accepted:
	case <-time.After(5 * time.Second):
		t.Fatal("the connector didn't connect")
	}
}

// Without Options.Logger, the connector logs through slog.Default().
func TestNilLoggerUsesDefault(t *testing.T) {
	var buf syncBuffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })

	// The first check fails, and the connector logs an error.
	newTestConnector(t, NewOptions("repmgr", "secret"), closedPort)
	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(buf.String(), "level=ERROR") {
		if time.Now().After(deadline) {
			t.Fatalf("the connector logged no error through slog.Default(), log: %q", buf.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestClose(t *testing.T) {
	c := NewConnector(quietOptions(), closedPort)
	c.Close()
	c.Close() // closing twice is fine
	// Nothing may wait for this context to be done
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for name, n := range map[string]NodeInfo{
		"Primary":        c.Primary(),
		"Nearest":        c.Nearest(),
		"WaitForPrimary": c.WaitForPrimary(ctx),
		"WaitForNearest": c.WaitForNearest(ctx),
	} {
		if n.Avail || !errors.Is(n.LastError, ErrClosed) {
			t.Errorf("%s after Close: Avail=%v LastError=%v, want ErrClosed", name, n.Avail, n.LastError)
		}
	}
	if err := c.Refresh(ctx); !errors.Is(err, ErrClosed) {
		t.Errorf("Refresh after Close: %v, want ErrClosed", err)
	}
}

// Close cancels a check in progress, rather than waiting for it.
func TestCloseCancelsCheck(t *testing.T) {
	seed, accepted := hangingServer(t)
	c := NewConnector(quietOptions().WithConnectTimeout(time.Minute), seed)
	waitForAccept(t, accepted)
	start := time.Now()
	c.Close()
	if d := time.Since(start); d > time.Second {
		t.Errorf("Close took %v, it should cancel the check in progress", d)
	}
}

// Refresh doesn't count a check that was already running when it was called, because that check may
// have started before the error that made the caller refresh.
func TestRefreshWaitsForNewCheck(t *testing.T) {
	seed, accepted := hangingServer(t)
	c := newTestConnector(t, quietOptions().
		WithConnectTimeout(300*time.Millisecond).
		WithRetryInterval(100*time.Millisecond), seed)
	waitForAccept(t, accepted) // the first check is running now
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if _, finished := c.checkCounts(); finished < 2 {
		t.Errorf("Refresh returned after check %d, it should wait for check 2", finished)
	}
}

// Concurrent Refresh calls share checks, rather than each waiting for a check of its own.
func TestRefreshSharesChecks(t *testing.T) {
	c := newTestConnector(t, quietOptions().WithRetryInterval(200*time.Millisecond), closedPort)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	before, _ := c.checkCounts()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := c.Refresh(ctx); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if after, _ := c.checkCounts(); after-before > 3 {
		t.Errorf("20 concurrent Refresh calls took %d checks, they should share them", after-before)
	}
}

// Checks are at least RetryInterval apart, even when Refresh is called all the time.
func TestRefreshKeepsRetryInterval(t *testing.T) {
	retry := 200 * time.Millisecond
	c := newTestConnector(t, quietOptions().WithRetryInterval(retry), closedPort)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	for i := 0; i < 5; i++ {
		if err := c.Refresh(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if d := time.Since(start); d < 4*retry {
		t.Errorf("5 Refresh calls in a row took %v, checks should be at least %v apart", d, retry)
	}
}

func TestWithoutRemoved(t *testing.T) {
	ids := func(nodes []NodeInfo) []int {
		var ids []int
		for _, n := range nodes {
			ids = append(ids, n.NodeId)
		}
		return ids
	}
	// Node 3 is the primary, and its repmgr.nodes no longer lists node 1.
	nodes := []NodeInfo{{NodeId: 1}, {NodeId: 2}, {NodeId: 3}}
	kept, primaryIdx, removed := withoutRemoved(nodes, 2, []NodeInfo{{NodeId: 2}, {NodeId: 3}})
	if !slices.Equal(ids(kept), []int{2, 3}) || primaryIdx != 1 || !slices.Equal(ids(removed), []int{1}) {
		t.Errorf("got kept=%v primaryIdx=%d removed=%v, want kept=[2 3] primaryIdx=1 removed=[1]",
			ids(kept), primaryIdx, ids(removed))
	}
}

func TestSetLag(t *testing.T) {
	nodes := []NodeInfo{
		{NodeId: 1, Avail: true, lsn: 1000},  // primary
		{NodeId: 2, Avail: true, lsn: 400},   // 600 bytes behind
		{NodeId: 3, Avail: true, lsn: 1010},  // read after the primary, so it seems to be ahead
		{NodeId: 4, Avail: true},             // position unknown
		{NodeId: 5, Avail: false, lsn: 1000}, // not available, for example a rogue primary
	}
	setLag(nodes, 0)
	var got []int64
	for _, n := range nodes {
		got = append(got, n.LagBytes)
	}
	if want := []int64{0, 600, 0, -1, -1}; !slices.Equal(got, want) {
		t.Errorf("with a primary: got %v, want %v", got, want)
	}
	setLag(nodes, -1)
	for _, n := range nodes {
		if n.LagBytes != -1 {
			t.Errorf("without a primary: node %d has LagBytes=%d, want -1", n.NodeId, n.LagBytes)
		}
	}
}

func TestSelectNearest(t *testing.T) {
	const mb = 1 << 20
	nodes := []NodeInfo{
		{NodeId: 1, Avail: true, Latency: 30 * time.Millisecond, LagBytes: 0},       // primary
		{NodeId: 2, Avail: true, Latency: 10 * time.Millisecond, LagBytes: 5 * mb},  // closest, far behind
		{NodeId: 3, Avail: true, Latency: 20 * time.Millisecond, LagBytes: 100_000}, // a bit behind
		{NodeId: 4, Avail: true, Latency: 15 * time.Millisecond, LagBytes: -1},      // lag unknown
		{NodeId: 5, Avail: false, Latency: time.Millisecond},
	}
	for _, tc := range []struct {
		name        string
		primaryIdx  int
		maxLagBytes int64
		want        int
	}{
		{"no limit", 0, 0, 2},
		{"limit", 0, mb, 3},
		{"only the primary is within the limit", 0, 1000, 1},
		{"no primary, so the lag is unknown", -1, mb, 2},
	} {
		if idx := selectNearest(nodes, tc.primaryIdx, tc.maxLagBytes); idx < 0 || nodes[idx].NodeId != tc.want {
			t.Errorf("%s: got index %d, want node %d", tc.name, idx, tc.want)
		}
	}
}

// Primary and Nearest give a reason when there is no node, and Nodes shows why each node is not
// available, with AppOverrides applied.
func TestNoNode(t *testing.T) {
	c := newTestConnector(t, quietOptions().WithAppOverride("application_name", "my app"), closedPort)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if n := c.Primary(); n.Avail || n.NodeId != InvalidNodeId || !errors.Is(n.LastError, ErrNoPrimary) {
		t.Errorf("Primary: NodeId=%v Avail=%v LastError=%v, want ErrNoPrimary", n.NodeId, n.Avail, n.LastError)
	}
	if n := c.Nearest(); n.Avail || n.NodeId != InvalidNodeId || !errors.Is(n.LastError, ErrNoNode) {
		t.Errorf("Nearest: NodeId=%v Avail=%v LastError=%v, want ErrNoNode", n.NodeId, n.Avail, n.LastError)
	}
	nodes := c.Nodes()
	if len(nodes) != 1 {
		t.Fatalf("Nodes returned %d nodes, want the seed", len(nodes))
	}
	if n := nodes[0]; n.LastError == nil || !strings.Contains(n.ConnInfo, "application_name='my app'") {
		t.Errorf("Nodes: ConnInfo=%q LastError=%v, want the app override and the connect error", n.ConnInfo, n.LastError)
	}
}

// The With methods of Options don't change the Options they were called on.
func TestOptionsCopyOverrides(t *testing.T) {
	base := NewOptions("repmgr", "secret")
	a := base.WithAppOverride("dbname", "a").WithCheckOverride("sslmode", "require")
	b := base.WithAppOverride("dbname", "b")
	if base.AppOverrides["dbname"] != "" || base.CheckOverrides["sslmode"] != "" {
		t.Errorf("the With methods changed the original Options")
	}
	if a.AppOverrides["dbname"] != "a" || b.AppOverrides["dbname"] != "b" {
		t.Errorf("got dbname %q and %q, want a and b", a.AppOverrides["dbname"], b.AppOverrides["dbname"])
	}
	// Options{} has no maps
	if o := (Options{}).WithAppOverride("dbname", "app"); o.AppOverrides["dbname"] != "app" {
		t.Errorf("WithAppOverride on Options{} didn't set the override")
	}
}
