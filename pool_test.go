package pgxrepmgr

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// settings returns the fields of cfg that DbPoolConfig can set.
func settings(cfg *pgxpool.Config) DbPoolConfig {
	return DbPoolConfig{
		MaxConnLifetime:       cfg.MaxConnLifetime,
		MaxConnLifetimeJitter: cfg.MaxConnLifetimeJitter,
		MaxConnIdleTime:       cfg.MaxConnIdleTime,
		MaxConns:              cfg.MaxConns,
		MinConns:              cfg.MinConns,
		HealthCheckPeriod:     cfg.HealthCheckPeriod,
	}
}

// node returns an available NodeInfo. GetPool doesn't connect, so nothing needs to listen on port.
func node(id int, port string) NodeInfo {
	return NodeInfo{NodeId: id, Avail: true, ConnInfo: "host=127.0.0.1 port=" + port}
}

// newTestPool returns a pool that is closed when the test ends.
func newTestPool(t *testing.T, c *Connector, cfg DbPoolConfig) *Pool {
	p := NewPool(c, cfg)
	t.Cleanup(p.Close)
	return p
}

// The zero fields of DbPoolConfig keep pgxpool's defaults.
func TestDbPoolConfigKeepsDefaults(t *testing.T) {
	defaults, err := pgxpool.ParseConfig(node(1, "1").ConnInfo)
	if err != nil {
		t.Fatal(err)
	}
	partial := settings(defaults)
	partial.MaxConns = 2
	partial.HealthCheckPeriod = 5 * time.Second

	c := newTestConnector(t, quietOptions(), closedPort)
	for _, tc := range []struct {
		name string
		cfg  DbPoolConfig
		want DbPoolConfig
	}{
		{"empty", DbPoolConfig{}, settings(defaults)},
		{"partial", DbPoolConfig{MaxConns: 2, HealthCheckPeriod: 5 * time.Second}, partial},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool, err := newTestPool(t, c, tc.cfg).GetPool(node(1, "1"))
			if err != nil {
				t.Fatal(err)
			}
			if got := settings(pool.Config()); got != tc.want {
				t.Errorf("got %+v, want %+v", got, tc.want)
			}
		})
	}
}

// When a node's connection string changes in repmgr.nodes, GetPool replaces its pool.
func TestGetPoolReplacesChangedConnInfo(t *testing.T) {
	p := newTestPool(t, newTestConnector(t, quietOptions(), closedPort), DbPoolConfig{})
	old, err := p.GetPool(node(1, "1"))
	if err != nil {
		t.Fatal(err)
	}
	changed, err := p.GetPool(node(1, "2"))
	if err != nil {
		t.Fatal(err)
	}
	if changed == old || changed.Config().ConnConfig.Port != 2 {
		t.Errorf("GetPool kept the pool with the old connection string")
	}
	if again, _ := p.GetPool(node(1, "2")); again != changed {
		t.Errorf("GetPool didn't reuse the pool for an unchanged connection string")
	}
}

// GetPool closes the pools of the nodes that the connector removed, because they left the cluster.
func TestGetPoolClosesPoolsOfRemovedNodes(t *testing.T) {
	c := NewConnector(quietOptions(), closedPort)
	c.Close() // so that no check changes nodes below
	c.nodes = []NodeInfo{{NodeId: 1}, {NodeId: 2}}
	p := newTestPool(t, c, DbPoolConfig{})
	for _, n := range []NodeInfo{node(1, "1"), node(2, "2")} {
		if _, err := p.GetPool(n); err != nil {
			t.Fatal(err)
		}
	}
	// A check removed node 2
	c.nodes = []NodeInfo{{NodeId: 1}}
	c.removals++
	if _, err := p.GetPool(node(1, "1")); err != nil {
		t.Fatal(err)
	}
	if _, ok := p.pools[2]; ok {
		t.Errorf("the pool of the removed node 2 is still open")
	}
	if _, ok := p.pools[1]; !ok {
		t.Errorf("the pool of node 1 was closed")
	}
}

func TestPoolClose(t *testing.T) {
	p := NewPool(newTestConnector(t, quietOptions(), closedPort), DbPoolConfig{})
	if _, err := p.GetPool(node(1, "1")); err != nil {
		t.Fatal(err)
	}
	p.Close()
	p.Close() // closing twice is fine
	if _, err := p.GetPool(node(1, "1")); !errors.Is(err, ErrClosed) {
		t.Errorf("GetPool after Close: %v, want ErrClosed", err)
	}
}

// Without a primary, ConnectPrimary returns ErrNoPrimary, and waits for a check, so that a retry
// loop doesn't spin.
func TestConnectPrimaryWithoutPrimary(t *testing.T) {
	c := newTestConnector(t, quietOptions().WithRetryInterval(100*time.Millisecond), closedPort)
	p := newTestPool(t, c, DbPoolConfig{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, before := c.checkCounts()
	_, err := p.ConnectPrimary(ctx)
	if !errors.Is(err, ErrNoPrimary) || !IsNodeError(err) {
		t.Fatalf("ConnectPrimary without a primary: %v, want ErrNoPrimary", err)
	}
	if _, after := c.checkCounts(); after == before {
		t.Errorf("ConnectPrimary returned without waiting for a check")
	}
}
