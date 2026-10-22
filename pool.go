package pgxrepmgr

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"sync"
	"time"
)

// DbPoolConfig default pool config see https://pkg.go.dev/github.com/jackc/pgx/v5/pgxpool#Config
//
// Fields that are zero (or negative) keep pgxpool's value: its default, or the one given in the
// connection string (pool_max_conns, pool_min_conns, pool_max_conn_lifetime, ...). The zero value is
// a valid config.
type DbPoolConfig struct {
	// MaxConnLifetime is the duration since creation after which a connection will be automatically closed.
	MaxConnLifetime time.Duration

	// MaxConnLifetimeJitter is the duration after MaxConnLifetime to randomly decide to close a connection.
	// This helps prevent all connections from being closed at the exact same time, starving the pool.
	MaxConnLifetimeJitter time.Duration

	// MaxConnIdleTime is the duration after which an idle connection will be automatically closed by the health check.
	MaxConnIdleTime time.Duration

	// MaxConns is the maximum size of the pool. The default is the greater of 4 or runtime.NumCPU().
	MaxConns int32

	// MinConns is the minimum size of the pool. After connection closes, the pool might dip below MinConns. A low
	// number of MinConns might mean the pool is empty after MaxConnLifetime until the health check has a chance
	// to create new connections.
	MinConns int32

	// HealthCheckPeriod is the duration between checks of the health of idle connections.
	HealthCheckPeriod time.Duration
}

// ApplyTo copies the positive fields to cfg, and leaves the others as they are in cfg.
//
// Zero is never useful for pgxpool here: MaxConns must be at least 1, HealthCheckPeriod 0 panics,
// MaxConnLifetime or MaxConnIdleTime 0 closes connections at every health check, and for MinConns
// and MaxConnLifetimeJitter zero is pgxpool's default anyway.
func (c DbPoolConfig) ApplyTo(cfg *pgxpool.Config) {
	if c.MinConns > 0 {
		cfg.MinConns = c.MinConns
	}
	if c.MaxConns > 0 {
		cfg.MaxConns = c.MaxConns
	}
	if c.MaxConnLifetime > 0 {
		cfg.MaxConnLifetime = c.MaxConnLifetime
	}
	if c.MaxConnLifetimeJitter > 0 {
		cfg.MaxConnLifetimeJitter = c.MaxConnLifetimeJitter
	}
	if c.HealthCheckPeriod > 0 {
		cfg.HealthCheckPeriod = c.HealthCheckPeriod
	}
	if c.MaxConnIdleTime > 0 {
		cfg.MaxConnIdleTime = c.MaxConnIdleTime
	}
}

// Pool keeps a pgxpool.Pool for each node of a Connector, and gives connections to the primary or the
// nearest node. It is safe for concurrent use.
type Pool struct {
	c        *Connector
	mu       sync.Mutex
	cfg      DbPoolConfig
	pools    map[int]nodePool
	removals int // the connector's removal count when the pools were last compared with its nodes
	closed   bool
}

// nodePool is the pool of a node, with the connection string it was created with
type nodePool struct {
	connInfo string
	pool     *pgxpool.Pool
}

// NewPool returns a Pool that creates the pools of the nodes with cfg, when they are first used.
func NewPool(connector *Connector, cfg DbPoolConfig) *Pool {
	return &Pool{c: connector, mu: sync.Mutex{}, cfg: cfg, pools: make(map[int]nodePool)}
}

// Close closes the pools of all nodes. Like pgxpool.Pool.Close, it waits until all acquired connections
// are released. After Close, GetPool returns ErrClosed. It doesn't close the Connector.
func (p *Pool) Close() {
	p.mu.Lock()
	pools := p.pools
	p.pools, p.closed = nil, true
	p.mu.Unlock()
	for _, np := range pools {
		np.pool.Close()
	}
}

// PrimaryNode returns the connector's primary node (see Connector.Primary).
func (p *Pool) PrimaryNode() NodeInfo {
	return p.c.Primary()
}

// NearestNode returns the connector's nearest node (see Connector.Nearest).
func (p *Pool) NearestNode() NodeInfo {
	return p.c.Nearest()
}

// NearestPool returns a cached pool for the (current) nearest node
func (p *Pool) NearestPool() (*pgxpool.Pool, error) {
	return p.GetPool(p.c.Nearest())
}

// PrimaryPool returns a cached pool for the (current) primary node
func (p *Pool) PrimaryPool() (*pgxpool.Pool, error) {
	return p.GetPool(p.c.Primary())
}

// GetPool returns a cached pool for the given node, which should come from the connector.
//
// If the node is not available, it returns the node's LastError, for example ErrNoPrimary for
// PrimaryPool during a failover. If the node's connection string has changed in repmgr.nodes, then
// GetPool replaces its pool. It also closes the pools of the nodes that were removed from the cluster.
// Connections acquired from a replaced or closed pool stay usable until they are released.
func (p *Pool) GetPool(nodeInfo NodeInfo) (*pgxpool.Pool, error) {
	if !nodeInfo.Avail {
		if nodeInfo.LastError != nil {
			return nil, nodeInfo.LastError
		}
		return nil, fmt.Errorf("pgxrepmgr: node %v is not available", nodeInfo.NodeId)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, ErrClosed
	}
	if removals := p.c.removalCount(); removals != p.removals {
		p.removals = removals
		for id, np := range p.pools {
			if !p.c.knows(id) {
				delete(p.pools, id)
				go np.pool.Close() // it waits until the acquired connections are released
			}
		}
	}
	np, ok := p.pools[nodeInfo.NodeId]
	if ok && np.connInfo != nodeInfo.ConnInfo {
		go np.pool.Close()
		ok = false
	}
	if !ok {
		cfg, err := pgxpool.ParseConfig(nodeInfo.ConnInfo)
		if err != nil {
			return nil, err
		}
		p.cfg.ApplyTo(cfg)
		pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
		if err != nil {
			return nil, err
		}
		np = nodePool{connInfo: nodeInfo.ConnInfo, pool: pool}
		p.pools[nodeInfo.NodeId] = np
	}
	return np.pool, nil
}

// refreshAfter refreshes the connector if err is a node error (see IsNodeError), so that a retry by the
// caller goes to the right node.
func (p *Pool) refreshAfter(ctx context.Context, err error) {
	if IsNodeError(err) {
		_ = p.c.Refresh(ctx) // if ctx is done, the check still happens, we just don't wait for it
	}
}

// ConnectPrimary returns a connection to the primary database, the caller **must** call Release() on it
//
// If connecting fails because of the node, or because there is no primary (see IsNodeError), then it
// refreshes the connector before returning the error, so that a retry goes to the new primary.
func (p *Pool) ConnectPrimary(ctx context.Context) (*pgxpool.Conn, error) {
	return p.acquire(ctx, p.c.Primary())
}

// ConnectNearest returns a connection to the nearest database, the caller **must** call Release() on it
//
// If connecting fails because of the node, or because no node is available (see IsNodeError), then it
// refreshes the connector before returning the error, so that a retry goes to another node.
func (p *Pool) ConnectNearest(ctx context.Context) (*pgxpool.Conn, error) {
	return p.acquire(ctx, p.c.Nearest())
}

// acquire returns a connection from the pool of node, and refreshes the connector after a node error.
func (p *Pool) acquire(ctx context.Context, node NodeInfo) (*pgxpool.Conn, error) {
	pool, err := p.GetPool(node)
	if err != nil {
		p.refreshAfter(ctx, err)
		return nil, err
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		p.refreshAfter(ctx, err)
		return nil, err
	}
	return conn, nil
}

// InTransaction runs fn in a transaction on the primary (see the InTransaction function).
//
// If it fails because of the node, or because there is no primary (see IsNodeError), then it refreshes
// the connector before returning the error, so that a retry goes to the new primary. It doesn't retry
// by itself, because only the caller knows whether fn is safe to run again.
func (p *Pool) InTransaction(ctx context.Context, fn func(conn *pgxpool.Conn, tx pgx.Tx) error) error {
	pool, err := p.PrimaryPool()
	if err == nil {
		err = InTransaction(ctx, pool, fn)
	}
	p.refreshAfter(ctx, err)
	return err
}
