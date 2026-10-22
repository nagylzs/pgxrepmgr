package pgxrepmgr

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/teivah/broadcast"
	"maps"
	"sync"
	"time"

	"log/slog"
)

// Connector monitors the status of a PostgreSQL repmgr cluster, and provides instant
// access to connection parameters for the primary (writable) and nearest (lowest latency) instances.
type Connector struct {
	mu                  sync.Mutex
	relay               *broadcast.Relay[struct{}]
	initial             []NodeInfo
	initialScanComplete bool

	options    Options
	nodes      []NodeInfo // the nodes found by the last check
	nearestIdx int        // index of the nearest available node (with lowest connect latency) or -1
	primaryIdx int        // index of the primary node (writable) or -1

	ctx            context.Context // canceled by Close
	cancel         context.CancelFunc
	done           chan struct{} // closed when run has returned
	refresh        chan struct{} // holds a token while a Refresh call waits for the next check to start
	closed         bool
	checksStarted  int
	checksFinished int
	removals       int // how many checks removed nodes from nodes (see Pool.GetPool)
}

// NewConnector monitors a PostgreSQL repmgr cluster, and provides access to its nodes. It discovers
// the nodes from the repmgr.nodes table of the seed nodes given in connStrings.
//
// It panics if the options are invalid (see the With methods of Options), if connStrings is empty, or
// if one of them is not a valid key=value connection string.
func NewConnector(opt Options, connStrings ...string) *Connector {
	if opt.CheckInterval < time.Duration(1)*time.Second {
		panic("CheckInterval should be at least one second")
	}
	if opt.ConnectTimeout < time.Duration(100)*time.Millisecond {
		panic("ConnectTimeout should be at least 100 msec")
	}
	if opt.FetchTimeout < time.Duration(100)*time.Millisecond {
		panic("FetchTimeout should be at least 100 msec")
	}
	if opt.RetryInterval < time.Duration(100)*time.Millisecond || opt.RetryInterval > opt.CheckInterval {
		panic("RetryInterval should be at least 100 msec, and at most CheckInterval")
	}
	if len(connStrings) == 0 {
		panic("at least one connStrings is required")
	}
	initial := make([]NodeInfo, 0)
	for _, d := range connStrings {
		if _, err := ParseConnString(d); err != nil {
			panic(fmt.Sprintf("invalid seed: %v", err))
		}
		initial = append(initial, NodeInfo{NodeId: InvalidNodeId, ConnInfo: d})
	}
	// The caller may change the maps later
	opt.CheckOverrides = maps.Clone(opt.CheckOverrides)
	opt.AppOverrides = maps.Clone(opt.AppOverrides)
	c := &Connector{
		mu:                  sync.Mutex{},
		relay:               broadcast.NewRelay[struct{}](),
		initial:             initial,
		initialScanComplete: false,

		options:    opt,
		nodes:      make([]NodeInfo, 0),
		nearestIdx: -1,
		primaryIdx: -1,

		done:    make(chan struct{}),
		refresh: make(chan struct{}, 1),
	}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	go c.run()
	return c
}

// Close stops the background checks, and waits until they have stopped. A check that is in progress
// is canceled. After Close, Primary and Nearest return Avail=false with LastError=ErrClosed,
// WaitForPrimary, WaitForNearest and Refresh return ErrClosed right away, and Nodes returns nil.
// Calling Close more than once is fine.
func (c *Connector) Close() {
	c.cancel()
	<-c.done
}

// Refresh starts a check now, and waits until it has finished or ctx is done. Call it after an error
// that suggests that the cluster has changed (see IsNodeError), so that Primary and Nearest are up to
// date when you retry.
//
// A check that is already running doesn't count, because it may have started before the error.
// Concurrent calls share one check, and checks are never started less than RetryInterval apart. The
// check is requested even if ctx is already done, so the caller doesn't have to wait for it.
func (c *Connector) Refresh(ctx context.Context) error {
	l := c.relay.Listener(1)
	defer l.Close()
	c.mu.Lock()
	target := c.checksStarted + 1
	select {
	case c.refresh <- struct{}{}:
	default: // the next check is requested already
	}
	c.mu.Unlock()
	for {
		c.mu.Lock()
		finished := c.checksFinished
		c.mu.Unlock()
		if finished >= target {
			return nil
		}
		select {
		case <-l.Ch():
		case <-ctx.Done():
			return ctx.Err()
		case <-c.done:
			return ErrClosed
		}
	}
}

// Logger returns the logger of the connector: Options.Logger, or slog.Default() if that is nil.
func (c *Connector) Logger() *slog.Logger {
	if c.options.Logger == nil {
		return slog.Default()
	}
	return c.options.Logger
}

// Nearest returns NodeInfo for the nearest node. (thread safe)
//
// If there is no nearest node available, then the result will have Avail=false, NodeId=InvalidNodeId
// and LastError=ErrNoNode (ErrClosed after Close).
func (c *Connector) Nearest() NodeInfo {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nearestNoLock()
}

func (c *Connector) nearestNoLock() NodeInfo {
	if c.closed {
		return NodeInfo{NodeId: InvalidNodeId, Avail: false, LastError: ErrClosed}
	} else if c.nearestIdx < 0 {
		return NodeInfo{NodeId: InvalidNodeId, Avail: false, LastError: ErrNoNode}
	} else {
		return c.forApp(c.nodes[c.nearestIdx])
	}
}

// Primary returns NodeInfo for the primary node (thread safe)
//
// If there is no primary node available, then the result will have Avail=false, NodeId=InvalidNodeId
// and LastError=ErrNoPrimary (ErrClosed after Close).
func (c *Connector) Primary() NodeInfo {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.primaryNoLock()
}

func (c *Connector) primaryNoLock() NodeInfo {
	if c.closed {
		return NodeInfo{NodeId: InvalidNodeId, Avail: false, LastError: ErrClosed}
	} else if c.primaryIdx < 0 {
		return NodeInfo{NodeId: InvalidNodeId, Avail: false, LastError: ErrNoPrimary}
	} else {
		return c.forApp(c.nodes[c.primaryIdx])
	}
}

// Nodes returns all nodes that the last check found, including the ones that are not available, with
// AppOverrides applied like for Primary and Nearest. After Close, it returns nil. (thread safe)
func (c *Connector) Nodes() []NodeInfo {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	nodes := make([]NodeInfo, 0, len(c.nodes))
	for _, n := range c.nodes {
		nodes = append(nodes, c.forApp(n))
	}
	return nodes
}

// forApp returns n with AppOverrides applied to its ConnInfo.
func (c *Connector) forApp(n NodeInfo) NodeInfo {
	connInfo, err := ReplaceInDsn(n.ConnInfo, c.options.AppOverrides)
	if err != nil {
		// Only if repmgr.nodes changed it since the connector connected with it
		n.Avail, n.LastError = false, err
		return n
	}
	n.ConnInfo = connInfo
	return n
}

// WaitForPrimary wait for the primary node to become available.
//
// If the context is done, then it will return a NodeInfo that has Avail=false, NodeId=InvalidNodeId
// and LastError=ctx.Err() (LastError=ErrClosed if the connector is closed).
func (c *Connector) WaitForPrimary(ctx context.Context) NodeInfo {
	l := c.relay.Listener(1)
	defer l.Close()
	for {
		n := c.Primary()
		if n.Avail {
			return n
		}
		// We did not find an available connection, wait until the next update, or the context is done.
		select {
		case <-l.Ch():
			continue
		case <-ctx.Done():
			return NodeInfo{NodeId: InvalidNodeId, Avail: false, LastError: ctx.Err()}
		case <-c.done:
			return NodeInfo{NodeId: InvalidNodeId, Avail: false, LastError: ErrClosed}
		}
	}
}

// WaitForNearest wait until a node becomes available.
//
// When multiple nodes are available, then it returns the one with the lowest latency.
// If the context is done, then it will return a NodeInfo that has Avail=false, NodeId=InvalidNodeId
// and LastError=ctx.Err() (LastError=ErrClosed if the connector is closed).
func (c *Connector) WaitForNearest(ctx context.Context) NodeInfo {
	l := c.relay.Listener(1)
	defer l.Close()
	for {
		n := c.Nearest()
		if n.Avail {
			return n
		}
		// We did not find an available connection, wait until the next update, or the context is done.
		select {
		case <-l.Ch():
			continue
		case <-ctx.Done():
			return NodeInfo{NodeId: InvalidNodeId, Avail: false, LastError: ctx.Err()}
		case <-c.done:
			return NodeInfo{NodeId: InvalidNodeId, Avail: false, LastError: ErrClosed}
		}
	}
}

// logCheckError logs an error of a check, unless Close has canceled the check.
func (c *Connector) logCheckError(ctx context.Context, msg string) {
	if ctx.Err() == nil {
		c.Logger().Error(msg)
	}
}

// checkNodeResult represents the information that can be retrieved from a single node
type checkNodeResult struct {
	Connected NodeInfo   // Info about the node that was actually connected to retrieve the statuses
	nodes     []NodeInfo // All nodes, queried from repmgr.nodes
}

// connect to a dsn, fetch repmgr.nodes and return a checkNodeResult
//
// You can set nodeid to InvalidNodeId if you only have a dsn.
func (c *Connector) checkNode(ctx context.Context, nodeId int, dsn string) (res checkNodeResult) {
	started := time.Now()
	res.Connected = NodeInfo{
		NodeId: nodeId, ConnInfo: dsn, IsPrimary: false, Avail: false,
		LastChecked: started, LastError: nil}
	res.nodes = make([]NodeInfo, 0)
	ctx2, cancel := context.WithTimeout(ctx, c.options.ConnectTimeout)
	defer cancel()
	// Try to connect to the node
	dsn2, err := ReplaceInDsn(dsn, c.options.CheckOverrides)
	if err != nil {
		res.Connected.LastError = err
		c.logCheckError(ctx, err.Error())
		return res
	}
	conn, err := pgx.Connect(ctx2, dsn2)
	if err != nil {
		res.Connected.LastError = err
		c.logCheckError(ctx, err.Error())
		return res
	}
	res.Connected.Latency = time.Since(started)

	defer func() {
		err := conn.Close(ctx)
		if err != nil {
			c.logCheckError(ctx, "could not close connection")
		}
	}()
	started = time.Now()
	ctx3, cancel := context.WithTimeout(ctx, c.options.FetchTimeout)
	defer cancel()
	// Only a node that is not in recovery can be the primary. If more than one node claims to be
	// the primary, the timeline tells which one was promoted last (see selectPrimary). The WAL
	// position (for a standby, up to where it has replayed) tells how far behind the primary it is.
	var inRecovery bool
	var walFile *string
	var lsn *int64
	err = conn.QueryRow(
		ctx3,
		"select pg_is_in_recovery(), "+
			"case when pg_is_in_recovery() then null else pg_walfile_name(pg_current_wal_lsn()) end, "+
			"pg_wal_lsn_diff(case when pg_is_in_recovery() then pg_last_wal_replay_lsn() "+
			"else pg_current_wal_lsn() end, '0/0')::bigint",
	).Scan(&inRecovery, &walFile, &lsn)
	if err != nil {
		res.Connected.LastError = err
		c.logCheckError(ctx, err.Error())
		return res
	}
	var timeline uint32
	if walFile != nil {
		// A WAL file name starts with its timeline ID as 8 hex digits.
		if _, err = fmt.Sscanf(*walFile, "%8x", &timeline); err != nil {
			res.Connected.LastError = fmt.Errorf("cannot parse WAL file name %q: %w", *walFile, err)
			c.logCheckError(ctx, res.Connected.LastError.Error())
			return res
		}
	}
	// Read all nodes before using any of them, so that a node is not available with a partial list
	rows, err := conn.Query(
		ctx3,
		"select node_id,upstream_node_id,active,node_name,type,location,"+
			"priority,conninfo,repluser,slot_name,config_file from repmgr.nodes order by active desc")
	var nodes []NodeInfo
	if err == nil {
		nodes, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (NodeInfo, error) {
			var node NodeInfo
			err := row.Scan(
				&node.NodeId, &node.UpstreamNodeId, &node.Active, &node.NodeName, &node.Type, &node.Location,
				&node.Priority, &node.ConnInfo, &node.ReplUser, &node.SlotName, &node.ConfigFile)
			return node, err
		})
	}
	if err != nil {
		res.Connected.LastError = err
		c.logCheckError(ctx, err.Error())
		return res
	}
	res.Connected.FetchDuration = time.Since(started)
	res.Connected.Avail = false // We could make a query, but we don't mark it as available until active==true
	hadPrimary := false
	for _, node := range nodes {
		node.IsPrimary = node.Type == "primary"
		node.Avail = false
		node.LastChecked = time.Now()
		if node.NodeId == res.Connected.NodeId && node.Active {
			node.Avail = true
			node.Latency = res.Connected.Latency
			node.InRecovery = inRecovery
			node.Timeline = int(timeline)
			if lsn != nil {
				node.lsn = *lsn
			}
			// A former primary that repmgr is turning into a standby keeps type=primary for a while.
			node.IsPrimary = node.IsPrimary && !inRecovery
			res.Connected = node
		}
		// After a failover, the old primary's row stays type=primary with active=false.
		if node.Type == "primary" && node.Active {
			if hadPrimary {
				c.Logger().Error("rogue primary", "node_id", node.NodeId)
			} else {
				hadPrimary = true
			}
		}
		res.nodes = append(res.nodes, node)
	}
	if !res.Connected.Avail {
		if nodeId == InvalidNodeId {
			res.Connected.LastError = errors.New("a seed is only used to discover the nodes in its repmgr.nodes")
		} else {
			res.Connected.LastError = errors.New("the node's own row in repmgr.nodes is missing or inactive")
		}
	}
	res.Connected.FetchDuration = time.Since(started)
	return res
}

func (c *Connector) collectInfos(ctx context.Context, src []NodeInfo) []checkNodeResult {
	// We will check them in parallel
	ci := make(chan checkNodeResult, len(src))
	wg := sync.WaitGroup{}
	wg.Add(len(src))
	for _, n := range src {
		nid, coni := n.NodeId, n.ConnInfo
		go func() {
			ci <- c.checkNode(ctx, nid, coni)
			wg.Done()
		}()
	}
	// close the channel when we got all infos
	go func() {
		wg.Wait()
		close(ci)
	}()
	// collect the results
	results := make([]checkNodeResult, 0)
	for di := range ci {
		results = append(results, di)
	}
	return results
}

// selectPrimary returns the index of the primary in nodes, or -1 if there is none.
//
// More than one node can claim to be the primary. After a failover the old primary can still be
// running, for example when it was cut off from the other nodes, and its own copy of repmgr.nodes
// still says it is the primary. Promotion always moves a node to a new, higher timeline, so the node
// on the highest timeline wins. The other nodes are marked unavailable: their data has diverged, so
// they must not be used for reading either.
func (c *Connector) selectPrimary(nodes []NodeInfo) int {
	primaryIdx := -1
	for idx, n := range nodes {
		if !n.Avail || !n.IsPrimary {
			continue
		}
		// The node ID only breaks a tie (two standbys promoted from the same timeline), so that the
		// choice doesn't depend on the order in which the checks finished.
		if primaryIdx < 0 || n.Timeline > nodes[primaryIdx].Timeline ||
			n.Timeline == nodes[primaryIdx].Timeline && n.NodeId < nodes[primaryIdx].NodeId {
			primaryIdx = idx
		}
	}
	if primaryIdx < 0 {
		return -1
	}
	primary := nodes[primaryIdx]
	for idx := range nodes {
		n := &nodes[idx]
		if idx == primaryIdx || !n.Avail || !n.IsPrimary {
			continue
		}
		n.Avail = false
		n.LastError = fmt.Errorf(
			"split brain: node %v (%v) on timeline %v claims to be primary, but node %v (%v) on timeline %v is the primary",
			n.NodeId, n.NodeName, n.Timeline, primary.NodeId, primary.NodeName, primary.Timeline)
		c.Logger().Error(n.LastError.Error())
	}
	return primaryIdx
}

// withoutRemoved returns nodes without the nodes that are missing from listed, the primary's copy
// of repmgr.nodes, because they were removed from the cluster. It also returns the primary's new
// index, and the removed nodes.
func withoutRemoved(nodes []NodeInfo, primaryIdx int, listed []NodeInfo) ([]NodeInfo, int, []NodeInfo) {
	inList := make(map[int]bool)
	for _, n := range listed {
		inList[n.NodeId] = true
	}
	primaryId := nodes[primaryIdx].NodeId
	kept, removed := make([]NodeInfo, 0, len(nodes)), make([]NodeInfo, 0)
	for _, n := range nodes {
		if !inList[n.NodeId] {
			removed = append(removed, n)
			continue
		}
		if n.NodeId == primaryId {
			primaryIdx = len(kept)
		}
		kept = append(kept, n)
	}
	return kept, primaryIdx, removed
}

// setLag sets LagBytes of the available nodes: how many bytes of WAL they have yet to replay, compared to
// the primary. It is -1 for the other nodes, and for all nodes if there is no primary. The positions
// were read at slightly different times, so a standby can seem to be ahead of the primary; its lag is 0.
func setLag(nodes []NodeInfo, primaryIdx int) {
	for i := range nodes {
		n := &nodes[i]
		n.LagBytes = -1
		if primaryIdx >= 0 && n.Avail && n.lsn != 0 && nodes[primaryIdx].lsn != 0 {
			n.LagBytes = max(0, nodes[primaryIdx].lsn-n.lsn)
		}
	}
}

// selectNearest returns the index of the available node with the lowest latency, or -1 if there is
// none. With maxLagBytes > 0, it skips the nodes that are further behind the primary, or whose lag is
// unknown. The primary itself is never behind. While there is no primary, all lags are unknown, so it
// doesn't skip any node.
func selectNearest(nodes []NodeInfo, primaryIdx int, maxLagBytes int64) int {
	nearestIdx := -1
	for idx, n := range nodes {
		if !n.Avail {
			continue
		}
		if maxLagBytes > 0 && primaryIdx >= 0 && (n.LagBytes < 0 || n.LagBytes > maxLagBytes) {
			continue
		}
		if nearestIdx < 0 || n.Latency < nodes[nearestIdx].Latency {
			nearestIdx = idx
		}
	}
	return nearestIdx
}

// run monitors the cluster until Close is called.
func (c *Connector) run() {
	defer func() {
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()
		close(c.done)
	}()
	for {
		started := time.Now()
		c.mu.Lock()
		c.checksStarted++
		// This check serves all Refresh calls made until now
		select {
		case <-c.refresh:
		default:
		}
		c.mu.Unlock()
		if !c.check() || !c.wait(started) {
			return
		}
	}
}

// wait waits until the next check is due, and returns false if the connector was closed. The next
// check is due CheckInterval after the previous one started, or RetryInterval after it while there
// is no primary. Refresh makes it due right away, but not earlier than RetryInterval after the
// previous one started.
func (c *Connector) wait(started time.Time) bool {
	c.mu.Lock()
	interval := c.options.CheckInterval
	if c.primaryIdx < 0 {
		interval = c.options.RetryInterval
	}
	c.mu.Unlock()
	timer := time.NewTimer(time.Until(started.Add(interval)))
	defer timer.Stop()
	select {
	case <-c.ctx.Done():
		return false
	case <-timer.C:
		return true
	case <-c.refresh:
	}
	spacing := time.NewTimer(time.Until(started.Add(c.options.RetryInterval)))
	defer spacing.Stop()
	select {
	case <-c.ctx.Done():
		return false
	case <-spacing.C:
		return true
	}
}

// check checks all known nodes once, and stores the results. It returns false if Close canceled it.
func (c *Connector) check() bool {
	ctx := c.ctx
	// Get connection config
	c.mu.Lock()
	initial, initialScanComplete, prevNodeInfos := c.initial, c.initialScanComplete, c.nodes
	c.mu.Unlock()
	// Go over dsns, extract infos
	var results []checkNodeResult
	if initialScanComplete {
		results = c.collectInfos(ctx, prevNodeInfos)
	} else {
		results = c.collectInfos(ctx, initial)
	}

	// Create a map of the results
	cMap := make(map[int]NodeInfo)
	for _, result := range results {
		i := result.Connected
		cMap[i.NodeId] = i
	}
	// Collect new nodes that appeared since last check
	cNew := make(map[int]NodeInfo)
	newNodes := make([]NodeInfo, 0)
	for _, result := range results {
		for _, seen := range result.nodes {
			_, exists := cMap[seen.NodeId]
			if !exists {
				_, exists = cNew[seen.NodeId]
			}
			if !exists {
				cNew[seen.NodeId] = seen
				newNodes = append(newNodes, seen)
				c.Logger().Info("New node discovered", "node", seen.String())
			}
		}
	}
	// New nodes appeared, collect infos from them too
	if len(newNodes) > 0 {
		if initialScanComplete {
			// initial scan completed, all results contain valid node ids
			results = append(results, c.collectInfos(ctx, newNodes)...)
		} else {
			// initial scan is not yet complete, we drop items with node ids and keep the real ones only
			results = c.collectInfos(ctx, newNodes)
		}
	}
	if ctx.Err() != nil {
		// Close canceled the check, so the results are not real
		return false
	}
	nodeInfos := make([]NodeInfo, 0, len(results))
	for _, result := range results {
		nodeInfos = append(nodeInfos, result.Connected)
	}
	// Select the primary first, because it marks the nodes that lost to it unavailable
	primaryIdx := c.selectPrimary(nodeInfos)
	var removed []NodeInfo
	if primaryIdx >= 0 {
		// nodeInfos[i] is results[i].Connected
		nodeInfos, primaryIdx, removed = withoutRemoved(nodeInfos, primaryIdx, results[primaryIdx].nodes)
		for _, n := range removed {
			c.Logger().Warn("Node removed from the cluster", "node", n.String())
		}
	}
	setLag(nodeInfos, primaryIdx)
	nearestIdx := selectNearest(nodeInfos, primaryIdx, c.options.MaxLagBytes)
	// Store modified config
	c.mu.Lock()
	prevPrimary, prevNearest := c.primaryNoLock(), c.nearestNoLock()
	c.nodes, c.nearestIdx, c.primaryIdx = nodeInfos, nearestIdx, primaryIdx
	primary, nearest := c.primaryNoLock(), c.nearestNoLock()
	if nearestIdx >= 0 {
		c.initialScanComplete = true
	}
	if len(removed) > 0 {
		c.removals++
	}
	c.checksFinished++
	c.mu.Unlock()
	// Notify waiters
	c.relay.Broadcast(struct{}{})

	if prevNearest.NodeId != nearest.NodeId {
		// Nearest can change anytime, this is just a debug message
		c.Logger().Debug("Nearest changed", "node", c.Nearest().String())
	}
	if prevPrimary.NodeId != primary.NodeId {
		// Primary change is serious, this is a warning
		c.Logger().Warn("Primary changed", "node", c.Primary().String())
	}
	return true
}

// removalCount returns how many checks have removed nodes so far.
func (c *Connector) removalCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.removals
}

// knows reports whether the connector checks the node with the given ID.
func (c *Connector) knows(nodeId int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, n := range c.nodes {
		if n.NodeId == nodeId {
			return true
		}
	}
	return false
}
