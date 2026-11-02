package pgxrepmgr

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/teivah/broadcast"
	"maps"
	"slices"
	"sync"
	"time"

	"log/slog"
)

// Connector monitors a PostgreSQL repmgr cluster in the background, and returns the primary node
// (the writable one) and the nearest node (the one with the lowest connect latency) without waiting.
// It is safe for concurrent use.
type Connector struct {
	mu                  sync.Mutex
	relay               *broadcast.Relay[struct{}]
	initial             []NodeInfo
	initialScanComplete bool
	source              NodeSource // nil: discover the nodes from repmgr.nodes
	listed              []Node     // the last list that source returned, used only by run

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
	validate(opt)
	if len(connStrings) == 0 {
		panic("at least one connStrings is required")
	}
	initial := make([]NodeInfo, 0)
	for _, d := range connStrings {
		if _, err := ParseDSN(d); err != nil {
			panic(fmt.Sprintf("invalid seed: %v", err))
		}
		initial = append(initial, NodeInfo{NodeID: InvalidNodeID, ConnInfo: d})
	}
	return start(opt, initial, nil)
}

// NewConnectorWithSource monitors the nodes that src lists, rather than the ones in repmgr.nodes.
// The nodes don't need repmgr: a node is available if the connector can connect to it and read its
// state, and the primary is the node that is not in recovery. If more than one node is not in
// recovery, the one on the highest timeline is the primary, like with repmgr.nodes.
//
// The connector checks the nodes in the database of the check overrides, which is repmgr after
// NewOptions. Use Options.WithoutCheckOverride("dbname") to check them in the database of their
// connection strings instead, or WithCheckOverride to set another one.
//
// The connector gets the list from src at the start of every check. After the list changes, call
// Refresh to use the new list without waiting for the next check. A node that is no longer listed is
// removed right away, and Pool closes its pool.
//
// It panics if the options are invalid (see the With methods of Options), or if src is nil.
func NewConnectorWithSource(opt Options, src NodeSource) *Connector {
	validate(opt)
	if src == nil {
		panic("src must not be nil")
	}
	return start(opt, nil, src)
}

// validate panics if opt is invalid.
func validate(opt Options) {
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
}

// start returns a connector that checks the seeds in initial, or the nodes that src lists, in the
// background.
func start(opt Options, initial []NodeInfo, src NodeSource) *Connector {
	// The caller may change the maps later
	opt.CheckOverrides = maps.Clone(opt.CheckOverrides)
	opt.AppOverrides = maps.Clone(opt.AppOverrides)
	c := &Connector{
		mu:                  sync.Mutex{},
		relay:               broadcast.NewRelay[struct{}](),
		initial:             initial,
		initialScanComplete: false,
		source:              src,

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

// Nearest returns the nearest available node: the one with the lowest connect latency, which may be
// the primary or a standby (see Options.WithMaxLagBytes).
//
// If no node is available, the result has Avail=false, NodeID=InvalidNodeID and
// LastError=ErrNoNode (ErrClosed after Close).
func (c *Connector) Nearest() NodeInfo {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nearestNoLock()
}

func (c *Connector) nearestNoLock() NodeInfo {
	if c.closed {
		return NodeInfo{NodeID: InvalidNodeID, Avail: false, LastError: ErrClosed}
	} else if c.nearestIdx < 0 {
		return NodeInfo{NodeID: InvalidNodeID, Avail: false, LastError: ErrNoNode}
	} else {
		return c.forApp(c.nodes[c.nearestIdx])
	}
}

// Primary returns the primary node.
//
// If there is no primary, the result has Avail=false, NodeID=InvalidNodeID and
// LastError=ErrNoPrimary (ErrClosed after Close).
func (c *Connector) Primary() NodeInfo {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.primaryNoLock()
}

func (c *Connector) primaryNoLock() NodeInfo {
	if c.closed {
		return NodeInfo{NodeID: InvalidNodeID, Avail: false, LastError: ErrClosed}
	} else if c.primaryIdx < 0 {
		return NodeInfo{NodeID: InvalidNodeID, Avail: false, LastError: ErrNoPrimary}
	} else {
		return c.forApp(c.nodes[c.primaryIdx])
	}
}

// Nodes returns all nodes that the last check found, including the ones that are not available, with
// AppOverrides applied like for Primary and Nearest. After Close, it returns nil.
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
	connInfo, err := ReplaceInDSN(n.ConnInfo, c.options.AppOverrides)
	if err != nil {
		// Only if repmgr.nodes changed it since the connector connected with it
		n.Avail, n.LastError = false, err
		return n
	}
	n.ConnInfo = connInfo
	return n
}

// WaitForPrimary waits until there is a primary, and returns it.
//
// If ctx is done first, the result has Avail=false, NodeID=InvalidNodeID and LastError=ctx.Err()
// (ErrClosed if the connector is closed).
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
			return NodeInfo{NodeID: InvalidNodeID, Avail: false, LastError: ctx.Err()}
		case <-c.done:
			return NodeInfo{NodeID: InvalidNodeID, Avail: false, LastError: ErrClosed}
		}
	}
}

// WaitForNearest waits until a node is available, and returns the nearest one (see Nearest).
//
// If ctx is done first, the result has Avail=false, NodeID=InvalidNodeID and LastError=ctx.Err()
// (ErrClosed if the connector is closed).
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
			return NodeInfo{NodeID: InvalidNodeID, Avail: false, LastError: ctx.Err()}
		case <-c.done:
			return NodeInfo{NodeID: InvalidNodeID, Avail: false, LastError: ErrClosed}
		}
	}
}

// logCheckError logs an error of a check, unless Close has canceled the check.
func (c *Connector) logCheckError(ctx context.Context, msg string) {
	if ctx.Err() == nil {
		c.Logger().Error(msg)
	}
}

// checkNodeResult is what checkNode found out from a single node.
type checkNodeResult struct {
	Connected NodeInfo   // Info about the node that was actually connected to retrieve the statuses
	nodes     []NodeInfo // All nodes, queried from repmgr.nodes
}

// checkNode connects to dsn, and reads the node's state and repmgr.nodes. nodeID is InvalidNodeID
// for a seed.
func (c *Connector) checkNode(ctx context.Context, nodeID int, dsn string) (res checkNodeResult) {
	started := time.Now()
	res.Connected = NodeInfo{
		NodeID: nodeID, ConnInfo: dsn, IsPrimary: false, Avail: false,
		LastChecked: started, LastError: nil}
	res.nodes = make([]NodeInfo, 0)
	conn, latency, err := c.connect(ctx, dsn)
	if err != nil {
		res.Connected.LastError = err
		c.logCheckError(ctx, err.Error())
		return res
	}
	res.Connected.Latency = latency
	defer c.closeConn(ctx, conn)

	started = time.Now()
	ctx3, cancel := context.WithTimeout(ctx, c.options.FetchTimeout)
	defer cancel()
	state, err := queryState(ctx3, conn, false)
	if err != nil {
		res.Connected.LastError = err
		c.logCheckError(ctx, err.Error())
		return res
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
				&node.NodeID, &node.UpstreamNodeID, &node.Active, &node.NodeName, &node.Type, &node.Location,
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
		if node.NodeID == res.Connected.NodeID && node.Active {
			node.Avail = true
			node.Latency = res.Connected.Latency
			node.InRecovery = state.inRecovery
			node.Timeline = state.timeline
			node.lsn = state.lsn
			// A former primary that repmgr is turning into a standby keeps type=primary for a while.
			node.IsPrimary = node.IsPrimary && !state.inRecovery
			res.Connected = node
		}
		// After a failover, the old primary's row stays type=primary with active=false.
		if node.Type == "primary" && node.Active {
			if hadPrimary {
				c.Logger().Error("rogue primary", "node_id", node.NodeID)
			} else {
				hadPrimary = true
			}
		}
		res.nodes = append(res.nodes, node)
	}
	if !res.Connected.Avail {
		if nodeID == InvalidNodeID {
			res.Connected.LastError = errors.New("a seed is only used to discover the nodes in its repmgr.nodes")
		} else {
			res.Connected.LastError = errors.New("the node's own row in repmgr.nodes is missing or inactive")
		}
	}
	res.Connected.FetchDuration = time.Since(started)
	return res
}

// connect connects to the node at dsn with the check overrides applied, and returns how long it took.
func (c *Connector) connect(ctx context.Context, dsn string) (*pgx.Conn, time.Duration, error) {
	started := time.Now()
	dsn2, err := ReplaceInDSN(dsn, c.options.CheckOverrides)
	if err != nil {
		return nil, 0, err
	}
	ctx2, cancel := context.WithTimeout(ctx, c.options.ConnectTimeout)
	defer cancel()
	conn, err := pgx.Connect(ctx2, dsn2)
	if err != nil {
		return nil, 0, err
	}
	return conn, time.Since(started), nil
}

// closeConn closes a connection that a check opened.
func (c *Connector) closeConn(ctx context.Context, conn *pgx.Conn) {
	if err := conn.Close(ctx); err != nil {
		c.logCheckError(ctx, "could not close connection")
	}
}

// nodeState is what a node says about itself.
type nodeState struct {
	inRecovery bool
	timeline   int   // the timeline the node writes WAL on, 0 in recovery
	lsn        int64 // the WAL position in bytes (for a standby, up to where it has replayed), 0 if unknown
	systemID   int64 // the system identifier, 0 if it wasn't read
}

// queryState reads the state of the node on conn. With withSystemID, it also reads the node's system
// identifier.
func queryState(ctx context.Context, conn *pgx.Conn, withSystemID bool) (nodeState, error) {
	// Only a node that is not in recovery can be the primary. If more than one node claims to be
	// the primary, the timeline tells which one was promoted last (see selectPrimary). The WAL
	// position (for a standby, up to where it has replayed) tells how far behind the primary it is.
	var state nodeState
	var walFile *string
	var lsn *int64
	query := "select pg_is_in_recovery(), " +
		"case when pg_is_in_recovery() then null else pg_walfile_name(pg_current_wal_lsn()) end, " +
		"pg_wal_lsn_diff(case when pg_is_in_recovery() then pg_last_wal_replay_lsn() " +
		"else pg_current_wal_lsn() end, '0/0')::bigint"
	dest := []any{&state.inRecovery, &walFile, &lsn}
	if withSystemID {
		query += ", (pg_control_system()).system_identifier"
		dest = append(dest, &state.systemID)
	}
	if err := conn.QueryRow(ctx, query).Scan(dest...); err != nil {
		return nodeState{}, err
	}
	if walFile != nil {
		// A WAL file name starts with its timeline ID as 8 hex digits.
		var timeline uint32
		if _, err := fmt.Sscanf(*walFile, "%8x", &timeline); err != nil {
			return nodeState{}, fmt.Errorf("cannot parse WAL file name %q: %w", *walFile, err)
		}
		state.timeline = int(timeline)
	}
	if lsn != nil {
		state.lsn = *lsn
	}
	return state, nil
}

// checkListedNode checks a node that the source listed. The node is available if the connector can
// connect to it and read its state, and its system identifier is node.SystemID (when that is set). It
// is the primary if it is not in recovery.
func (c *Connector) checkListedNode(ctx context.Context, node Node) NodeInfo {
	n := NodeInfo{
		NodeID: node.ID, NodeName: node.Name, Location: node.Location, ConnInfo: node.ConnInfo,
		Active: true, LastChecked: time.Now()}
	conn, latency, err := c.connect(ctx, n.ConnInfo)
	if err != nil {
		n.LastError = err
		c.logCheckError(ctx, err.Error())
		return n
	}
	n.Latency = latency
	defer c.closeConn(ctx, conn)

	started := time.Now()
	ctx3, cancel := context.WithTimeout(ctx, c.options.FetchTimeout)
	defer cancel()
	state, err := queryState(ctx3, conn, node.SystemID != 0)
	if err == nil && state.systemID != node.SystemID {
		err = fmt.Errorf("%w: node %v (%v) has the system identifier %v, not %v",
			ErrSystemIDMismatch, n.NodeID, n.NodeName, state.systemID, node.SystemID)
	}
	if err != nil {
		n.LastError = err
		c.logCheckError(ctx, err.Error())
		return n
	}
	n.FetchDuration = time.Since(started)
	n.InRecovery, n.Timeline, n.lsn = state.inRecovery, state.timeline, state.lsn
	n.IsPrimary = !state.inRecovery
	n.Avail = true
	return n
}

// checkListedNodes checks nodes in parallel, and returns the results in the same order.
func (c *Connector) checkListedNodes(ctx context.Context, nodes []Node) []NodeInfo {
	results := make([]NodeInfo, len(nodes))
	var wg sync.WaitGroup
	for i, n := range nodes {
		wg.Go(func() { results[i] = c.checkListedNode(ctx, n) })
	}
	wg.Wait()
	return results
}

func (c *Connector) collectInfos(ctx context.Context, src []NodeInfo) []checkNodeResult {
	// We will check them in parallel
	ci := make(chan checkNodeResult, len(src))
	wg := sync.WaitGroup{}
	wg.Add(len(src))
	for _, n := range src {
		nid, coni := n.NodeID, n.ConnInfo
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
			n.Timeline == nodes[primaryIdx].Timeline && n.NodeID < nodes[primaryIdx].NodeID {
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
			n.NodeID, n.NodeName, n.Timeline, primary.NodeID, primary.NodeName, primary.Timeline)
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
		inList[n.NodeID] = true
	}
	primaryID := nodes[primaryIdx].NodeID
	kept, removed := make([]NodeInfo, 0, len(nodes)), make([]NodeInfo, 0)
	for _, n := range nodes {
		if !inList[n.NodeID] {
			removed = append(removed, n)
			continue
		}
		if n.NodeID == primaryID {
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

// checkRepmgr checks the seeds or the known nodes, and the nodes that their repmgr.nodes lists. It
// returns the results, the index of the primary in them, and the nodes that the primary's repmgr.nodes
// no longer lists. ok is false if Close canceled the check.
func (c *Connector) checkRepmgr() (nodeInfos []NodeInfo, primaryIdx int, removed []NodeInfo, ok bool) {
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
		cMap[i.NodeID] = i
	}
	// Collect new nodes that appeared since last check
	cNew := make(map[int]NodeInfo)
	newNodes := make([]NodeInfo, 0)
	for _, result := range results {
		for _, seen := range result.nodes {
			_, exists := cMap[seen.NodeID]
			if !exists {
				_, exists = cNew[seen.NodeID]
			}
			if !exists {
				cNew[seen.NodeID] = seen
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
		return nil, -1, nil, false
	}
	nodeInfos = make([]NodeInfo, 0, len(results))
	for _, result := range results {
		nodeInfos = append(nodeInfos, result.Connected)
	}
	// Select the primary first, because it marks the nodes that lost to it unavailable
	primaryIdx = c.selectPrimary(nodeInfos)
	if primaryIdx >= 0 {
		// nodeInfos[i] is results[i].Connected
		nodeInfos, primaryIdx, removed = withoutRemoved(nodeInfos, primaryIdx, results[primaryIdx].nodes)
	}
	return nodeInfos, primaryIdx, removed, true
}

// checkListed checks the nodes that the source lists. It returns the results, the index of the
// primary in them, and the known nodes that are no longer listed. ok is false if Close canceled the
// check.
func (c *Connector) checkListed() (nodeInfos []NodeInfo, primaryIdx int, removed []NodeInfo, ok bool) {
	ctx := c.ctx
	list := c.sourceList(ctx)
	listed := make(map[int]bool, len(list))
	for _, n := range list {
		listed[n.ID] = true
	}
	c.mu.Lock()
	for _, n := range c.nodes {
		if !listed[n.NodeID] {
			removed = append(removed, n)
		}
	}
	c.mu.Unlock()
	nodeInfos = c.checkListedNodes(ctx, list)
	if ctx.Err() != nil {
		return nil, -1, nil, false
	}
	return nodeInfos, c.selectPrimary(nodeInfos), removed, true
}

// sourceList returns the node list from the source. If the source fails or returns an invalid list,
// it logs the error, and returns the last valid list.
func (c *Connector) sourceList(ctx context.Context) []Node {
	ctx2, cancel := context.WithTimeout(ctx, c.options.FetchTimeout)
	defer cancel()
	list, err := c.source.Nodes(ctx2)
	if err == nil {
		err = validateNodes(list)
	}
	if err != nil {
		c.logCheckError(ctx, fmt.Sprintf("cannot get the node list, using the previous one: %v", err))
		return c.listed
	}
	c.listed = slices.Clone(list)
	return c.listed
}

// check checks all known nodes once, and stores the results. It returns false if Close canceled it.
func (c *Connector) check() bool {
	var nodeInfos, removed []NodeInfo
	var primaryIdx int
	var ok bool
	if c.source != nil {
		nodeInfos, primaryIdx, removed, ok = c.checkListed()
	} else {
		nodeInfos, primaryIdx, removed, ok = c.checkRepmgr()
	}
	if !ok {
		// Close canceled the check, so the results are not real
		return false
	}
	for _, n := range removed {
		c.Logger().Warn("Node removed from the cluster", "node", n.String())
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

	if prevNearest.NodeID != nearest.NodeID {
		// Nearest can change anytime, this is just a debug message
		c.Logger().Debug("Nearest changed", "node", c.Nearest().String())
	}
	if prevPrimary.NodeID != primary.NodeID {
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
func (c *Connector) knows(nodeID int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, n := range c.nodes {
		if n.NodeID == nodeID {
			return true
		}
	}
	return false
}
