package pgxrepmgr

import (
	"fmt"
	"math"
	"time"
)

// InvalidNodeId is the NodeId of a seed, which the connector only uses to discover the nodes, and of
// the NodeInfo that Primary, Nearest and the WaitFor methods return when there is no node.
const InvalidNodeId = math.MinInt32

// NodeInfo represents the status of a PostgreSQL repmgr node
//
// it basically holds all values from repmgr.repmgr.nodes with some extensions
type NodeInfo struct {
	NodeId         int
	UpstreamNodeId *int
	Active         bool
	NodeName       string
	Type           string
	Location       string
	Priority       int
	ConnInfo       string
	ReplUser       string
	SlotName       string
	ConfigFile     string

	IsPrimary     bool          // the node claims to be the primary: its own row says so, and it is writable
	Avail         bool          // the node can be used: it answered, and its own row in repmgr.nodes is active
	InRecovery    bool          // the node is in recovery (a standby)
	Timeline      int           // the timeline the node writes WAL on (0 when InRecovery)
	LagBytes      int64         // bytes of WAL the node had yet to replay, compared to the primary (0 for the primary, -1 if unknown)
	Latency       time.Duration // how long connecting took at the last check (0 if it failed)
	FetchDuration time.Duration // the time it took to fetch the node's state and repmgr.nodes
	LastChecked   time.Time     // time of the last check
	LastError     error         // why the node is not available (nil when Avail)

	lsn int64 // the WAL position in bytes (for a standby, up to where it has replayed), 0 if unknown
}

func (n NodeInfo) String() string {
	return fmt.Sprintf("Id=%v Name=%v Location=%v IsPrimary=%v Avail=%v Latency=%v",
		n.NodeId, n.NodeName, n.Location, n.IsPrimary, n.Avail, n.Latency.String())
}
