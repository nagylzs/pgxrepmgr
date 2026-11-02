package pgxrepmgr

import (
	"fmt"
	"math"
	"time"
)

// InvalidNodeID is the NodeID of a seed, which the connector only uses to discover the nodes, and of
// the NodeInfo that Primary, Nearest and the WaitFor methods return when there is no node.
const InvalidNodeID = math.MinInt32

// NodeInfo is a node of the cluster: its row in repmgr.nodes (or what a NodeSource listed), and what
// the connector found when it last checked the node.
type NodeInfo struct {
	// The node's own row in repmgr.nodes. With a NodeSource, only NodeID,
	// NodeName, Location and ConnInfo are set, and Active is true.
	NodeID         int
	UpstreamNodeID *int
	Active         bool
	NodeName       string
	Type           string
	Location       string
	Priority       int
	ConnInfo       string
	ReplUser       string
	SlotName       string
	ConfigFile     string

	// The node claims to be the primary: it is writable, and its own row
	// says so (with a NodeSource: it is writable)
	IsPrimary bool
	// The node can be used: it answered, and its own row in repmgr.nodes
	// is active (with a NodeSource: it answered, with the Node's SystemID
	// if that is set)
	Avail bool
	// The node is in recovery (a standby)
	InRecovery bool
	// The timeline the node writes WAL on (0 when InRecovery)
	Timeline int
	// Bytes of WAL the node had yet to replay, compared to the primary
	// (0 for the primary, -1 if unknown)
	LagBytes int64
	// How long connecting took at the last check (0 if it failed)
	Latency time.Duration
	// The time it took to fetch the node's state and repmgr.nodes
	FetchDuration time.Duration
	// Time of the last check
	LastChecked time.Time
	// Why the node is not available (nil when Avail)
	LastError error

	// The WAL position in bytes (for a standby, up to where it has
	// replayed), 0 if unknown
	lsn int64
}

// String returns the node's ID, name, location, role, availability and latency, for example to log it.
func (n NodeInfo) String() string {
	return fmt.Sprintf("ID=%v Name=%v Location=%v IsPrimary=%v Avail=%v Latency=%v",
		n.NodeID, n.NodeName, n.Location, n.IsPrimary, n.Avail, n.Latency.String())
}
