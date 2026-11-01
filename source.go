package pgxrepmgr

import (
	"context"
	"fmt"
)

// Node is a node of the cluster as a NodeSource lists it.
type Node struct {
	// ID identifies the node, and becomes NodeInfo.NodeID. It must be
	// unique, must not be InvalidNodeID, and must stay the same for the
	// node, also in other processes, because the lower ID wins a tie
	// between two primaries on the same timeline.
	ID int
	// Name becomes NodeInfo.NodeName.
	Name string
	// ConnInfo is the node's key=value connection string.
	ConnInfo string
	// Location becomes NodeInfo.Location. It is optional.
	Location string
}

// NodeSource lists the nodes of the cluster, for a Connector made with NewConnectorWithSource. Use it
// when the node list comes from another source than repmgr.nodes, such as a cluster manager.
type NodeSource interface {
	// Nodes returns the current list of nodes. The connector calls it at the start of every check,
	// with a context that has FetchTimeout. If it returns an error, the connector logs it, and keeps
	// checking the nodes of the last list that it could get.
	Nodes(ctx context.Context) ([]Node, error)
}

// NodeSourceFunc is a function that serves as a NodeSource.
type NodeSourceFunc func(ctx context.Context) ([]Node, error)

// Nodes calls f.
func (f NodeSourceFunc) Nodes(ctx context.Context) ([]Node, error) {
	return f(ctx)
}

// validateNodes returns an error if an ID in nodes is invalid or not unique. The error doesn't contain
// connection strings, because they may contain passwords.
func validateNodes(nodes []Node) error {
	seen := make(map[int]bool, len(nodes))
	for _, n := range nodes {
		if n.ID == InvalidNodeID {
			return fmt.Errorf("node %q has the ID InvalidNodeID", n.Name)
		}
		if seen[n.ID] {
			return fmt.Errorf("node ID %d is listed more than once", n.ID)
		}
		seen[n.ID] = true
	}
	return nil
}
