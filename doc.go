// Package pgxrepmgr discovers and monitors a PostgreSQL repmgr cluster, and tells which node is the
// primary and which one is the nearest (the one with the lowest connect latency).
//
// A Connector reads the nodes from repmgr.nodes, starting from one or more seed DSNs, and checks
// them in the background. Primary and Nearest return what the last check found. After a failover,
// the primary is the node on the highest timeline, so an old primary that still claims to be the
// primary is ignored. A Pool keeps a pgxpool.Pool for each node, and gives connections to the
// primary or the nearest node.
//
// Only key=value connection strings (DSNs) are supported, not postgres:// URLs.
package pgxrepmgr
