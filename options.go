package pgxrepmgr

import (
	"log/slog"
	"maps"
	"time"
)

// Options configure a Connector. Create them with NewOptions, and change them with the With methods.
// The With methods copy the override maps, so changing a copy of Options doesn't change the others.
type Options struct {
	CheckOverrides map[string]string // override connection string before checking repmgr.nodes
	AppOverrides   map[string]string // override connection string before returning Primary() or Nearest()

	CheckInterval  time.Duration // interval between cluster status checks
	RetryInterval  time.Duration // interval between checks while there is no primary, and the least time between checks started by Refresh
	ConnectTimeout time.Duration // timeout for connecting a node
	FetchTimeout   time.Duration // timeout for fetching cluster status from a node
	MaxLagBytes    int64         // Nearest skips the standbys that are more bytes of WAL behind the primary (0: no limit)

	Logger *slog.Logger // nil means slog.Default()
}

// NewOptions returns the default options for a Connector that checks the nodes as repmgrUser, in the
// repmgr database. The user only needs to read repmgr.nodes.
func NewOptions(repmgrUser string, repmgrPassword string) Options {
	co := make(map[string]string)
	co["user"] = repmgrUser
	co["password"] = repmgrPassword
	co["dbname"] = "repmgr"

	ao := make(map[string]string)

	return Options{
		CheckOverrides: co,
		AppOverrides:   ao,
		CheckInterval:  time.Duration(30) * time.Second,
		RetryInterval:  time.Second,
		ConnectTimeout: time.Duration(5) * time.Second,
		FetchTimeout:   time.Duration(5) * time.Second,
	}
}

// withEntry returns a copy of m with key set to value, so that the Options that m belongs to don't change.
func withEntry(m map[string]string, key string, value string) map[string]string {
	m = maps.Clone(m)
	if m == nil {
		m = make(map[string]string)
	}
	m[key] = value
	return m
}

// WithCheckOverride sets key to value in the connection strings that the connector checks the nodes with.
func (o Options) WithCheckOverride(key string, value string) Options {
	o.CheckOverrides = withEntry(o.CheckOverrides, key, value)
	return o
}

// WithAppOverride sets key to value in the connection strings of the nodes that Primary, Nearest and
// Nodes return, and so also in the ones that Pool connects with.
func (o Options) WithAppOverride(key string, value string) Options {
	o.AppOverrides = withEntry(o.AppOverrides, key, value)
	return o
}

// WithConnectTimeout sets the timeout for connecting to a node when checking it. It must be at least 100 ms.
func (o Options) WithConnectTimeout(t time.Duration) Options {
	o.ConnectTimeout = t
	return o
}

// WithFetchTimeout sets the timeout for reading a node's state and repmgr.nodes. It must be at least 100 ms.
func (o Options) WithFetchTimeout(t time.Duration) Options {
	o.FetchTimeout = t
	return o
}

// WithCheckInterval sets how often the connector checks the cluster. It must be at least one second.
func (o Options) WithCheckInterval(t time.Duration) Options {
	o.CheckInterval = t
	return o
}

// WithRetryInterval sets how often the connector checks the cluster while there is no primary, and
// how close together Refresh can start checks. It must be at least 100 ms, and at most CheckInterval.
func (o Options) WithRetryInterval(t time.Duration) Options {
	o.RetryInterval = t
	return o
}

// WithMaxLagBytes makes Nearest skip the standbys that are more than n bytes of WAL behind the primary,
// as measured at the last check. The primary itself is never behind, so Nearest falls back to it.
// While there is no primary, the lag is unknown, and Nearest doesn't skip any node. 0 means no limit.
func (o Options) WithMaxLagBytes(n int64) Options {
	o.MaxLagBytes = n
	return o
}

// WithLogger sets the logger of the connector. Without it, the connector logs through slog.Default().
func (o Options) WithLogger(l *slog.Logger) Options {
	o.Logger = l
	return o
}
