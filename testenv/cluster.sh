#!/bin/bash
# Controls the three-node repmgr test cluster defined in compose.yaml. Run without arguments for usage.
set -euo pipefail
cd "$(dirname "$0")"

NODES=(node1 node2 node3)
declare -A IP=([node1]=172.28.0.11 [node2]=172.28.0.12 [node3]=172.28.0.13)

usage() {
    cat <<'EOF'
Usage: testenv/cluster.sh <command> [args]

  up [--build]             start the cluster and wait until both standbys stream; a running
                           cluster is kept as it is. Pass --build after changing the Dockerfile
                           or node-entrypoint.sh: it rebuilds the image and recreates all nodes.
  down                     stop the cluster and throw away all data
  show                     print each node's own copy of repmgr.nodes (what the connector reads)
  logs [node...]           follow the PostgreSQL and repmgrd logs
  psql <node> [args]       run psql on a node as postgres (database repmgr unless given)
  isolate <node>           cut <node> off from the other nodes; the host can still reach it
  heal                     undo all isolate commands
  repair                   make every node a streaming standby of the primary (the one on the
                           highest timeline), rejoining the nodes that don't stream within 15s
  delay <node> <ms>        add network delay to all traffic of <node>; 0 removes it
  stop <node>              stop the node cleanly (docker compose stop)
  kill <node>              kill the node's container (SIGKILL, like a crash)
  start <node>             start a stopped or killed node again
  rejoin <node> <primary>  turn a former primary into a standby of <primary> (uses pg_rewind)

Nodes: node1 172.28.0.11 (initial primary), node2 172.28.0.12, node3 172.28.0.13 (lower priority).
Credentials: repmgr/repmgr (database repmgr), app/app (database app, table writes).
EOF
}

dc() {
    docker compose "$@"
}

check_node() {
    if [ -z "${1:-}" ] || [ -z "${IP[$1]:-}" ]; then
        echo "Unknown node: '${1:-}'. Nodes: ${NODES[*]}" >&2
        exit 2
    fi
}

# Prints the nodes that run as primary, that is, not in recovery.
primaries() {
    local n
    for n in "${NODES[@]}"; do
        if [ "$(dc exec -T "$n" psql -XtA -d repmgr -c "select pg_is_in_recovery()" 2>/dev/null)" = f ]; then
            echo "$n"
        fi
    done
}

up() {
    # Builds the image only if it doesn't exist yet. Every rebuild gives a new image ID, even when
    # nothing changed, and docker compose would then recreate the nodes, losing their data.
    dc up -d "$@"
    echo "Waiting for the cluster..."
    local primary streaming registered repmgrd n
    for _ in $(seq 120); do
        primary=$(primaries)
        if [ "$(wc -w <<<"$primary")" -gt 1 ]; then
            echo "More than one node runs as primary:" $primary >&2
            echo "Fix it with '$0 repair', or start over with '$0 down' and '$0 up'." >&2
            exit 1
        fi
        streaming="" registered="" repmgrd=0
        if [ -n "$primary" ]; then
            streaming=$(dc exec -T "$primary" psql -XtA -d repmgr \
                -c "select count(*) from pg_stat_replication where state = 'streaming'" 2>/dev/null || true)
            registered=$(dc exec -T "$primary" psql -XtA -d repmgr \
                -c "select count(*) from repmgr.nodes where active" 2>/dev/null || true)
        fi
        for n in "${NODES[@]}"; do
            if dc exec -T "$n" bash -c 'kill -0 "$(cat /tmp/repmgrd.pid)"' 2>/dev/null; then
                repmgrd=$((repmgrd + 1))
            fi
        done
        if [ "$streaming" = 2 ] && [ "$registered" = 3 ] && [ "$repmgrd" = 3 ]; then
            show
            return
        fi
        sleep 1
    done
    echo "The cluster did not become healthy in 2 minutes. Try '$0 repair', or see '$0 logs'." >&2
    exit 1
}

# Prints the timeline of a node that runs as primary, and nothing for a standby.
primary_timeline() {
    dc exec -T "$1" psql -XtA -d repmgr -c "
        select ('x' || substr(pg_walfile_name(pg_current_wal_lsn()), 1, 8))::bit(32)::int
        where not pg_is_in_recovery()" 2>/dev/null || true
}

# Waits up to $3 seconds until node $2 streams from node $1.
wait_streaming() {
    for _ in $(seq "$3"); do
        if [ "$(dc exec -T "$1" psql -XtA -d repmgr -c "select count(*) from pg_stat_replication
                where client_addr = '${IP[$2]}' and state = 'streaming'" 2>/dev/null)" = 1 ]; then
            return 0
        fi
        sleep 1
    done
    return 1
}

repair() {
    local primary="" best=0 n tli
    for n in "${NODES[@]}"; do
        tli=$(primary_timeline "$n")
        if [ -n "$tli" ] && [ "$tli" -gt "$best" ]; then
            primary=$n best=$tli
        fi
    done
    if [ -z "$primary" ]; then
        echo "No node runs as primary" >&2
        exit 1
    fi
    echo "Primary: $primary (timeline $best)"
    for n in "${NODES[@]}"; do
        [ "$n" = "$primary" ] && continue
        # Give repmgrd a chance first, it may be following the new primary right now.
        if wait_streaming "$primary" "$n" 15; then
            echo "$n streams from $primary"
        else
            echo "Rejoining $n to $primary"
            rejoin "$n" "$primary"
        fi
    done
}

show() {
    local n
    for n in "${NODES[@]}"; do
        local header
        header=$(dc exec -T "$n" psql -XtA -d repmgr -c "
            select format('recovery=%s timeline=%s', pg_is_in_recovery(),
                case when pg_is_in_recovery() then (select received_tli from pg_stat_wal_receiver)::text
                     else ('x' || substr(pg_walfile_name(pg_current_wal_lsn()), 1, 8))::bit(32)::int::text
                end)" 2>/dev/null) || {
            echo "== $n ${IP[$n]}  not running"
            continue
        }
        local writes
        writes=$(dc exec -T "$n" psql -XtA -d app -c "select count(*) from writes" 2>/dev/null || echo "?")
        echo "== $n ${IP[$n]}  $header writes=$writes"
        dc exec -T "$n" psql -XtA -d repmgr -c "
            select format('   %s %-6s %-8s %s%s', node_id, node_name, type,
                case when active then 'active' else 'INACTIVE' end,
                coalesce(' upstream=' || upstream_node_id, ''))
            from repmgr.nodes order by node_id"
    done
}

isolate() {
    check_node "$1"
    local other
    # Reset rather than drop TCP connections. With DROP, a connection that was already open hangs
    # instead of failing: a repmgrd stuck on one misses the failover, and after heal the connection
    # comes back to life, so a standby can receive WAL from the old primary.
    for other in "${NODES[@]}"; do
        [ "$other" = "$1" ] && continue
        dc exec -T -u root "$1" iptables -A INPUT -s "${IP[$other]}" -p tcp -j REJECT --reject-with tcp-reset
        dc exec -T -u root "$1" iptables -A INPUT -s "${IP[$other]}" -j DROP
        dc exec -T -u root "$1" iptables -A OUTPUT -d "${IP[$other]}" -p tcp -j REJECT --reject-with tcp-reset
        dc exec -T -u root "$1" iptables -A OUTPUT -d "${IP[$other]}" -j DROP
    done
    echo "$1 is cut off from the other nodes"
}

heal() {
    local n
    for n in "${NODES[@]}"; do
        dc exec -T -u root "$n" iptables -F 2>/dev/null || true
    done
    echo "All partitions removed"
}

delay() {
    check_node "$1"
    if [ "$2" = 0 ]; then
        dc exec -T -u root "$1" tc qdisc del dev eth0 root 2>/dev/null || true
    else
        dc exec -T -u root "$1" tc qdisc replace dev eth0 root netem delay "${2}ms"
    fi
}

rejoin() {
    check_node "$1"
    check_node "$2"
    dc exec -T "$1" bash -ec "
        kill \"\$(cat /tmp/repmgrd.pid)\" 2>/dev/null || true
        pg_ctl -D \"\$PGDATA\" -m fast -w stop || true
        repmgr -f /var/lib/postgresql/repmgr.conf node rejoin --force-rewind \
            -d 'host=${IP[$2]} user=repmgr dbname=repmgr'
        repmgrd -f /var/lib/postgresql/repmgr.conf"
}

cmd=${1:-}
shift || true
case "$cmd" in
    up) up "$@" ;;
    down) dc down -v --remove-orphans ;;
    show) show ;;
    logs) dc logs -f "$@" ;;
    psql)
        check_node "${1:-}"
        node=$1
        shift
        if [ -t 0 ]; then
            dc exec "$node" psql -d repmgr "$@"
        else
            dc exec -T "$node" psql -d repmgr "$@"
        fi
        ;;
    isolate) isolate "${1:-}" ;;
    heal) heal ;;
    repair) repair ;;
    delay) delay "${1:-}" "${2:?delay in ms}" ;;
    stop | kill | start)
        check_node "${1:-}"
        dc "$cmd" "$1"
        ;;
    rejoin) rejoin "${1:-}" "${2:-}" ;;
    *)
        usage
        [ -z "$cmd" ] || exit 2
        ;;
esac
