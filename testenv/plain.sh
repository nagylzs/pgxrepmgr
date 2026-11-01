#!/bin/bash
# Controls the three-node test cluster without repmgr defined in plain.yaml. Run without arguments for
# usage.
set -euo pipefail
cd "$(dirname "$0")"

NODES=(plain1 plain2 plain3)

usage() {
    cat <<'EOF'
Usage: testenv/plain.sh <command> [args]

  up              start a new cluster, throwing away the old one, and wait until both standbys
                  stream from plain1
  down            stop the cluster and throw away all data
  show            print each node's recovery state, timeline and write count
  promote <node>  promote <node> with pg_promote(), whether or not the primary still runs
  logs [node...]  follow the PostgreSQL logs

Nodes: plain1 172.28.1.11 (the primary after up), plain2 172.28.1.12, plain3 172.28.1.13.
Credentials: app/app (database app, table writes), replicator/replicator (replication only).
EOF
}

dc() {
    docker compose -f plain.yaml "$@"
}

check_node() {
    if [ -z "${1:-}" ] || ! printf '%s\n' "${NODES[@]}" | grep -qx "$1"; then
        echo "Unknown node: '${1:-}'. Nodes: ${NODES[*]}" >&2
        exit 2
    fi
}

up() {
    # Without repmgr, there is nothing to repair a cluster after a test, so every test starts anew.
    dc down -v --remove-orphans
    dc up -d
    echo "Waiting for the cluster..."
    for _ in $(seq 120); do
        if [ "$(dc exec -T plain1 psql -XtA \
            -c "select count(*) from pg_stat_replication where state = 'streaming'" 2>/dev/null)" = 2 ]; then
            show
            return
        fi
        sleep 1
    done
    echo "The cluster did not become healthy in 2 minutes, see '$0 logs'." >&2
    exit 1
}

show() {
    local n state writes
    for n in "${NODES[@]}"; do
        state=$(dc exec -T "$n" psql -XtA -c "
            select format('recovery=%s timeline=%s', pg_is_in_recovery(),
                case when pg_is_in_recovery() then (select received_tli from pg_stat_wal_receiver)::text
                     else ('x' || substr(pg_walfile_name(pg_current_wal_lsn()), 1, 8))::bit(32)::int::text
                end)" 2>/dev/null) || {
            echo "== $n  not running"
            continue
        }
        writes=$(dc exec -T "$n" psql -XtA -d app -c "select count(*) from writes" 2>/dev/null || echo "?")
        echo "== $n  $state writes=$writes"
    done
}

cmd=${1:-}
shift || true
case "$cmd" in
    up) up ;;
    down) dc down -v --remove-orphans ;;
    show) show ;;
    promote)
        check_node "${1:-}"
        dc exec -T "$1" psql -XtA -v ON_ERROR_STOP=1 -c "select pg_promote()"
        ;;
    logs) dc logs -f "$@" ;;
    *)
        usage
        [ -z "$cmd" ] || exit 2
        ;;
esac
