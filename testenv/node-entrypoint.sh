#!/bin/bash
# Starts one node of the test cluster: PostgreSQL registered with repmgr, plus repmgrd.
#
# PostgreSQL is started with pg_ctl rather than as the container's main process, so repmgr can
# restart it (standby follow, node rejoin) without the container exiting.
set -euo pipefail

: "${NODE_ID:?}" "${NODE_NAME:?}" "${NODE_HOST:?}" "${PRIMARY_HOST:?}" "${REPMGR_PASSWORD:?}"

HOME_DIR=/var/lib/postgresql
CONF=$HOME_DIR/repmgr.conf
PGLOG=$HOME_DIR/postgres.log
REPMGRD_LOG=$HOME_DIR/repmgrd.log
REPMGRD_PID=/tmp/repmgrd.pid

echo "*:*:*:repmgr:$REPMGR_PASSWORD" > "$HOME_DIR/.pgpass"
chmod 600 "$HOME_DIR/.pgpass"

# Outside PGDATA, so pg_basebackup and pg_rewind don't copy it between nodes.
echo "cluster_name = '$NODE_NAME'" > "$HOME_DIR/node.conf"

cat > "$CONF" <<EOF
node_id=$NODE_ID
node_name='$NODE_NAME'
conninfo='host=$NODE_HOST port=5432 user=repmgr dbname=repmgr connect_timeout=2'
data_directory='$PGDATA'
priority=${NODE_PRIORITY:-100}
use_replication_slots=yes
pg_bindir='/usr/lib/postgresql/17/bin'

service_start_command='pg_ctl -D $PGDATA -l $PGLOG -w start'
service_stop_command='pg_ctl -D $PGDATA -m fast -w stop'
service_restart_command='pg_ctl -D $PGDATA -l $PGLOG -m fast -w restart'
service_reload_command='pg_ctl -D $PGDATA reload'

failover='automatic'
promote_command='repmgr standby promote -f $CONF --log-to-file'
follow_command='repmgr standby follow -f $CONF --log-to-file --upstream-node-id=%n'
monitor_interval_secs=2
reconnect_attempts=3
reconnect_interval=2

log_file='$REPMGRD_LOG'
log_level=INFO
repmgrd_pid_file='$REPMGRD_PID'
EOF

start_postgres() {
    pg_ctl -D "$PGDATA" -l "$PGLOG" -w start
}

init_primary() {
    initdb -D "$PGDATA" -U postgres --auth-local=trust --auth-host=scram-sha-256
    cat >> "$PGDATA/postgresql.conf" <<EOF
listen_addresses = '*'
wal_level = replica
max_wal_senders = 10
max_replication_slots = 10
hot_standby = on
wal_log_hints = on
# Keeps the WAL from before a failover, which pg_rewind needs to rejoin the old primary.
wal_keep_size = '512MB'
shared_preload_libraries = 'repmgr'
include_if_exists = '$HOME_DIR/node.conf'
EOF
    cat >> "$PGDATA/pg_hba.conf" <<EOF
host all all all scram-sha-256
host replication all all scram-sha-256
EOF
    start_postgres
    psql -v ON_ERROR_STOP=1 -U postgres <<EOF
create user repmgr superuser password '$REPMGR_PASSWORD';
create database repmgr owner repmgr;
create user app password 'app';
create database app owner app;
EOF
    psql -v ON_ERROR_STOP=1 -U app -d app <<EOF
create table writes (
    id bigserial primary key,
    at timestamptz not null default now(),
    node text not null default current_setting('cluster_name')
);
EOF
    repmgr -f "$CONF" primary register
}

init_standby() {
    echo "Waiting for the primary at $PRIMARY_HOST..."
    until psql -XtA "host=$PRIMARY_HOST user=repmgr dbname=repmgr connect_timeout=2" \
        -c "select 1 from repmgr.nodes where type = 'primary' and active" 2>/dev/null | grep -q 1; do
        sleep 1
    done
    repmgr -f "$CONF" -h "$PRIMARY_HOST" -U repmgr -d repmgr standby clone --fast-checkpoint
    start_postgres
    repmgr -f "$CONF" standby register
}

if [ ! -s "$PGDATA/PG_VERSION" ]; then
    if [ "$NODE_HOST" = "$PRIMARY_HOST" ]; then
        init_primary
    else
        init_standby
    fi
else
    # Restarted container: start in whatever role the data directory is in.
    start_postgres
fi

repmgrd -f "$CONF"

shutdown() {
    echo "Stopping $NODE_NAME"
    if [ -f "$REPMGRD_PID" ]; then
        kill "$(cat "$REPMGRD_PID")" 2>/dev/null || true
    fi
    pg_ctl -D "$PGDATA" -m fast -w stop || true
    kill "$TAIL_PID" 2>/dev/null || true
    exit 0
}

touch "$PGLOG" "$REPMGRD_LOG"
tail -n +1 -F "$PGLOG" "$REPMGRD_LOG" &
TAIL_PID=$!
trap shutdown TERM INT
wait "$TAIL_PID"
