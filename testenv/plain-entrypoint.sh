#!/bin/bash
# Starts one node of the plain test cluster: PostgreSQL with streaming replication, without repmgr.
# The primary creates the users and the app database, and the standbys clone it with pg_basebackup.
set -euo pipefail

: "${NODE_NAME:?}" "${NODE_HOST:?}" "${PRIMARY_HOST:?}"

HOME_DIR=/var/lib/postgresql

echo "*:*:*:replicator:replicator" > "$HOME_DIR/.pgpass"
chmod 600 "$HOME_DIR/.pgpass"

# Outside PGDATA, so pg_basebackup doesn't copy it between nodes.
echo "cluster_name = '$NODE_NAME'" > "$HOME_DIR/node.conf"

init_primary() {
    initdb -D "$PGDATA" -U postgres --auth-local=trust --auth-host=scram-sha-256
    cat >> "$PGDATA/postgresql.conf" <<EOF
listen_addresses = '*'
wal_level = replica
max_wal_senders = 10
hot_standby = on
wal_keep_size = '512MB'
include_if_exists = '$HOME_DIR/node.conf'
EOF
    cat >> "$PGDATA/pg_hba.conf" <<EOF
host all all all scram-sha-256
host replication all all scram-sha-256
EOF
    # Only through the socket, so the standbys don't clone it before the users exist.
    pg_ctl -D "$PGDATA" -o "-c listen_addresses=''" -w start
    psql -v ON_ERROR_STOP=1 -U postgres <<EOF
create user replicator replication password 'replicator';
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
    pg_ctl -D "$PGDATA" -m fast -w stop
}

init_standby() {
    echo "Cloning the primary at $PRIMARY_HOST..."
    until pg_basebackup -h "$PRIMARY_HOST" -U replicator -D "$PGDATA" -R -X stream --checkpoint=fast; do
        rm -rf "$PGDATA"
        sleep 1
    done
}

if [ ! -s "$PGDATA/PG_VERSION" ]; then
    if [ "$NODE_HOST" = "$PRIMARY_HOST" ]; then
        init_primary
    else
        init_standby
    fi
fi

exec postgres -D "$PGDATA"
