#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE USER replicationUser WITH REPLICATION ENCRYPTED PASSWORD 'replicator_password';
    SELECT pg_create_physical_replication_slot('replication_slot');
EOSQL

PGDATA="/var/lib/postgresql/data" # Adjust if necessary

# Add the replication entry (replace 172.22.0.X with your worker's IP or network)
echo "host    replication     \"replicationuser\"     0.0.0.0/0         trust" >> "$PGDATA/pg_hba.conf"
echo "host    all    all     0.0.0.0/0         trust" >> "$PGDATA/pg_hba.conf"
# You might need to reload the configuration
pg_ctl reload -D "$PGDATA"
