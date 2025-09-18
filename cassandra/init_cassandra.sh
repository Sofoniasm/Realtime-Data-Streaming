#!/usr/bin/env bash
set -euo pipefail

echo "Waiting for Cassandra to be available..."
until cqlsh cassandra 9042 -e "describe keyspaces" >/dev/null 2>&1; do
  sleep 2
done

echo "Applying init_cassandra.cql"
cqlsh cassandra 9042 -f /opt/cassandra-init/init_cassandra.cql

echo "Cassandra init complete"
