#!/bin/bash
set -euo pipefail

 # Install runtime deps for the tester
 pip install --no-cache-dir kafka-python cassandra-driver || true

## Ensure Cassandra keyspace/table exist (wait until Cassandra is reachable)
python - <<'PY'
from cassandra.cluster import Cluster
import time, sys

for _ in range(30):
        try:
                cluster = Cluster(['cassandra'])
                session = cluster.connect()
                session.execute("""CREATE KEYSPACE IF NOT EXISTS events_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}""")
                session.execute("""
                        CREATE TABLE IF NOT EXISTS events_keyspace.events (
                            id uuid PRIMARY KEY,
                            first_name text,
                            last_name text,
                            gender text,
                            address text,
                            post_code text,
                            email text,
                            username text,
                            dob text,
                            registered_date text,
                            phone text,
                            picture text
                        )
                """)
                session.shutdown()
                cluster.shutdown()
                print('cassandra init ok')
                break
        except Exception as e:
                print('waiting for cassandra...', e)
                time.sleep(1)
else:
        print('cassandra not ready')
        sys.exit(1)
PY

# Produce a single test event to Kafka
python - <<'PY'
from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(bootstrap_servers='broker:29092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
event = {
  "first_name":"Test",
  "last_name":"User",
  "gender":"other",
  "address":"123 Test St",
  "post_code":"00000",
  "email":"test@example.com",
  "username":"test_user",
  "dob":"1990-01-01",
  "registered_date":"2025-09-18",
  "phone":"+1000000000",
  "picture":""
}
producer.send('users_created', event)
producer.flush()
print('produced')
PY

# Wait a moment for streaming job to consume
sleep 5

# Query Cassandra for the inserted username
python - <<'PY'
from cassandra.cluster import Cluster
import time
import sys

cluster = Cluster(['cassandra'])
session = cluster.connect()

query = "SELECT username, email FROM events_keyspace.events WHERE username='test_user' ALLOW FILTERING"
start = time.time()
found = False
while time.time() - start < 30:
    rows = session.execute(query)
    for r in rows:
        print('found:', r.username, r.email)
        found = True
        break
    if found:
        break
    time.sleep(1)

if not found:
    print('not found')
    sys.exit(1)
else:
    sys.exit(0)
PY
