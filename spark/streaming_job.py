"""Lightweight Kafka consumer that writes messages from Kafka to Cassandra.

This file is intentionally small and self-contained to avoid Spark
classpath/JAR issues during development. It uses kafka-python to
consume from the topic 'users_created' and the DataStax cassandra-driver
to persist rows into the keyspace/table created by the init script.

Run inside the spark-job container (the repository mounts this file at /opt/app).
"""

import json
import time
import uuid
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('consumer')


def connect_cassandra(retries: int = 5, delay: float = 2.0):
    try:
        from cassandra.cluster import Cluster
    except Exception as exc:  # pragma: no cover - runtime dependency
        logger.exception('cassandra-driver is not installed in this image')
        raise

    for attempt in range(1, retries + 1):
        try:
            cluster = Cluster(['cassandra'])
            session = cluster.connect()
            return cluster, session
        except Exception as exc:
            logger.warning('Cassandra connect attempt %s failed: %s', attempt, exc)
            time.sleep(delay)
    raise RuntimeError('unable to connect to Cassandra after retries')


def run_consumer():
    try:
        from kafka import KafkaConsumer
    except Exception:
        logger.exception('kafka-python is not installed in this image')
        raise

    cluster, session = connect_cassandra()

    insert_cql = (
        "INSERT INTO events_keyspace.events (id, first_name, last_name, gender, address, post_code, email, username, dob, registered_date, phone, picture)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )

    prepared = session.prepare(insert_cql)

    consumer = KafkaConsumer(
        'users_created',
        bootstrap_servers='broker:29092',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='consumer-direct-cassandra',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else {},
    )

    logger.info('consumer started, listening for messages...')
    try:
        for msg in consumer:
            data = msg.value if isinstance(msg.value, dict) else {}
            uid = uuid.uuid4()
            try:
                session.execute(prepared, (
                    uid,
                    data.get('first_name'),
                    data.get('last_name'),
                    data.get('gender'),
                    data.get('address'),
                    data.get('post_code'),
                    data.get('email'),
                    data.get('username'),
                    data.get('dob'),
                    data.get('registered_date'),
                    data.get('phone'),
                    data.get('picture'),
                ))
                logger.info('inserted id=%s partition=%s offset=%s', uid, msg.partition, msg.offset)
            except Exception as exc:
                logger.exception('failed to insert message into Cassandra: %s', exc)
                # small backoff to avoid tight-loop on persistent failures
                time.sleep(1)
    except KeyboardInterrupt:
        logger.info('consumer interrupted, shutting down')
    finally:
        try:
            session.shutdown()
            cluster.shutdown()
        except Exception:
            pass


if __name__ == '__main__':
    run_consumer()
