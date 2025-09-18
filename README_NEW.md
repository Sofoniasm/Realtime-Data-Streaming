# Realtime Data Streaming | End-to-End Data Engineering Project

This repository contains a runnable local demo of an end-to-end realtime data pipeline (Airflow → Kafka → Spark → Cassandra) using Docker Compose.

## Quick start

1. Clone the repository:

```bash
git clone https://github.com/Sofoniasm/Realtime-Data-Streaming.git
```

2. Enter the project folder:

```bash
cd Realtime-Data-Streaming
```

3. Launch the stack:

```bash
docker compose up --build -d
```

4. Run the built-in tester to produce sample messages and verify Cassandra writes:

```bash
docker compose run --rm tester
```

5. Inspect Cassandra rows:

```bash
docker exec -it cassandra cqlsh -e "SELECT * FROM events_keyspace.events LIMIT 5;"
```

## Video walkthrough

A community walkthrough for this project is available on YouTube. Credit: Codewithyou

[Codewithyou — Realtime Data Streaming walkthrough (YouTube)](https://www.youtube.com/watch?v=GqAcTrqKcrY)

---

If you want, I can remove runtime checkpoint files from git and add a `.gitignore` entry, or harden the spark-job startup to wait for Cassandra. Tell me which you'd like next.
