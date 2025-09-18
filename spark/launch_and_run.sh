#!/bin/bash
set -euo pipefail

# Debug: print Python executable and pip info
/opt/bitnami/python/bin/python3 -c "import sys; print('sys.executable:', sys.executable); import site; print('site-packages:', site.getsitepackages())" || true
/opt/bitnami/python/bin/python3 -m pip show cassandra-driver || true

# Also check /usr/bin/python3
/usr/bin/python3 -c "import sys; print('sys.executable:', sys.executable); import site; print('site-packages:', site.getsitepackages())" || true
/usr/bin/python3 -m pip show cassandra-driver || true

# Now run spark-submit with the existing args
echo "Starting spark-submit in local mode to avoid cluster resource issues"
exec /opt/bitnami/spark/bin/spark-submit --master local[*] --conf spark.jars.ivy=/tmp/.ivy2 /opt/app/streaming_job.py
