FROM bitnami/spark:3.4.1

# Install system python3 and pip, then install required Python packages into the interpreters
USER root
RUN apt-get update \
    && apt-get install -y python3 python3-pip curl gnupg2 --no-install-recommends \
    && python3 -m pip install --no-cache-dir cassandra-driver==3.25.0 kafka-python || true \
    # Also try to install into Bitnami's python if present so executors using that interpreter have packages
    && if [ -x /opt/bitnami/python/bin/python3 ]; then /opt/bitnami/python/bin/python3 -m pip install --no-cache-dir cassandra-driver==3.25.0 kafka-python || true; fi \
    && rm -rf /var/lib/apt/lists/*

# Force Spark to use the system python interpreter for driver and executors to avoid venv shebang issues on Windows hosts
ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3

USER 1001

WORKDIR /opt/app
COPY streaming_job.py /opt/app/streaming_job.py
COPY launch_and_run.sh /opt/app/launch_and_run.sh

# Ensure launcher is executable
USER root
RUN chmod +x /opt/app/launch_and_run.sh || true
USER 1001

# Copy any pre-downloaded connector jars into Spark's jars directory to avoid Ivy downloads at runtime.
# Place connector jars in repo's spark/libs/ (e.g. spark-sql-kafka-0-10_2.12-3.4.1.jar) before building.
USER root
RUN mkdir -p /opt/app/libs
COPY libs/ /opt/app/libs/
RUN sh -c 'for f in /opt/app/libs/*.jar; do [ -f "$f" ] && cp "$f" /opt/bitnami/spark/jars/ || true; done'
USER 1001

USER root
# Do not create a venv here - system python installations above are used by Spark (see ENV)
USER 1001
