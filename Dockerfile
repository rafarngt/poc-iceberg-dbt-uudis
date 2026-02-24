FROM eclipse-temurin:17-jdk-jammy AS builder

# Environment variables
ENV SCALA_VERSION=2.12 \
    SPARK_VERSION=3.5.3 \
    HADOOP_VERSION=3 \
    SPARK_HOME=/opt/spark \
    DBT_PROFILES_DIR=/dbt_project \
    PYTHONUNBUFFERED=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    tar \
    python3 \
    python3-pip \
    python3-venv \
    procps \
    git \
    && rm -rf /var/lib/apt/lists/*

# Make python3 the default python
RUN ln -s /usr/bin/python3 /usr/bin/python || true

# Download and install Spark
RUN curl -fSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -o /tmp/spark.tgz \
    && mkdir -p ${SPARK_HOME} \
    && tar -xvf /tmp/spark.tgz -C /opt \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}/* ${SPARK_HOME} \
    && rm /tmp/spark.tgz

# Add Iceberg JARs for Spark 3.5 + Scala 2.12
RUN curl -fSL https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.1/iceberg-spark-runtime-3.5_2.12-1.6.1.jar \
        -o ${SPARK_HOME}/jars/iceberg-spark-runtime-3.5_2.12-1.6.1.jar \
    && curl -fSL https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-extensions-3.5_2.12/1.6.1/iceberg-spark-extensions-3.5_2.12-1.6.1.jar \
        -o ${SPARK_HOME}/jars/iceberg-spark-extensions-3.5_2.12-1.6.1.jar

# ─────────────────────────────────────────────────────────────
# Python dependencies — Estrategia híbrida de Surrogate Keys
#
# uuid-utils (v0.14.1) — backend Rust via PyO3, BSD-3-Clause
#   → SCD1: genera UUID v7 time-ordered (3.4M UUIDs/seg)
#   → Benchmarked: 0.29 µs/uuid vs 2.84 µs (uuid6) — 9.7x más rápido
#
# stdlib uuid (built-in Python — sin instalación adicional)
#   → SCD2: uuid5(NAMESPACE, seed) — deterministico e idempotente
#   → Mismo center_cd + valid_from = mismo SK siempre
#   → Full-refresh seguro, FKs downstream nunca se rompen
# ─────────────────────────────────────────────────────────────
RUN pip3 install --no-cache-dir --break-system-packages \
    dbt-spark[PyHive]==1.8.0 \
    dbt-core==1.8.0 \
    pyspark==3.5.3 \
    thrift==0.16.0 \
    uuid-utils==0.14.1 \
    || pip3 install --no-cache-dir \
    dbt-spark[PyHive]==1.8.0 \
    dbt-core==1.8.0 \
    pyspark==3.5.3 \
    thrift==0.16.0 \
    uuid-utils==0.14.1

# Create spark-defaults.conf with Iceberg configuration
RUN mkdir -p ${SPARK_HOME}/conf
COPY spark_conf/spark-defaults.conf ${SPARK_HOME}/conf/spark-defaults.conf
COPY spark_conf/hive-site.xml ${SPARK_HOME}/conf/hive-site.xml

# Create non-root user
RUN useradd -m sparkuser \
    && mkdir -p /opt/data /dbt_project \
    && chown -R sparkuser:sparkuser ${SPARK_HOME} \
    && chown -R sparkuser:sparkuser /opt/data \
    && chown -R sparkuser:sparkuser /dbt_project

USER sparkuser
WORKDIR /dbt_project

VOLUME ["/opt/data", "/dbt_project"]

ENV PATH=${SPARK_HOME}/bin:${SPARK_HOME}/sbin:$PATH

CMD ["bash"]
