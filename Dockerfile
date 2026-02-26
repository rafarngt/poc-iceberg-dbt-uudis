FROM eclipse-temurin:17-jdk-jammy

ENV SPARK_VERSION=3.5.3 \
    HADOOP_VERSION=3 \
    ICEBERG_VERSION=1.6.1 \
    HADOOP_AWS_VERSION=3.3.4 \
    AWS_SDK_VERSION=1.12.262 \
    SPARK_HOME=/opt/spark \
    DBT_PROFILES_DIR=/dbt_project \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y \
    curl tar python3 python3-pip python3-venv procps \
    && rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/bin/python3 /usr/bin/python || true

# Spark 3.5.3
RUN curl -fSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -o /tmp/spark.tgz \
    && mkdir -p ${SPARK_HOME} \
    && tar -xf /tmp/spark.tgz -C /opt \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}/* ${SPARK_HOME} \
    && rm /tmp/spark.tgz

# Iceberg 1.6.1
RUN curl -fSL https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar \
        -o ${SPARK_HOME}/jars/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar

# S3A JARs (para MinIO)
RUN curl -fSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
        -o ${SPARK_HOME}/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
    && curl -fSL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
        -o ${SPARK_HOME}/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar

# Python: dbt-spark + uuid-utils
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

# Spark config
RUN mkdir -p ${SPARK_HOME}/conf
COPY spark_conf/spark-defaults.conf ${SPARK_HOME}/conf/spark-defaults.conf

# Non-root user
RUN useradd -m sparkuser \
    && mkdir -p /opt/data /dbt_project \
    && chown -R sparkuser:sparkuser ${SPARK_HOME} \
    && chown -R sparkuser:sparkuser /opt/data \
    && chown -R sparkuser:sparkuser /dbt_project

USER sparkuser
WORKDIR /dbt_project
ENV PATH=${SPARK_HOME}/bin:${SPARK_HOME}/sbin:$PATH
CMD ["bash"]
