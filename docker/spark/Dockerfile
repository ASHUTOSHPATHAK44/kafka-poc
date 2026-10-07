# =============================================================================
# Spark image for the CDC -> Iceberg POC
#
# Base: apache/spark:3.5.9-java17-python3
#   -> Spark 3.5.9 / Scala 2.12 / OpenJDK 17 / Python 3 (all inside the image,
#      nothing is taken from the host machine)
#
# Stage 1 downloads the connector jars from Maven Central so the final image
# needs no package manager and no build tools.
# =============================================================================

ARG SPARK_VERSION=3.5.9

# --- stage 1: fetch jars -----------------------------------------------------
FROM alpine:3.22 AS jars

ARG SPARK_VERSION=3.5.9
ARG SCALA_BINARY_VERSION=2.12
ARG ICEBERG_VERSION=1.10.1
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG COMMONS_POOL2_VERSION=2.11.1

ENV MAVEN=https://repo1.maven.org/maven2

RUN set -eux; \
    apk add --no-cache curl; \
    mkdir -p /jars; \
    cd /jars; \
    # --- Iceberg: Spark 3.5 runtime + AWS SDK v2 bundle (S3FileIO for MinIO) --
    curl -fsSLO "${MAVEN}/org/apache/iceberg/iceberg-spark-runtime-3.5_${SCALA_BINARY_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_${SCALA_BINARY_VERSION}-${ICEBERG_VERSION}.jar"; \
    curl -fsSLO "${MAVEN}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"; \
    # --- Structured Streaming Kafka source (versions pinned by Spark's POM) ---
    curl -fsSLO "${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_${SCALA_BINARY_VERSION}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_BINARY_VERSION}-${SPARK_VERSION}.jar"; \
    curl -fsSLO "${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_BINARY_VERSION}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_BINARY_VERSION}-${SPARK_VERSION}.jar"; \
    curl -fsSLO "${MAVEN}/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar"; \
    curl -fsSLO "${MAVEN}/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar"; \
    ls -l /jars

# --- stage 2: final image ----------------------------------------------------
FROM apache/spark:${SPARK_VERSION}-java17-python3

USER root
COPY --from=jars --chown=spark:spark /jars/*.jar /opt/spark/jars/
RUN mkdir -p /opt/spark/work-dir /opt/spark/checkpoints /opt/spark/checkpoints_v2 /opt/spark/events \
 && chown -R spark:spark /opt/spark/work-dir /opt/spark/checkpoints /opt/spark/checkpoints_v2 /opt/spark/events

# Python Kafka client — used by the batch consumer (batch_consumer.py) to read a
# consumer group's committed offsets before a run and commit them after a
# successful Iceberg write (manual offset management for the Airflow-driven job).
RUN pip install --no-cache-dir kafka-python==2.0.2

# uid 185 = the "spark" user in the official image
USER 185
WORKDIR /opt/spark/work-dir
