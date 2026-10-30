#!/usr/bin/env bash
# download-jars.sh — fetch connector JARs into ./lib/ for the Flink session cluster.
#
# Flink lib/ volume mount in docker-compose.yml maps ./lib/ → /opt/flink/lib/extra/
# Flink loads JARs from all directories in plugin.classloader.parent-first-patterns,
# but for SQL connectors the simplest path is to copy them into the base lib/ dir
# by symlinking via the FLINK_PLUGINS_DIR mount or a Dockerfile.
#
# The lib/ directory here is mounted read-only into jobmanager, taskmanager, and sql-client.
# For Flink to pick them up automatically, symlink lib/ entries into /opt/flink/lib/:
#
#   entrypoint override in docker-compose:
#     command: >
#       sh -c "cp /opt/flink/lib/extra/*.jar /opt/flink/lib/ && <original-cmd>"
#
# Alternatively, build a custom Flink image (see Dockerfile.flink).

set -euo pipefail

MAVEN="https://repo1.maven.org/maven2"
FLUSS_VERSION="0.9.1-incubating"
FLINK_VERSION="1.20"
KAFKA_CONNECTOR_VERSION="3.3.0-1.20"
CDC_CONNECTOR_VERSION="3.2.1"
# Must match the Iceberg build bundled inside fluss-lake-iceberg (0.9.1-incubating ships
# Iceberg 1.10.1). A mismatch surfaces as NoSuchMethodError at the first tiering commit.
# Verify with:  unzip -p lib/fluss-lake-iceberg-*.jar iceberg-build.properties
ICEBERG_VERSION="1.10.1"

LIB_DIR="$(dirname "$0")/lib"
# lib/tiering/ is NOT on any classpath: the tiering service is a Flink job submitted with
# `flink run` by scripts/tiering.sh (or the Pipeline Builder), and its JAR is the main artifact.
mkdir -p "$LIB_DIR/tiering"

download() {
  local filename="$1"
  local url="$2"
  if [ -f "$LIB_DIR/$filename" ]; then
    echo "  [skip] $filename already present"
    return
  fi
  echo "  [download] $filename"
  curl -fsSL -o "$LIB_DIR/$filename.part" "$url"
  mv "$LIB_DIR/$filename.part" "$LIB_DIR/$filename"
}

# No fluss-fs-s3: Fluss keeps its remote data on a shared local volume, not on S3 (see
# remote.data.dir in docker-compose.yml). Only the Iceberg lake lives on MinIO.
echo "==> Downloading Fluss connector + the tiering service..."
download "fluss-flink-${FLINK_VERSION}-${FLUSS_VERSION}.jar" \
  "$MAVEN/org/apache/fluss/fluss-flink-${FLINK_VERSION}/${FLUSS_VERSION}/fluss-flink-${FLINK_VERSION}-${FLUSS_VERSION}.jar"

download "tiering/fluss-flink-tiering-${FLUSS_VERSION}.jar" \
  "$MAVEN/org/apache/fluss/fluss-flink-tiering/${FLUSS_VERSION}/fluss-flink-tiering-${FLUSS_VERSION}.jar"

echo "==> Fluss ↔ Iceberg: the lake format plugin + S3FileIO for MinIO..."
# fluss-lake-iceberg is what both the tiering job (writing) and the Fluss connector's
# union read (reading) use to talk to Iceberg. It bundles iceberg-core but NOT S3FileIO,
# which lives in iceberg-aws and needs the AWS SDK from iceberg-aws-bundle plus failsafe
# (iceberg-aws's retry library). Without the three, the first commit fails with
#   ClassNotFoundException: org.apache.iceberg.aws.s3.S3FileIO
download "fluss-lake-iceberg-${FLUSS_VERSION}.jar" \
  "$MAVEN/org/apache/fluss/fluss-lake-iceberg/${FLUSS_VERSION}/fluss-lake-iceberg-${FLUSS_VERSION}.jar"

download "iceberg-aws-${ICEBERG_VERSION}.jar" \
  "$MAVEN/org/apache/iceberg/iceberg-aws/${ICEBERG_VERSION}/iceberg-aws-${ICEBERG_VERSION}.jar"

download "iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
  "$MAVEN/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"

download "failsafe-3.3.2.jar" \
  "$MAVEN/dev/failsafe/failsafe/3.3.2/failsafe-3.3.2.jar"

echo "==> Iceberg's own Flink catalog (optional — the 'iceberg' catalog in 01_sources_and_catalogs.sql, for time travel)..."
download "iceberg-flink-runtime-${FLINK_VERSION}-${ICEBERG_VERSION}.jar" \
  "$MAVEN/org/apache/iceberg/iceberg-flink-runtime-${FLINK_VERSION}/${ICEBERG_VERSION}/iceberg-flink-runtime-${FLINK_VERSION}-${ICEBERG_VERSION}.jar"

# The apache/flink image ships without Hadoop, and Iceberg code paths — the Flink
# catalog factory, and the Iceberg writer inside the tiering job — instantiate
# org.apache.hadoop.conf.Configuration even when every byte goes through S3FileIO.
#
# Trino's hadoop-apache, NOT flink-shaded-hadoop-2-uber. The uber JAR bundles Avro 1.7,
# which shadows the Avro 1.12 Iceberg is built against, and the tiering job restart-loops
# on its first write with
#   NoSuchMethodError: 'org.apache.avro.LogicalTypes$TimestampNanos
#                       org.apache.avro.LogicalTypes.timestampNanos()'
# hadoop-apache relocates all of Hadoop's third-party dependencies, Avro included.
download "hadoop-apache-3.3.5-2.jar" \
  "$MAVEN/io/trino/hadoop/hadoop-apache/3.3.5-2/hadoop-apache-3.3.5-2.jar"

# A lib/ populated by an earlier version of this script would still carry the uber JAR,
# and every JAR in lib/ is copied onto the Flink classpath.
if [ -f "$LIB_DIR/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar" ]; then
  echo "  [remove] flink-shaded-hadoop-2-uber-2.8.3-10.0.jar (Avro 1.7 clash, see above)"
  rm -f "$LIB_DIR/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar"
fi

echo "==> Downloading Flink SQL connectors..."
download "flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar" \
  "$MAVEN/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar"

download "flink-sql-connector-postgres-cdc-${CDC_CONNECTOR_VERSION}.jar" \
  "$MAVEN/org/apache/flink/flink-sql-connector-postgres-cdc/${CDC_CONNECTOR_VERSION}/flink-sql-connector-postgres-cdc-${CDC_CONNECTOR_VERSION}.jar"

echo "==> Downloading Flink JDBC catalog + PostgreSQL driver..."
download "flink-connector-jdbc-3.4.0-1.20.jar" \
  "$MAVEN/org/apache/flink/flink-connector-jdbc/3.4.0-1.20/flink-connector-jdbc-3.4.0-1.20.jar"

download "postgresql-42.7.4.jar" \
  "$MAVEN/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar"

echo ""
echo "==> JARs in $LIB_DIR:"
ls -lh "$LIB_DIR" "$LIB_DIR/tiering"
echo ""
echo "Done. Run 'docker compose up -d' to start the cluster."
