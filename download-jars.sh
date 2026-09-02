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

LIB_DIR="$(dirname "$0")/lib"
mkdir -p "$LIB_DIR"

download() {
  local filename="$1"
  local url="$2"
  if [ -f "$LIB_DIR/$filename" ]; then
    echo "  [skip] $filename already present"
    return
  fi
  echo "  [download] $filename"
  curl -fsSL -o "$LIB_DIR/$filename" "$url"
}

# Prune JARs left over from the removed lakehouse tier. start.sh copies every lib/*.jar
# into the Flink containers, so a lib/ populated before the removal would keep loading
# Iceberg and Hadoop into every JVM. Matching by prefix rather than exact filename so this
# still fires for other versions.
prune_lakehouse_jars() {
  local stale=(
    fluss-flink-tiering-
    fluss-lake-iceberg-
    fluss-fs-s3-
    iceberg-flink-runtime-
    iceberg-aws-
    flink-shaded-hadoop-
  )
  local prefix found=0 f
  for prefix in "${stale[@]}"; do
    for f in "$LIB_DIR/$prefix"*.jar; do
      [ -e "$f" ] || continue
      [ "$found" -eq 0 ] && echo "==> Pruning lakehouse JARs (this scenario has no Iceberg tier)..."
      found=1
      echo "  [remove] $(basename "$f")"
      rm -f "$f"
    done
  done
}

prune_lakehouse_jars

# No fluss-flink-tiering / fluss-lake-iceberg / fluss-fs-s3 here: this scenario has no
# lakehouse tier and Fluss keeps its remote data on a local bind mount, so nothing needs
# a lake format or an S3 filesystem.
echo "==> Downloading Fluss connector JARs..."
download "fluss-flink-${FLINK_VERSION}-${FLUSS_VERSION}.jar" \
  "$MAVEN/org/apache/fluss/fluss-flink-${FLINK_VERSION}/${FLUSS_VERSION}/fluss-flink-${FLINK_VERSION}-${FLUSS_VERSION}.jar"

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

# No Iceberg JARs and no flink-shaded-hadoop here. The Hadoop uber JAR existed only so
# Iceberg's Flink catalog factory could instantiate org.apache.hadoop.conf.Configuration
# during CREATE CATALOG; with no Iceberg catalog and checkpoints on a file:// path,
# Flink's built-in filesystem is enough.

echo ""
echo "==> JARs in $LIB_DIR:"
ls -lh "$LIB_DIR"
echo ""
echo "Done. Run 'docker compose up -d' to start the cluster."
