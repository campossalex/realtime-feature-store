#!/usr/bin/env bash
# tiering.sh — start the Fluss Datalake Tiering Service
#
# The tiering service is an ordinary Flink streaming job, shipped as a JAR by the Fluss
# project (fluss-flink-tiering). It asks the Fluss coordinator which tables have
# 'table.datalake.enabled' = 'true' — here the four event log tables in fluss.betting —
# reads their new log data, writes it out as Parquet files on MinIO, and commits an
# Iceberg snapshot through Lakekeeper once per Flink checkpoint. ONE job serves every
# Fluss table in the cluster: you never deploy one per table.
#
# Runs INSIDE the jobmanager container, where it is mounted at /opt/tiering.sh:
#
#   docker compose exec jobmanager /opt/tiering.sh
#
# start.sh does this for you unless --services-only; then it is one click in the Pipeline
# Builder (sql-editor/app.py). Submitting from the jobmanager container means the job
# inherits the cluster's config, and with it the 60s checkpoint interval — without
# checkpoints the job would never commit anything.
set -euo pipefail

JAR="$(ls /opt/flink/lib/extra/tiering/fluss-flink-tiering-*.jar 2>/dev/null | head -1)"
[ -n "$JAR" ] || { echo "ERROR: no fluss-flink-tiering JAR in lib/tiering/ — run ./download-jars.sh" >&2; exit 1; }

# Refuse to start a second copy: two tiering jobs would compete for the same tables.
if curl -sf http://localhost:8081/jobs/overview \
    | grep -q '"name":"Fluss Lake Tiering Service[^"]*"[^}]*"state":"\(RUNNING\|CREATED\|INITIALIZING\|RESTARTING\)"'; then
  echo "The tiering service is already running — nothing to do."
  exit 0
fi

# The arguments live in tiering.args (mounted at /opt/tiering.args), shared with the
# Pipeline Builder's Start button. Word splitting is intended: one "--key value" per line.
ARGS="$(grep -vE '^[[:space:]]*(#|$)' /opt/tiering.args)"
# shellcheck disable=SC2086
exec /opt/flink/bin/flink run -d "$JAR" $ARGS
