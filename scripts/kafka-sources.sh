#!/usr/bin/env bash
# kafka-sources.sh — print a job file with its event views reading Kafka (simple mode)
#
# The feature and scoring jobs (05_, 06_) read their events through temporary views
# whose one FROM line names a Fluss log table, from its latest offset (complete mode).
# Simple mode has no Fluss log tables: this swaps that line — and only that one — for
# the Kafka table of 01_, from its latest offset. See 05_feature_calculations.sql.
# The SQL editor applies the same swap (KAFKA_SOURCE in sql-editor/app.py).
#
# Usage, from the repository root:
#   scripts/kafka-sources.sh 05_feature_calculations.sql \
#     | docker compose exec -T sql-client sh -c 'cat > /tmp/job.sql && \
#         /opt/flink/bin/sql-client.sh embedded \
#           --init /opt/flink_sql/01_sources_and_catalogs.sql -f /tmp/job.sql'

set -euo pipefail
cd "$(dirname "$0")/.."

[ $# -eq 1 ] || { echo "usage: $0 <file in flink_sql/>" >&2; exit 1; }

sed -E "s#FROM fluss\.betting\.([a-z_]+) /\*\+ OPTIONS\('scan\.startup\.mode' = 'latest'\) \*/#FROM \1_kafka /*+ OPTIONS('scan.startup.mode' = 'latest-offset') */#" \
  "flink_sql/$1"
