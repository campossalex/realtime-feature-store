#!/usr/bin/env bash
# start.sh — betting_features_oss scenario bootstrap
#
# Open-source Flink + Fluss feature store demo — no VVP required.
# Fluss is the only store; there is no Iceberg/MinIO lakehouse tier.
#
# Usage:
#   cd scenarios/betting_features_oss
#   ./start.sh                  # services + DDL + all 9 streaming jobs
#   ./start.sh --services-only  # containers only; run the DDL and jobs yourself
#
# Memory defaults target a 64 GB host. On a laptop:
#   TM_PROCESS_SIZE=5g JM_PROCESS_SIZE=2g TASK_SLOTS=12 TM_FRAMEWORK_OFFHEAP=512m ./start.sh

set -euo pipefail
cd "$(dirname "$0")"

SERVICES_ONLY=0

usage() {
  cat <<'EOF'
start.sh — betting_features_oss scenario bootstrap

  ./start.sh                  Start every container, run the setup DDL, and submit
                              all 9 streaming jobs (A, B1, B2, B3, C1, C2, D, plus
                              risk-scoring-kafka and risk-score-log-postgres).

  ./start.sh --services-only  Start the containers and install the connector JARs,
                              then stop. No DDL, no jobs — the commands to run them
                              by hand are printed at the end.

  -h, --help                  Show this.

Memory defaults target a 64 GB host. Override for a laptop:
  TM_PROCESS_SIZE=5g JM_PROCESS_SIZE=2g TASK_SLOTS=12 TM_FRAMEWORK_OFFHEAP=512m ./start.sh
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    --services-only) SERVICES_ONLY=1 ;;
    -h|--help)       usage; exit 0 ;;
    *) echo "ERROR: unknown option '$1'"; echo; usage; exit 1 ;;
  esac
  shift
done

# ── Prerequisite checks ────────────────────────────────────────────────────────

require_cmd() { command -v "$1" &>/dev/null || { echo "ERROR: '$1' not found"; exit 1; }; }
require_cmd docker
require_cmd curl
require_cmd jq

# ── Step 1: Download connector JARs ───────────────────────────────────────────

if [ ! -d lib ] || [ -z "$(ls -A lib 2>/dev/null)" ]; then
  echo "==> Downloading connector JARs..."
  bash download-jars.sh
else
  echo "==> [skip] lib/ already populated ($(ls lib/*.jar 2>/dev/null | wc -l | tr -d ' ') JARs)"
fi

# ── Step 2: Start Docker Compose services ─────────────────────────────────────

echo ""
echo "==> Starting Docker Compose services..."
docker compose up -d --remove-orphans

echo ""
echo "==> Waiting for core services to be healthy..."

wait_healthy() {
  local service="$1"
  local max_attempts="${2:-30}"
  local attempt=0
  echo -n "    $service "
  until docker compose ps "$service" 2>/dev/null | grep -q "healthy\|running"; do
    attempt=$((attempt + 1))
    if [ $attempt -ge $max_attempts ]; then
      echo " TIMEOUT"
      echo "ERROR: $service did not become healthy within $(( max_attempts * 5 ))s"
      docker compose logs "$service" --tail=20
      exit 1
    fi
    echo -n "."
    sleep 5
  done
  echo " OK"
}

wait_healthy zookeeper 12
wait_healthy redpanda 20
wait_healthy postgres 12
wait_healthy jobmanager 24

echo ""
echo "==> All core services healthy."

# ── Step 3: Copy JARs into Flink lib (needed for SQL Client to find connectors) ──

echo ""
echo "==> Installing connector JARs into Flink containers..."
for jar in lib/*.jar; do
  [ -f "$jar" ] || continue
  filename=$(basename "$jar")
  docker compose exec -T jobmanager \
    sh -c "[ -f /opt/flink/lib/$filename ] || cp /opt/flink/lib/extra/$filename /opt/flink/lib/" 2>/dev/null || true
  docker compose exec -T taskmanager \
    sh -c "[ -f /opt/flink/lib/$filename ] || cp /opt/flink/lib/extra/$filename /opt/flink/lib/" 2>/dev/null || true
  docker compose exec -T sql-client \
    sh -c "[ -f /opt/flink/lib/$filename ] || cp /opt/flink/lib/extra/$filename /opt/flink/lib/" 2>/dev/null || true
done
echo "    JARs installed."

# ── Step 4: Submit Flink SQL (setup + feature engineering jobs) ───────────────

if [ "$SERVICES_ONLY" = "1" ]; then
  echo ""
  echo "==> [--services-only] Skipping DDL and job submission."
  echo "    The commands to run them by hand are printed below."
  RUNNING_JOBS=0
else
  echo ""
  echo "==> Submitting Flink SQL via SQL Client..."
  echo "    This submits 9 streaming jobs:"
  echo "      A, B1, B2, B3, C1, C2, D          — feature engineering → Fluss"
  echo "      risk-scoring-kafka                 — bets ⋈ feature store → risk_scores topic"
  echo "      risk-score-log-postgres            — risk_scores → PostgreSQL (Grafana reads this)"
  echo ""

  # The SQL Client processes SQL files sequentially in one session.
  # --init registers catalogs (session-scoped); -f executes statements and submits streaming jobs.
  # Streaming INSERT jobs are non-blocking: the client submits them and moves to the next statement.

  # Step A+B: create Fluss table (DDL only — completes quickly)
  echo "    Running setup DDL (catalogs + feature store table)..."
  docker compose exec -T sql-client \
    /opt/flink/bin/sql-client.sh embedded \
    --init /opt/flink_sql/01_sources_and_catalogs.sql \
    -f /opt/flink_sql/02_fluss_feature_store.sql \
    2>&1 | tee /tmp/flink-sql-setup.log || {
    echo ""
    echo "WARNING: DDL submission had errors — check /tmp/flink-sql-setup.log"
  }

  # Step C: submit the CDC dimensions job first — it populates kyc_tier / balance /
  # lifetime_deposits, and on a cold start it snapshots both PostgreSQL tables before any
  # row reaches Fluss.
  echo "    Submitting CDC dimensions job (A)..."
  docker compose exec -T sql-client \
    /opt/flink/bin/sql-client.sh embedded \
    --init /opt/flink_sql/01_sources_and_catalogs.sql \
    -f /opt/flink_sql/03_cdc_dimensions.sql \
    2>&1 | tee /tmp/flink-sql-cdc.log || {
    echo ""
    echo "WARNING: CDC job submission had errors — check /tmp/flink-sql-cdc.log"
    echo "You can also submit manually (see the 'Manual SQL setup' section below)."
  }

  # Step D: submit the windowed feature calculations
  echo "    Submitting feature calculation jobs (B1, B2, B3, C1, C2, D)..."
  docker compose exec -T sql-client \
    /opt/flink/bin/sql-client.sh embedded \
    --init /opt/flink_sql/01_sources_and_catalogs.sql \
    -f /opt/flink_sql/04_feature_calculations.sql \
    2>&1 | tee /tmp/flink-sql-jobs.log || {
    echo ""
    echo "WARNING: Job submission had errors — check /tmp/flink-sql-jobs.log"
    echo "You can also submit manually (see the 'Manual SQL setup' section below)."
  }

  # Step E: risk scoring. Submitted last on purpose — it temporal-joins the feature store,
  # so a bet only produces a row once that player already has a feature vector. Nothing
  # writes to PostgreSQL without these two jobs, and every Grafana panel queries
  # public.risk_score_log, so skipping them leaves the dashboard blank.
  echo "    Submitting risk scoring jobs (Kafka + PostgreSQL)..."
  docker compose exec -T sql-client \
    /opt/flink/bin/sql-client.sh embedded \
    --init /opt/flink_sql/01_sources_and_catalogs.sql \
    -f /opt/flink_sql/05_risk_scoring_inference.sql \
    2>&1 | tee /tmp/flink-sql-risk.log || {
    echo ""
    echo "WARNING: Risk scoring submission had errors — check /tmp/flink-sql-risk.log"
    echo "You can also submit manually (see the 'Manual SQL setup' section below)."
  }

  echo ""
  echo "==> Flink jobs submitted. Verifying..."
  sleep 5
  RUNNING_JOBS=$(curl -s http://localhost:8081/jobs | jq -r '.jobs[] | select(.status == "RUNNING") | .id' | wc -l | tr -d ' ')
  echo "    Running Flink jobs: $RUNNING_JOBS  (expected 9)"
fi

# ── Step 5: Status summary ────────────────────────────────────────────────────

echo ""
echo "══════════════════════════════════════════════════════════════"
if [ "$SERVICES_ONLY" = "1" ]; then
  echo "  Betting Feature Store OSS — Services Started (no jobs)"
else
  echo "  Betting Feature Store OSS — Setup Complete"
fi
echo "══════════════════════════════════════════════════════════════"
echo ""
echo "  Services:"
echo "    Flink Web UI        →  http://localhost:8081"
echo "    Inference service   →  http://localhost:9051"
echo "    Outliner generator  →  http://localhost:9050"
echo ""
echo "    Grafana             →  http://localhost:3000"
echo "    Redpanda Console    →  http://localhost:8082"
echo ""
echo "  Kafka (from host):    localhost:19092"
echo "  Fluss (from host):    localhost:9123"
echo "  PostgreSQL (from host): localhost:5432"
echo ""
if [ "$SERVICES_ONLY" = "1" ]; then
  echo "  ─────────────────────────────────────────────────────────────"
  echo "  NO DDL OR JOBS WERE SUBMITTED (--services-only)."
  echo "  Run the steps below in order to bring the pipeline up."
  echo "  ─────────────────────────────────────────────────────────────"
else
  echo "  Manual SQL — to re-run a step, or if auto-submission failed:"
fi
echo ""
echo "  Every command below is copy-pasteable from this directory."
echo "  01_ is always passed as --init: it registers the Fluss catalog, the Kafka"
echo "  and CDC source tables, and the table.exec.source.idle-timeout that keeps"
echo "  event-time windows from stalling. Nothing works without it."
echo ""
echo "  1. DDL — create the Fluss feature store table (run once, idempotent):"
echo "     docker compose exec -T sql-client /opt/flink/bin/sql-client.sh embedded \\"
echo "       --init /opt/flink_sql/01_sources_and_catalogs.sql \\"
echo "       -f    /opt/flink_sql/02_fluss_feature_store.sql"
echo ""
echo "  2. Job A — PostgreSQL CDC → dimension columns (run before 3, it snapshots first):"
echo "     docker compose exec -T sql-client /opt/flink/bin/sql-client.sh embedded \\"
echo "       --init /opt/flink_sql/01_sources_and_catalogs.sql \\"
echo "       -f    /opt/flink_sql/03_cdc_dimensions.sql"
echo ""
echo "  3. Jobs B1,B2,B3,C1,C2,D — windowed features → Fluss (6 jobs):"
echo "     docker compose exec -T sql-client /opt/flink/bin/sql-client.sh embedded \\"
echo "       --init /opt/flink_sql/01_sources_and_catalogs.sql \\"
echo "       -f    /opt/flink_sql/04_feature_calculations.sql"
echo ""
echo "  4. Risk scoring — bets ⋈ feature store → Kafka, then → PostgreSQL (2 jobs):"
echo "     docker compose exec -T sql-client /opt/flink/bin/sql-client.sh embedded \\"
echo "       --init /opt/flink_sql/01_sources_and_catalogs.sql \\"
echo "       -f    /opt/flink_sql/05_risk_scoring_inference.sql"
echo "     # Without this, public.risk_score_log stays empty and Grafana shows nothing."
echo ""
echo "  5. Ad-hoc analytics queries (SELECT-only, nothing to deploy):"
echo "     docker compose exec -it sql-client /opt/flink/bin/sql-client.sh embedded \\"
echo "       --init /opt/flink_sql/01_sources_and_catalogs.sql"
echo "     # then paste from flink_sql/06_analytics_and_monitoring.sql"
echo ""
echo "  Interactive SQL Client (catalogs pre-loaded):"
echo "    docker compose exec -it sql-client /opt/flink/bin/sql-client.sh embedded \\"
echo "      --init /opt/flink_sql/01_sources_and_catalogs.sql"
echo ""
echo "  Verify feature store (BOT_001 after outliner fires):"
echo "    curl -s http://localhost:9051/score \\"
echo "      -H 'Content-Type: application/json' \\"
echo "      -d '{\"player_id\": \"BOT_001\", \"amount\": 50}'"
echo ""
echo "  Verify in Flink SQL:"
echo "    SELECT * FROM fluss.betting.player_feature_store"
echo "    WHERE player_id = 'BOT_001';"
echo ""
echo "  Verify the PostgreSQL analytics table (what Grafana charts):"
echo "    docker compose exec -T postgres psql -U root -d betting \\"
echo "      -c 'SELECT risk_level, COUNT(*) FROM public.risk_score_log GROUP BY 1;'"
echo ""
echo "  Pause (keeps PostgreSQL / Fluss / checkpoint state):"
echo "    docker compose down"
echo ""
echo "  Destroy everything:"
echo "    ./destroy.sh            # containers + volumes + network"
echo "    ./destroy.sh --all -y   # also lib/, fluss-remote-data/ and images"
echo "══════════════════════════════════════════════════════════════"
