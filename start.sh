#!/usr/bin/env bash
# start.sh — betting_features_oss scenario bootstrap
#
# Open-source Flink + Fluss feature store demo — no VVP required.
# Fluss holds the event logs and the online feature store. Two modes:
#   complete  Kafka is ingested into Fluss log tables, which are tiered to Iceberg
#             (Lakekeeper + MinIO) for point-in-time history; the feature jobs read Fluss,
#             and a twelfth feature, avg_stake_7d, is bootstrapped from the lake
#   simple    the feature jobs read the Kafka topics directly: no ingest, no Fluss log
#             tables, no lakehouse, the 11 core features only
#
# Usage:
#   cd scenarios/betting_features_oss
#   ./start.sh                  # complete: services + DDL + 12 streaming jobs
#   ./start.sh --simple         # simple: Kafka → features, 9 streaming jobs
#   ./start.sh --services-only  # containers only; run the DDL and jobs yourself
#   ./start.sh --reset          # wipe every volume first (combines with the others)
#
# Memory defaults target a 64 GB host. On a laptop:
#   TM_PROCESS_SIZE=5g JM_PROCESS_SIZE=2g TASK_SLOTS=16 TM_FRAMEWORK_OFFHEAP=512m ./start.sh

set -euo pipefail
cd "$(dirname "$0")"

SERVICES_ONLY=0
RESET=0
SIMPLE=0

usage() {
  cat <<'EOF'
start.sh — betting_features_oss scenario bootstrap

  ./start.sh                  Complete mode. Start every container, including the
                              lakehouse (Lakekeeper, MinIO), create the tiered Fluss
                              log tables and submit 12 streaming jobs: tiering,
                              ingest (Kafka → Fluss), A, B1, B2, B3, C1, C2, D reading
                              Fluss, the new feature's job E (avg_stake_7d,
                              08_add_feature_avg_stake.sql), risk-scoring-kafka-v2
                              and risk-score-log-postgres.

  ./start.sh --simple         Simple mode. The feature and scoring jobs read the Kafka
                              topics directly: no ingest, no Fluss log tables, no
                              lakehouse containers and no avg_stake_7d. A, B1, B2,
                              B3, C1, C2, D, risk-scoring-kafka and
                              risk-score-log-postgres (9 jobs).

  ./start.sh --services-only  Start the containers and install the connector JARs,
                              then stop. No DDL, no jobs; in complete mode the tiering
                              service is deployed but not started — build the pipeline
                              in the browser (http://localhost:8084/pipeline), or run
                              the commands printed at the end by hand.

  ./start.sh --reset          Remove every container and volume first: Fluss tables,
                              the Iceberg lake, Kafka topics, PostgreSQL, checkpoints
                              and running jobs are all rebuilt. Needed to switch modes.
                              Combine with --services-only for a clean, empty
                              environment.

  -h, --help                  Show this.

Memory defaults target a 64 GB host. Override for a laptop:
  TM_PROCESS_SIZE=5g JM_PROCESS_SIZE=2g TASK_SLOTS=16 TM_FRAMEWORK_OFFHEAP=512m ./start.sh
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    --services-only) SERVICES_ONLY=1 ;;
    --simple)        SIMPLE=1 ;;
    --reset)         RESET=1 ;;
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

# ── Mode ──────────────────────────────────────────────────────────────────────
# Written to .env, which Compose reads on every command: a later `docker compose up` or
# `restart` keeps the mode. COMPOSE_PROFILES starts the lakehouse containers; LAKEHOUSE
# gives the Fluss servers their datalake config and tells the editor what to show.

PROJECT="betting-features-oss"
if [ "$SIMPLE" = "1" ]; then MODE=simple; else MODE=complete; fi

# A running stack's mode is whether it has a Lakekeeper container. The Fluss servers'
# config and the log tables' properties differ between the modes, so switching needs a
# clean start.
if [ "$RESET" = "0" ] && [ -n "$(docker ps -aq --filter "label=com.docker.compose.project=$PROJECT")" ]; then
  if [ -n "$(docker ps -aq --filter "label=com.docker.compose.project=$PROJECT" \
               --filter "label=com.docker.compose.service=lakekeeper")" ]; then
    CURRENT=complete
  else
    CURRENT=simple
  fi
  if [ "$CURRENT" != "$MODE" ]; then
    echo "ERROR: the stack is running in $CURRENT mode. Switching to $MODE mode needs a clean start:"
    if [ "$MODE" = "simple" ]; then echo "  ./start.sh --simple --reset"; else echo "  ./start.sh --reset"; fi
    exit 1
  fi
fi

if [ "$MODE" = "complete" ]; then
  printf 'COMPOSE_PROFILES=lakehouse\nLAKEHOUSE=1\n' > .env
else
  printf 'COMPOSE_PROFILES=\nLAKEHOUSE=0\n' > .env
fi
echo "==> Mode: $MODE"

# ── Step 1: Download connector JARs ───────────────────────────────────────────

# Always run it: it skips every JAR already present, and a lib/ populated by an older
# version of this repo lacks the lake and tiering JARs.
echo "==> Checking connector JARs..."
bash download-jars.sh >/dev/null
echo "    $(ls lib/*.jar 2>/dev/null | wc -l | tr -d ' ') JARs in lib/, $(ls lib/tiering/*.jar 2>/dev/null | wc -l | tr -d ' ') in lib/tiering/"

# ── Step 2: Start Docker Compose services ─────────────────────────────────────

if [ "$RESET" = "1" ]; then
  echo ""
  echo "==> --reset: removing containers and every volume..."
  # Every bit of state is a named volume: PostgreSQL, the Flink checkpoints, Fluss's
  # remote data, the Iceberg files on MinIO and Lakekeeper's catalog database.
  # The lakehouse profile too, so a complete-mode stack's lake containers go whatever the
  # new mode is.
  COMPOSE_PROFILES=lakehouse docker compose down -v --remove-orphans
fi

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
[ "$MODE" = "complete" ] && wait_healthy lakekeeper 24
wait_healthy coordinator-server 24
wait_healthy jobmanager 24
wait_healthy sql-gateway 24

# The coordinator only starts once lakekeeper-warehouse has run, but that one-shot exits 0
# even when its POST fails (a re-run gets a harmless 4xx). Check the warehouse is there:
# without it the first tiering commit fails.
if [ "$MODE" = "complete" ]; then
  echo -n "    lakehouse warehouse "
  if curl -sf http://localhost:8181/management/v1/warehouse | grep -q '"name":"lakehouse"'; then
    echo " OK"
  else
    echo " MISSING"
    echo "ERROR: Lakekeeper has no 'lakehouse' warehouse — see: docker compose logs lakekeeper-warehouse"
    exit 1
  fi
fi

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

# ── Step 4: Deploy the tiering service's JAR ─────────────────────────────────
# Complete mode, with or without --services-only. The SQL editor uploads it to the
# JobManager, so it is listed under Submit New Job in the Flink UI and the Pipeline
# Builder's tiering Start button can run it. Uploaded JARs live in the JobManager's /tmp:
# a JobManager restart forgets them, and the Start button uploads again when that happens.

if [ "$MODE" = "complete" ]; then
  echo ""
  echo -n "==> Deploying the tiering service JAR "
  for _ in $(seq 1 30); do
    curl -sf http://localhost:8084/api/config >/dev/null 2>&1 && break
    echo -n "."; sleep 2
  done
  if curl -sf -X POST http://localhost:8084/api/tiering/deploy >/dev/null; then
    echo " OK (not started)"
  else
    echo " FAILED — start it later with: docker compose exec jobmanager /opt/tiering.sh"
  fi
fi

# ── Step 5: Submit Flink SQL (setup + feature engineering jobs) ───────────────

if [ "$SERVICES_ONLY" = "1" ]; then
  echo ""
  echo "==> [--services-only] Skipping DDL and job submission."
  echo "    The commands to run them by hand are printed below."
  RUNNING_JOBS=0
else
  echo ""
  echo "==> Submitting Flink SQL via SQL Client..."
  if [ "$MODE" = "complete" ]; then
    EXPECTED_JOBS=12
    echo "    This submits 12 streaming jobs:"
    echo "      Fluss Lake Tiering Service         — Fluss log tables → Iceberg"
    echo "      ingest-kafka-to-fluss              — Kafka topics → Fluss log tables"
    echo "      A, B1, B2, B3, C1, C2, D          — feature engineering (from Fluss) → Fluss"
  else
    EXPECTED_JOBS=9
    echo "    This submits 9 streaming jobs:"
    echo "      A, B1, B2, B3, C1, C2, D          — feature engineering (from Kafka) → Fluss"
  fi
  if [ "$MODE" = "complete" ]; then
    echo "      E                                  — avg_stake_7d, bootstrapped from the lake"
    echo "      risk-scoring-kafka-v2              — bets ⋈ feature store → risk_scores topic"
  else
    echo "      risk-scoring-kafka                 — bets ⋈ feature store → risk_scores topic"
  fi
  echo "      risk-score-log-postgres            — risk_scores → PostgreSQL (Grafana reads this)"
  echo ""

  # Tiering first: it picks up every table with 'table.datalake.enabled', including the
  # ones 03 is about to create, and commits once per checkpoint from then on.
  if [ "$MODE" = "complete" ]; then
    echo "    Starting the tiering service..."
    docker compose exec -T jobmanager /opt/tiering.sh 2>&1 | tail -1 || {
      echo "WARNING: the tiering service did not start — retry with: docker compose exec jobmanager /opt/tiering.sh"
    }
  fi

  # The path to run a job file from. Complete mode: the file itself. Simple mode: a copy
  # in the sql-client container whose event views read the Kafka topics, not the Fluss
  # log tables (scripts/kafka-sources.sh; see 05_'s header).
  event_source() {
    if [ "$MODE" = "complete" ]; then
      echo "/opt/flink_sql/$1"
    else
      scripts/kafka-sources.sh "$1" | docker compose exec -T sql-client sh -c "cat > /tmp/$1"
      echo "/tmp/$1"
    fi
  }

  # The SQL Client processes SQL files sequentially in one session.
  # --init registers catalogs (session-scoped); -f executes statements and submits streaming jobs.
  # Streaming INSERT jobs are non-blocking: the client submits them and moves to the next statement.

  # Step A+B: create the Fluss feature store (DDL only — completes quickly)
  echo "    Running setup DDL (catalogs, feature store)..."
  docker compose exec -T sql-client \
    /opt/flink/bin/sql-client.sh embedded \
    --init /opt/flink_sql/01_sources_and_catalogs.sql \
    -f /opt/flink_sql/02_fluss_feature_store.sql \
    2>&1 | tee /tmp/flink-sql-setup.log || {
    echo ""
    echo "WARNING: DDL submission had errors — check /tmp/flink-sql-setup.log"
  }

  # Complete mode: the tiered log tables and the ingest job, before any feature job —
  # they all read the Fluss log tables it fills.
  if [ "$MODE" = "complete" ]; then
    echo "    Creating the Fluss log tables, submitting the ingest job (Kafka → Fluss)..."
    docker compose exec -T sql-client \
      /opt/flink/bin/sql-client.sh embedded \
      --init /opt/flink_sql/01_sources_and_catalogs.sql \
      -f /opt/flink_sql/03_ingest_to_fluss.sql \
      2>&1 | tee /tmp/flink-sql-ingest.log || {
      echo ""
      echo "WARNING: Ingest job submission had errors — check /tmp/flink-sql-ingest.log"
    }
  fi

  # Step C: submit the CDC dimensions job first — it populates kyc_tier / balance /
  # lifetime_deposits, and on a cold start it snapshots both PostgreSQL tables before any
  # row reaches Fluss.
  echo "    Submitting CDC dimensions job (A)..."
  docker compose exec -T sql-client \
    /opt/flink/bin/sql-client.sh embedded \
    --init /opt/flink_sql/01_sources_and_catalogs.sql \
    -f /opt/flink_sql/04_cdc_dimensions.sql \
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
    -f "$(event_source 05_feature_calculations.sql)" \
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
    -f "$(event_source 06_risk_scoring_inference.sql)" \
    2>&1 | tee /tmp/flink-sql-risk.log || {
    echo ""
    echo "WARNING: Risk scoring submission had errors — check /tmp/flink-sql-risk.log"
    echo "You can also submit manually (see the 'Manual SQL setup' section below)."
  }

  # Complete mode: add the twelfth feature, exactly as the walkthrough in 08 does it —
  # retire the scoring job, then ALTER the store, deploy job E and the scoring job that
  # uses the new feature. Two scorers would write every bet to risk_scores twice.
  if [ "$MODE" = "complete" ]; then
    echo "    Adding avg_stake_7d (ALTER, job E, risk-scoring-kafka-v2)..."
    for jid in $(curl -s http://localhost:8081/jobs/overview \
                 | jq -r '.jobs[] | select(.name == "risk-scoring-kafka" and .state == "RUNNING") | .jid'); do
      curl -sf -X PATCH "http://localhost:8081/jobs/$jid?mode=cancel" >/dev/null || true
    done
    docker compose exec -T sql-client \
      /opt/flink/bin/sql-client.sh embedded \
      --init /opt/flink_sql/01_sources_and_catalogs.sql \
      -f /opt/flink_sql/08_add_feature_avg_stake.sql \
      2>&1 | tee /tmp/flink-sql-feature.log || {
      echo ""
      echo "WARNING: avg_stake_7d submission had errors — check /tmp/flink-sql-feature.log"
    }
  fi

  echo ""
  echo "==> Flink jobs submitted. Verifying..."
  sleep 5
  # Pipeline jobs only: an open Pipeline Builder runs short SELECT probes of its own.
  RUNNING_JOBS=$(curl -s http://localhost:8081/jobs/overview \
    | jq -r '.jobs[] | select(.state == "RUNNING" and (.name | test("^\\s*SELECT") | not)) | .jid' | wc -l | tr -d ' ')
  echo "    Running Flink jobs: $RUNNING_JOBS  (expected $EXPECTED_JOBS)"
fi

# ── Step 6: Status summary ────────────────────────────────────────────────────

echo ""
echo "══════════════════════════════════════════════════════════════"
if [ "$SERVICES_ONLY" = "1" ]; then
  echo "  Betting Feature Store OSS — Services Started, $MODE mode (no jobs)"
else
  echo "  Betting Feature Store OSS — Setup Complete, $MODE mode"
fi
echo "══════════════════════════════════════════════════════════════"
echo ""
echo "  Services:"
echo "    Environment homepage →  http://localhost        (start here; links to everything)"
echo "    Pipeline Builder    →  http://localhost:8084/pipeline  (the diagram as buttons)"
echo "    SQL Editor          →  http://localhost:8084   (Flink SQL in the browser)"
echo "    Flink Web UI        →  http://localhost:8081"
echo "    Inference service   →  http://localhost:9051"
echo "    Outliner generator  →  http://localhost:9050"
echo ""
echo "    Grafana             →  http://localhost:3000"
echo "    Redpanda Console    →  http://localhost:8082"
if [ "$MODE" = "complete" ]; then
  echo "    Lakekeeper          →  http://localhost:8181/ui  (Iceberg catalog)"
  echo "    MinIO console       →  http://localhost:9001  (admin / password)"
fi
echo ""
echo "  Kafka (from host):    localhost:19092"
echo "  Fluss (from host):    localhost:9123"
echo "  PostgreSQL (from host): localhost:5432"
echo ""
if [ "$SERVICES_ONLY" = "1" ]; then
  echo "  ─────────────────────────────────────────────────────────────"
  echo "  NO DDL OR JOBS WERE SUBMITTED (--services-only)."
  echo "  Build the pipeline in the browser: http://localhost:8084/pipeline"
  echo "  — or run the steps below in order."
  echo "  ─────────────────────────────────────────────────────────────"
else
  echo "  Manual SQL — to re-run a step, or if auto-submission failed:"
fi
echo ""
echo "  Every command below is copy-pasteable from this directory."
echo "  01_ is always passed as --init: it registers the Fluss catalog, the Kafka and"
echo "  CDC source tables, and the table.exec.source.idle-timeout that keeps event-time"
echo "  windows from stalling. Nothing works without it."
echo ""
if [ "$MODE" = "complete" ]; then
  echo "  1. Tiering service — Fluss log tables → Iceberg (one job for every tiered table):"
  echo "     docker compose exec jobmanager /opt/tiering.sh"
  echo ""
fi
echo "  2. DDL — the Fluss feature store (run once, idempotent):"
echo "     docker compose exec -T sql-client /opt/flink/bin/sql-client.sh embedded \\"
echo "       --init /opt/flink_sql/01_sources_and_catalogs.sql \\"
echo "       -f    /opt/flink_sql/02_fluss_feature_store.sql"
echo ""
if [ "$MODE" = "complete" ]; then
  echo "  3. Ingest — the tiered Fluss log tables, then Kafka topics → them (1 job; before 5 and 6):"
  echo "     docker compose exec -T sql-client /opt/flink/bin/sql-client.sh embedded \\"
  echo "       --init /opt/flink_sql/01_sources_and_catalogs.sql \\"
  echo "       -f    /opt/flink_sql/03_ingest_to_fluss.sql"
  echo ""
fi
echo "  4. Job A — PostgreSQL CDC → dimension columns (it snapshots first):"
echo "     docker compose exec -T sql-client /opt/flink/bin/sql-client.sh embedded \\"
echo "       --init /opt/flink_sql/01_sources_and_catalogs.sql \\"
echo "       -f    /opt/flink_sql/04_cdc_dimensions.sql"
echo ""
# Steps 5 and 6 read the events from Fluss (complete) or from Kafka (simple).
job_cmd() {
  if [ "$MODE" = "complete" ]; then
    echo "     docker compose exec -T sql-client /opt/flink/bin/sql-client.sh embedded \\"
    echo "       --init /opt/flink_sql/01_sources_and_catalogs.sql \\"
    echo "       -f    /opt/flink_sql/$1"
  else
    echo "     scripts/kafka-sources.sh $1 \\"
    echo "       | docker compose exec -T sql-client sh -c 'cat > /tmp/job.sql && \\"
    echo "           /opt/flink/bin/sql-client.sh embedded \\"
    echo "             --init /opt/flink_sql/01_sources_and_catalogs.sql -f /tmp/job.sql'"
  fi
}
if [ "$MODE" = "complete" ]; then SRC="from the Fluss log tables"; else SRC="from the Kafka topics"; fi
echo "  5. Jobs B1,B2,B3,C1,C2,D — windowed features $SRC → Fluss (6 jobs):"
job_cmd 05_feature_calculations.sql
echo ""
echo "  6. Risk scoring — bets ($SRC) ⋈ feature store → Kafka, then → PostgreSQL (2 jobs):"
job_cmd 06_risk_scoring_inference.sql
echo "     # Without this, public.risk_score_log stays empty and Grafana shows nothing."
echo ""
if [ "$MODE" = "complete" ]; then
  echo "  7. Ad-hoc analytics and point-in-time queries (SELECT-only, nothing to deploy):"
else
  echo "  7. Ad-hoc analytics (SELECT-only; its point-in-time section needs complete mode):"
fi
echo "     docker compose exec -it sql-client /opt/flink/bin/sql-client.sh embedded \\"
echo "       --init /opt/flink_sql/01_sources_and_catalogs.sql"
echo "     # then paste from flink_sql/07_analytics_and_monitoring.sql"
echo ""
if [ "$MODE" = "complete" ]; then
  echo "  8. Add a feature — avg_stake_7d: ALTER TABLE, then a job that bootstraps from the"
  echo "     Iceberg history and continues on live events, then the scoring job that uses it."
  echo "     Cancel risk-scoring-kafka first: step 3 of the file replaces it."
  echo "     (Already done above unless --services-only.)"
  echo "     docker compose exec -T sql-client /opt/flink/bin/sql-client.sh embedded \\"
  echo "       --init /opt/flink_sql/01_sources_and_catalogs.sql \\"
  echo "       -f    /opt/flink_sql/08_add_feature_avg_stake.sql"
  echo ""
fi
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
echo "  Start again from nothing (Fluss, the lake, Kafka, PostgreSQL all wiped):"
echo "    ./start.sh --reset --services-only"
echo "  Switch mode (needs --reset):"
echo "    ./start.sh --reset            # complete"
echo "    ./start.sh --reset --simple   # simple"
echo ""
echo "  Stop (keeps the volumes, but Fluss tables and Kafka topics are lost — see README):"
echo "    docker compose down"
echo ""
echo "  Destroy everything:"
echo "    ./destroy.sh            # containers + volumes + network"
echo "    ./destroy.sh --all -y   # also lib/ and the images"
echo "══════════════════════════════════════════════════════════════"
