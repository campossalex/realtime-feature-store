# Realtime Feature Store

Real-time feature engineering for a sports betting platform using **Apache Flink + Apache Fluss**, fully self-contained with Docker Compose.

The data generators and PostgreSQL schema live in this directory (`datagen/`, `pg_betting_ddl.sql`,
`pg_betting_sink_ddl.sql`) rather than being shared with the VVP scenario, so they can be changed here
without affecting it. Two things are still shared: the inference service build context and the Grafana
dashboards.

---

## Architecture

```
                        ┌──────────────────────────────────────────┐
   PostgreSQL  ─── CDC ─►                                          │
                        │        Flink SQL Session Cluster          │
   Redpanda             │                                          │
   (Kafka)    ─ Kafka ─►   7 streaming jobs (A, B1, B2, B3, C1,  │
                        │   C2, D) — each writes only its columns  │
                        │                      │                   │
                        └──────────────────────┼───────────────────┘
                                               │ partial column INSERT
                                               ▼
                              ┌────────────────────────────────┐
                              │  Fluss PK Table                │  ← online store
                              │  fluss.betting.                │    O(1) lookup
                              │  player_feature_store          │    sub-20 ms
                              └───────────────┬────────────────┘
                                              │  pyfluss
                                              │  (Python client)
                                              ▼
                                     ┌───────────────────┐
                                     │ Inference Service │
                                     │  POST /score      │
                                     │  port 9051        │
                                     └───────────────────┘
```

The risk-scoring job (`05_risk_scoring_inference.sql`) reads the same table from inside the
cluster via a Flink temporal join (`FOR SYSTEM_TIME AS OF proc_time`) — see the Readers row
below.

Fluss is the only store — there is no cold/lakehouse tier. See
[No lakehouse tier](#no-lakehouse-tier) for what that costs and why it was removed.

**Key properties of the Fluss PK table:**

| Property | Detail |
|----------|--------|
| Primary key | `player_id` |
| Writers | 7 independent Flink jobs — each writes only its own columns |
| Readers | Flink temporal join (sub-ms) + pyfluss REST API (5–20 ms) |
| Zero-fill | Double-HOP LEFT JOIN resets stale features to `0` when a player goes quiet |
| Loss streak | MATCH_RECOGNIZE on `bet_settlements` — fires on each pattern `L+ W` |
| Cold storage | None. Fluss holds current state only; no tiering, no time travel |

---

## Prerequisites

| Tool | Version |
|------|---------|
| Docker + Docker Compose v2 | `docker compose version` ≥ 2.20 |
| `curl`, `jq` | Any recent version |
| RAM available to Docker | 40 GB at the defaults; ~10 GB works — see [Running on a laptop](#running-on-a-laptop) |

### Host sizing and TaskManager memory

The compose file is tuned for a **64 GB host**. The TaskManager takes
`taskmanager.memory.process.size: 24g` and the JobManager 4 GB; everything else — ZooKeeper,
the two Fluss servers, Redpanda (pinned at 1 GB), PostgreSQL, the two generators, the
inference service, Grafana and Redpanda Console — comes to roughly 10 GB. That leaves ~25 GB
of headroom on a 64 GB box.

**Do not read process size as heap.** `state.backend: filesystem` means HashMapStateBackend,
so *all* operator state lives on the task heap — and only a fraction of process memory ends
up there. The original `4g` setting resolved to:

```
taskmanager.memory.task.heap.size = 1127428886b   (1.05 GB)
taskmanager.memory.managed.size   = 1438814063b   (1.34 GB)
Maximum heap size: 1204 MiBytes
```

Barely 1 GB of usable heap for seven windowed / join / MATCH_RECOGNIZE jobs, with more than
that parked in managed memory HashMapStateBackend never touches. State grows monotonically
and the heap is gone in roughly half an hour.

What that failure looks like is misleading. The TaskManager does not throw
`OutOfMemoryError` — it GC-thrashes to a standstill, stops answering RPC, and the JobManager
reports:

```
Triggering Checkpoint N ... failed due to java.util.concurrent.TimeoutException
Heartbeat of TaskManager with id <id> timed out
```

followed by every job failing and the UI showing `Registered TMs: 0`. That reads like a dead
container or a host OOM-kill, and it is neither. Rule the host out first:

```bash
docker inspect betting-features-oss-taskmanager-1 --format '{{.State.OOMKilled}} {{.State.ExitCode}}'
```

`false` with a non-137 exit code means the host had memory to spare and the problem is inside
the JVM. Two settings address it:

| Setting | Value | Effect |
|---------|-------|--------|
| `taskmanager.memory.process.size` | `24g` | Total TM footprint |
| `taskmanager.memory.managed.fraction` | `0.1` | Down from the 0.4 default, which would strand ~9 GB in managed memory. Redirects it to the task heap. Raise it again if you switch to RocksDB, which *does* use managed memory. |

Always confirm the resolved split rather than assuming it:

```bash
docker compose logs taskmanager | grep -iE "task.heap.size|managed.size"
```

### Running on a laptop

The defaults target a 64 GB server. A laptop's Docker VM is usually 8–12 GB, where a 24 GB
TaskManager (`-Xmx17.6g`) will thrash. Four environment variables override the sizing — no
file edits, same compose file:

```bash
TM_PROCESS_SIZE=5g JM_PROCESS_SIZE=2g TASK_SLOTS=12 TM_FRAMEWORK_OFFHEAP=512m ./start.sh
```

| Variable | Default (server) | Laptop |
|----------|------------------|--------|
| `TM_PROCESS_SIZE` | `24g` | `5g` |
| `JM_PROCESS_SIZE` | `4g` | `2g` |
| `TASK_SLOTS` | `16` | `12` |
| `TM_FRAMEWORK_OFFHEAP` | `1g` | `512m` |

Verified end-to-end on a 9.95 GB Docker VM: all 7 jobs RUNNING, all 7 checkpointing, features
populating. Resolved memory and actual usage:

```
taskmanager.memory.task.heap.size = 2.77 GB     (vs 1.05 GB at the old 4g default)
taskmanager.memory.managed.size   = 0.43 GB
Total container RSS across the stack: ~5.1 GB of 9.95 GB
```

Keep `managed.fraction` at `0.1` when scaling down — that ratio, not the total, is what makes
the heap usable. Below about `4g` for `TM_PROCESS_SIZE` you are back under 2 GB of task heap
and the 30-minute death returns.

Give `TASK_SLOTS` some slack: the 7 feature jobs hold one slot each for their lifetime, plus
one for `05_risk_scoring_inference.sql`, and the batch `SELECT`s in `06_` need a free slot or
they queue forever. At `TASK_SLOTS=8` the cluster sits at 1 free slot with just the 7 jobs
running. Slots cost almost nothing here — they only subdivide managed memory — so 12 is a
safer floor than 8.

**On any other host size**, scale `process.size` down and keep `managed.fraction` at `0.1`.
`8g` yields roughly 5 GB of task heap.

### Surviving a TaskManager loss

Four settings work together so that losing the TaskManager is a blip rather than a total loss:

| Setting | Where | Why |
|---------|-------|-----|
| `restart: unless-stopped` | `jobmanager`, `taskmanager` | A killed container comes back. Without it the cluster sits at `Registered TMs: 0` forever. |
| `restart-strategy: exponential-delay` | both, **and `sql-client`** | Jobs restore from the last checkpoint instead of failing outright. |
| `execution.checkpointing.interval: 60s` | **`sql-client`**, not just `jobmanager` | Checkpointing is baked into the JobGraph by whoever submits it. |
| `flink-checkpoints` shared volume | `jobmanager`, `taskmanager` | `state.checkpoints.dir` is a `file://` path; the JM writes `_metadata` and the TM writes the state files it references. |

The `sql-client` rows are the subtle ones. That container never runs
`/docker-entrypoint.sh`, so it ignores `FLINK_PROPERTIES` and gets its config appended to
`config.yaml` by its `command` instead. When those keys were only on the `jobmanager`, all six
feature jobs ran with **checkpointing disabled** — and Flink's default restart strategy with
checkpointing off is *no restart*. One TaskManager loss failed all seven jobs permanently with
`Recovery is suppressed by NoRestartBackoffTimeStrategy`. The since-removed tiering job was the
tell: `start.sh` submitted it from the `jobmanager` container, so it did inherit checkpointing,
and it alone went to RESTARTING rather than FAILED.

To check that the feature jobs really are checkpointing:

```bash
docker compose logs jobmanager | grep -c "Completed checkpoint"
```

Count it against the number of running jobs — one per job per minute. A count of zero means the
`sql-client` config block did not take effect.

---

## Quick Start

```bash
git clone https://github.com/campossalex/realtime-feature-store

cd realtime-feature-store

# 1. Download connector JARs (one-time, ~92 MB)
./download-jars.sh

# 2. Start everything — services, DDL and all 9 streaming jobs
./start.sh

#    ...or bring up only the containers and drive the SQL yourself.
#    Prints every DDL / job command, ready to paste.
./start.sh --services-only

# 3. Trigger a fraud outliner and watch the risk score flip
curl -X POST http://localhost:9050/trigger \
  -H "Content-Type: application/json" \
  -d '{"archetype": "BOT"}'
```

---

## Step-by-Step Setup

### 1. Download JARs

```bash
./download-jars.sh
```

Downloads to `./lib/`. Safe to re-run — already-present JARs are skipped. Files needed:

- `fluss-flink-1.20-0.9.1-incubating.jar` — Fluss catalog + connector
- `flink-sql-connector-kafka-3.3.0-1.20.jar` — Kafka source/sink
- `flink-sql-connector-postgres-cdc-3.2.1.jar` — PostgreSQL CDC source
- `flink-connector-jdbc-3.4.0-1.20.jar` — JDBC catalog (for PostgreSQL sink)
- `postgresql-42.7.4.jar` — PostgreSQL JDBC driver

Five JARs, ~92 MB — down from 295 MB. Removing the lakehouse tier dropped seven: the Fluss tiering and
`fluss-lake-iceberg` JARs, `fluss-fs-s3`, the three Iceberg runtime/AWS JARs, and the
`flink-shaded-hadoop-2-uber` that existed only so Iceberg's catalog factory could construct
a Hadoop `Configuration`. `download-jars.sh` deletes those seven if a `lib/` populated before
the removal still has them — `start.sh` copies every `lib/*.jar` into the Flink containers, so
leaving them would keep loading Iceberg and Hadoop into every JVM.

### 2. Start Docker Compose

```bash
docker compose up -d
```

Services and their host ports:

| Service | URL / Port |
|---------|-----------|
| Flink Web UI | http://localhost:8081 |
| Inference API | http://localhost:9051 |
| Outliner UI | http://localhost:9050 |
| Grafana | http://localhost:3000 |
| Redpanda Console | http://localhost:8082 |
| Redpanda (Kafka) | `localhost:19092` |
| Fluss | `localhost:9123` |
| PostgreSQL | `localhost:5432` |

### 3. Submit Flink SQL jobs

The automated path (run by `start.sh`):

```bash
# DDL: register catalogs + create Fluss PK table
docker compose exec sql-client /opt/flink/bin/sql-client.sh \
  --init /opt/flink_sql/01_sources_and_catalogs.sql \
  -f    /opt/flink_sql/02_fluss_feature_store.sql

# CDC dimensions (Job A) — run first; cold start snapshots both PostgreSQL tables
docker compose exec sql-client /opt/flink/bin/sql-client.sh \
  --init /opt/flink_sql/01_sources_and_catalogs.sql \
  -f    /opt/flink_sql/03_cdc_dimensions.sql

# Feature calculations (Jobs B1, B2, B3, C1, C2, D) — submits 6 jobs at once
docker compose exec sql-client /opt/flink/bin/sql-client.sh \
  --init /opt/flink_sql/01_sources_and_catalogs.sql \
  -f    /opt/flink_sql/04_feature_calculations.sql

# Risk scoring job (Pipeline 2)
docker compose exec sql-client /opt/flink/bin/sql-client.sh \
  --init /opt/flink_sql/01_sources_and_catalogs.sql \
  -f    /opt/flink_sql/05_risk_scoring_inference.sql
```

On a memory-constrained host, submit `03_` on its own and hold off on `04_` — or submit `04_`
and cancel the jobs you don't need from the Flink UI. Each job is independent and writes
only its own columns.

Or interactively (paste statements directly — `EXECUTE FILE` only works for DML, not DDL):

```bash
docker compose exec -it sql-client /opt/flink/bin/sql-client.sh
# Inside the client, paste each CREATE CATALOG / CREATE TABLE statement directly.
# To open a session pre-loaded with catalogs and sources, use --init from the shell (above).
```

After successful submission, `http://localhost:8081` shows **9 running jobs**:

| Job | Source file | Writes to |
|-----|-------------|-----------|
| `jobA-cdc-dimensions` | `03_` | Fluss — `kyc_tier`, `balance`, `lifetime_deposits` |
| `jobB1`, `jobB2`, `jobB3`, `jobC1`, `jobC2`, `jobD` | `04_` | Fluss — windowed feature columns |
| `risk-scoring-kafka` | `05_` | Kafka `risk_scores` — bets ⋈ feature store |
| `risk-score-log-postgres` | `05_` | **PostgreSQL `public.risk_score_log`** |

The last two matter more than they look: `public.risk_score_log` is the only table the
Grafana dashboard queries (11 of its panels hit it), so without them the dashboard is blank
and nothing reaches PostgreSQL. `start.sh` used to submit `04_` but not `05_`, which is
exactly that situation.

### `--services-only`

```bash
./start.sh --services-only
```

Starts every container and installs the connector JARs, then stops — no DDL, no jobs. The
Fluss database is left empty. Use it when you want to run the SQL by hand, demo the steps
live, or submit a subset. The exact commands for each step are printed at the end, and the
same list is printed on a normal run so you can re-run any single step.

Expect rows in `risk_score_log` to appear slowly at first. The lookup join is point-in-time
against Fluss, so a bet only scores once that player already has a feature vector; throughput
climbs as the feature store fills. Measured `enrichment_lag_ms` is ~50-60 ms end to end.

### No lakehouse tier

This scenario has no Iceberg/MinIO cold tier. It was removed along with the `minio`,
`minio-init` and `iceberg-rest` services, the Flink `iceberg` catalog, the `fs.s3a.*` config,
seven JARs, and the tiering job `start.sh` used to submit.

**Why.** It cost real memory and delivered nothing. `datalake.format` was never actually
enabled in `conf/fluss-server.yaml` — it is commented out, and for a good reason documented
there: setting it rewrites the primary-key encoding of every Fluss table to match the lake
format's bucketing, and Fluss 0.9.1's Python client has no Iceberg key encoder, so every
pyfluss point lookup fails with `KeyEncoder for Iceberg format is not yet implemented`. That
breaks the online feature store, which is the point of the demo, and it breaks silently as an
empty feature dict from `/score`.

So the tiering job ran against a cluster with no tierable tables. In a JobManager log from a
37-minute run it reported the same empty state every 30 seconds and never moved a byte:

```
TieringSourceEnumerator - currentFinishedTables: {}, currentFailedTableEpochs: {}, tieringTableEpochs: {}
```

Meanwhile MinIO, the REST catalog and the tiering job's slot were consuming memory on a host
that did not have it to spare.

**What you lose.** No `FOR SYSTEM_TIME AS OF` time travel and no point-in-time training
snapshots. Fluss holds the current feature vector per player and nothing older. For labelled
training rows, read the `bets` / `bet_settlements` topics out of Redpanda, which retain their
full history — see the closing section of `06_analytics_and_monitoring.sql`.

**Re-adding it** is not a config flag. You would need to restore the three services, the seven
JARs, the `iceberg` catalog in `01_sources_and_catalogs.sql`, the `datalake.*` server config,
and the tiering-job submission — and then solve the Python key-encoder problem, or accept that
the inference service returns empty features.

---

## Demo Walkthrough

### Trigger the fraud bot (BOT archetype)

```bash
# Via the outliner generator web UI
open http://localhost:9050

# Or via REST
curl -X POST http://localhost:9050/trigger \
  -H "Content-Type: application/json" \
  -d '{"archetype": "BOT", "player_id": "BOT_001"}'
```

BOT_001 will place 20 bets in under 10 minutes. The feature store updates within 10 seconds.

### Verify the feature store

```sql
-- In SQL Client:
SELECT player_id, bets_last_10min, geo_mismatches_15min, last_updated_at
FROM fluss.betting.player_feature_store
WHERE player_id = 'BOT_001';
```

Expected after ~30 s: `bets_last_10min > 8`.

### Query via the inference service

```bash
curl -X POST http://localhost:9051/score \
  -H "Content-Type: application/json" \
  -d '{"player_id": "BOT_001", "amount": 50}'
```

Expected response:
```json
{
  "player_id": "BOT_001",
  "risk_level": "FRAUD_HIGH",
  "risk_reasons": "BOT_VELOCITY",
  "features": { "bets_last_10min": 20, ... }
}
```

### Verify in Grafana

Open http://localhost:3000 → **Betting Features** dashboard. The `FRAUD_HIGH` count rises in real time as the bot bets arrive.

---

## SQL Files Reference

| File | What it does | Run once or continuously |
|------|-------------|--------------------------|
| `01_sources_and_catalogs.sql` | Registers the Fluss catalog; defines Kafka + CDC source tables and the JDBC sink | Once per session (re-run in each new SQL Client session) |
| `02_fluss_feature_store.sql` | `CREATE TABLE` for the Fluss PK table | Once (DDL is idempotent — uses `IF NOT EXISTS`) |
| `03_cdc_dimensions.sql` | 1 streaming INSERT job (A) — PostgreSQL CDC → dimension columns | Once — runs until stopped |
| `04_feature_calculations.sql` | 6 streaming INSERT jobs (B1, B2, B3, C1, C2, D) — windowed features | Once — runs until stopped |
| `05_risk_scoring_inference.sql` | Risk scoring → Kafka + PostgreSQL audit log | Once — runs until stopped |
| `06_analytics_and_monitoring.sql` | Ad-hoc SELECT queries for verification | On-demand |

---

## Teardown

```bash
./destroy.sh
```

Removes every container, volume and network in the project and prompts before it does. It
sweeps by Compose project label rather than by service name, so it also catches containers
whose service has since been renamed or removed from the compose file.

| Command | Effect |
|---------|--------|
| `./destroy.sh` | Containers, volumes, network, locally built inference image |
| `./destroy.sh --data` | Also `fluss-remote-data/` (Fluss KV snapshots) |
| `./destroy.sh --jars` | Also `lib/` — needs `./download-jars.sh` again (~92 MB) |
| `./destroy.sh --images` | Also the pulled images (~4 GB, shared with other OSS scenarios) |
| `./destroy.sh --all -y` | Everything, no prompt |

Both `--data` and `--jars` target git-ignored generated directories.

To pause without losing state, stop the containers but keep the volumes — PostgreSQL and the
Flink checkpoints both survive:

```bash
docker compose down
```

---

## Timestamps and timezones

Every container pins `TZ=UTC`, and the Flink services (including `sql-client`) pin
`table.local-time-zone: UTC`. Keep it that way.

The generators emit `event_time` as a **naive** string from `datetime.now()`, so the container's zone is
what defines its meaning. Flink parses it with `TO_TIMESTAMP(...)` into a naive `TIMESTAMP(3)`, while
`CURRENT_TIMESTAMP` and `PROCTIME()` are `TIMESTAMP_LTZ`. `05_risk_scoring_inference.sql` mixes the two —
in `enrichment_lag_ms` and in the loss-streak staleness guard — so those only produce correct values while
both sides agree on UTC. Setting a local timezone anywhere in the stack would make `enrichment_lag_ms`
report the offset (about 7,200,000 ms for UTC+2) and would break loss-streak decay silently.

**A Kafka record timestamp that looks two hours off is not a bug.** The record timestamp is epoch millis,
which Redpanda Console renders in your browser's timezone, while `event_time` in the payload is printed
verbatim as UTC. Compare the two by converting the epoch rather than reading the rendered clock:

```bash
docker compose exec redpanda rpk topic consume login_events -n 1 -o -1
```

The payload `event_time` and the converted record timestamp should be a few hundred milliseconds apart.

---

## Troubleshooting

**A feature takes minutes to update after triggering an outliner**

Almost always watermark lag, not slow processing. Every window job here is **event-time**
(`DESCRIPTOR(event_ts)`), so a window emits only once the watermark passes `window_end` —
a starved watermark stalls output no matter how idle the CPU is.

Every topic is created with **4 partitions** and the generators call `producer.send()` with
no key, so records round-robin across all four. A Flink Kafka source emits the **minimum**
watermark across its assigned partitions, so a single quiet partition pins the whole job.
`login_events` is the worst case: the baseline generator emits one login per ~50 iterations,
which is only one per 40–90 s *per partition*. `jobD-geo-mismatches` was measured running
70–245 s behind wall clock because of it.

`table.exec.source.idle-timeout` (set in `01_sources_and_catalogs.sql`) fixes this by marking
a split idle after 5 s and dropping it from the watermark minimum. Measured before and after
on `jobD-geo-mismatches`:

| | `watermarkLag` |
|---|---|
| Without idle-timeout | 70–245 s, fluctuating |
| With `idle-timeout = 5 s` | **2.0 s**, steady — the `INTERVAL '2' SECOND` bound |

Diagnose it on a running job. `pendingRecords: 0` with a large `watermarkLag` is the
signature — the source has read everything available and is still holding the watermark
back:

```bash
JOB=<job-id>
SRC=$(curl -s localhost:8081/jobs/$JOB | jq -r '.vertices[]|select(.name|startswith("Source"))|.id')
curl -s localhost:8081/jobs/$JOB/vertices/$SRC/metrics | jq -r '.[].id' | grep -E 'watermarkLag|pendingRecords|sourceIdleTime'
```

Then fetch those ids with `?get=<id>`. `currentEmitEventTimeLag` near 0 alongside a large
`watermarkLag` confirms it — records are arriving on time, the watermark is not.

Note this option is `table.exec.*`, so it is compiled into the plan **client-side**. Setting
it in the jobmanager's `FLINK_PROPERTIES` has no effect; it must be in the SQL session or the
`sql-client` config.

Expect ~15–25 s end-to-end after the fix: the HOP slide is 10 s, plus the 2 s watermark
delay, plus the Fluss write. Also check the archetype's own pacing before calling it slow —
`run_geo()` sleeps 35 s between the GB and Brazil logins, so the mismatch does not exist to
be detected until 35 s after you trigger it.

**`--init` fails with "Failed to initialize from sql script"**

The connector JARs in `./lib/` must be in `/opt/flink/lib/` inside the container, not a subdirectory. The `docker-compose.yml` entrypoint copies them on startup. If you started the stack without those entrypoints (older version), copy manually then retry:
```bash
docker compose exec sql-client  sh -c "cp /opt/flink/lib/extra/*.jar /opt/flink/lib/"
docker compose exec jobmanager  sh -c "cp /opt/flink/lib/extra/*.jar /opt/flink/lib/"
docker compose exec taskmanager sh -c "cp /opt/flink/lib/extra/*.jar /opt/flink/lib/"
```

**`docker compose logs sql-client` prints nothing**

Expected. The container's command is just `cp … ; sleep infinity`, so it writes nothing to stdout.
Everything you run via `docker compose exec` logs to a file *inside* the container instead:

```bash
docker compose exec sql-client tail -100 /opt/flink/log/flink--sql-client-sql-client.log
```

Jump straight to failures:

```bash
docker compose exec sql-client grep -nE "ERROR|Caused by" /opt/flink/log/flink--sql-client-sql-client.log | tail -20
```

**Flink jobs not appearing after SQL submission**

Read the log file above. The most common causes:

- `Connection refused: /0.0.0.0:8081` — the SQL Client isn't pointed at the JobManager. Its
  entrypoint bypasses `/docker-entrypoint.sh`, so `FLINK_PROPERTIES` is ignored; the compose file
  appends `rest.address: jobmanager` to `config.yaml` directly instead. Verify with
  `docker compose exec sql-client tail -3 /opt/flink/conf/config.yaml`.
- `Could not find any factory for identifier '…'` — a connector JAR is missing from
  `/opt/flink/lib/`. See the `--init` entry above.

**Fluss coordinator not starting**

Check ZooKeeper connection:
```bash
docker compose logs coordinator-server | tail -20
docker compose logs zookeeper | grep -i error
```

**Postgres CDC slot already exists**

If restarting after a crash, the replication slot may already exist. The `pg_cdc_setup.sql` script handles this with a `WHERE NOT EXISTS` guard. If the slot is stuck:
```bash
docker compose exec postgres psql -U root -d betting \
  -c "SELECT pg_drop_replication_slot('flink_cdc_betting_slot');"
```
Then restart the CDC jobs.

