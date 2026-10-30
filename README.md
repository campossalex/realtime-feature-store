# Realtime Feature Store

Real-time feature engineering for a sports betting platform using **Apache Flink + Apache Fluss**, fully self-contained with Docker Compose.

The data generators and PostgreSQL schema live in this directory (`datagen/`, `pg_betting_ddl.sql`,
`pg_betting_sink_ddl.sql`) rather than being shared with the VVP scenario, so they can be changed here
without affecting it. Two things are still shared: the inference service build context and the Grafana
dashboards.

---

## Architecture

```
   Redpanda (Kafka)                    Flink SQL session cluster
   bets, bet_settlements,  ──────►  ingest-kafka-to-fluss ──────►  Fluss log tables ──── tiering ───►  Iceberg lake
   login_events, deposits                                          fluss.betting.bets,     service      (Lakekeeper +
                                                                   bet_settlements, …                    MinIO)
                                                                          │                                  │
   PostgreSQL ── CDC ──► job A                                            │ 6 feature jobs                   │ union read
       (players, wallets)  │                                              │ (B1–D, per-event)                │ (complete
                           │                                              │                                  │  mode: job E)
                           ▼                                              ▼                                  ▼
                        ┌──────────────────────────────────────────────────────────────────────────────────────┐
                        │  Fluss PK table  fluss.betting.player_feature_store   ← online store, O(1) lookup     │
                        │  each job writes only its own columns (partial-column upserts)                       │
                        └───────────────────┬─────────────────────────────────────────────┬──────────────────────┘
                                            │ lookup FOR SYSTEM_TIME AS OF                │ pyfluss
                                            ▼                                             ▼
                                   risk-scoring-kafka → risk_scores → PostgreSQL     Inference service
                                   (06_)                                 → Grafana    POST /score · :9051
```

Every event lands in a **Fluss log table** first (`03_ingest_to_fluss.sql`), and every job reads
from there. The ingest job, the log tables, the lake, the tiering service and job E are the
**complete** pipeline; in `./start.sh --simple` the feature jobs read the Kafka topics directly
instead (see [Quick Start](#quick-start) and [Simple mode](#simple-mode-jobs-read-kafka)). The **tiering service** copies those log tables to **Iceberg**, so their full history
stays queryable, as of any past instant, long after Fluss's own retention — see
[Lakehouse tier and point-in-time queries](#lakehouse-tier-and-point-in-time-queries). A new
feature can start from that history instead of from zero — see
[Add a feature to the online store](#add-a-feature-to-the-online-store).

The risk-scoring job (`06_risk_scoring_inference.sql`) reads the feature store from inside the
cluster via a Flink temporal join (`FOR SYSTEM_TIME AS OF proc_time`) — see the Readers row
below.

**Key properties of the Fluss PK table:**

| Property | Detail |
|----------|--------|
| Primary key | `player_id` |
| Writers | 7 independent Flink jobs — each writes only its own columns |
| Readers | Flink temporal join (sub-ms) + pyfluss REST API (5–20 ms) |
| Freshness | Per-event: a processing-time OVER window rewrites the player's features on every bet, settlement or login |
| Zero-fill | A HOP window in the same job resets a feature to its default once the player goes quiet |
| Loss streak | MATCH_RECOGNIZE on `bet_settlements` — fires on each pattern `L+ W` |
| Cold storage | None for the feature store itself (current state only). In complete mode the event logs it is computed from are tiered to Iceberg |

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
inference service, Grafana, Redpanda Console, and the lake's MinIO, Lakekeeper and its
PostgreSQL — comes to roughly 10 GB. That leaves ~25 GB
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
TM_PROCESS_SIZE=5g JM_PROCESS_SIZE=2g TASK_SLOTS=16 TM_FRAMEWORK_OFFHEAP=512m ./start.sh
```

| Variable | Default (server) | Laptop |
|----------|------------------|--------|
| `TM_PROCESS_SIZE` | `24g` | `5g` |
| `JM_PROCESS_SIZE` | `4g` | `2g` |
| `TASK_SLOTS` | `16` | `16` |
| `TM_FRAMEWORK_OFFHEAP` | `1g` | `512m` |

Verified end-to-end on a 9.95 GB Docker VM: tiering, ingest, the 7 feature jobs, scoring and
the alert log RUNNING and checkpointing, plus job E and scoring v2. Total container memory
with 11 of those jobs: **~5.8 GB** of 9.95 GB, of which the lake (MinIO, Lakekeeper, its
PostgreSQL) is ~0.3 GB — the part `--simple` does not start. Resolved TaskManager memory and the earlier,
7-job measurement:

```
taskmanager.memory.task.heap.size = 2.77 GB     (vs 1.05 GB at the old 4g default)
taskmanager.memory.managed.size   = 0.43 GB
Total container RSS across the stack: ~5.1 GB of 9.95 GB
```

Keep `managed.fraction` at `0.1` when scaling down — that ratio, not the total, is what makes
the heap usable. Below about `4g` for `TM_PROCESS_SIZE` you are back under 2 GB of task heap
and the 30-minute death returns.

Give `TASK_SLOTS` some slack: `./start.sh` runs 12 jobs that each hold one slot for their
lifetime (tiering, ingest, the 7 feature jobs, job E, two scoring jobs; `--simple` runs 9),
and the batch `SELECT`s in `07_`, the Pipeline Builder's data probes and every streaming
*View data* need a free slot or they queue forever. Slots cost almost nothing here — they
only subdivide managed memory — so 16 is the floor rather than 12.

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
git clone https://github.com/campossalex/realtime-feature-store.git
cd realtime-feature-store

./start.sh --services-only
```

Then open **http://localhost** — the environment homepage, with a link to every tool.

`start.sh` downloads the JARs on first run, starts the stack and waits until every
service is healthy, then prints the URLs and a runbook of manual commands.

Two pipelines, picked with one flag:

| Pipeline | Command | What runs |
|---|---|---|
| **Complete** | `./start.sh` | The lakehouse (Lakekeeper, MinIO) and the tiering service, the 11 core features, and a twelfth, [`avg_stake_7d`](#add-a-feature-to-the-online-store), bootstrapped from the lake and used by `risk-scoring-kafka-v2`. **12 jobs.** |
| **Simple** | `./start.sh --simple` | The feature and scoring jobs read the **Kafka topics directly**: no ingest job, no Fluss log tables, no lakehouse containers, no tiering and no `avg_stake_7d`. Kafka → the 11 core features (in Fluss) → scoring. **9 jobs**, and fewer containers. |

Either combines with:

| Flag | Result | Use it for |
|---|---|---|
| `--services-only` | Everything running, nothing submitted; the Fluss database is empty; in complete mode the tiering service is deployed but not started | **A workshop** — attendees build the pipeline in the [Pipeline Builder](#browser-interfaces), which shows only that mode's boxes |
| `--reset` | Wipes every volume first: Fluss, the lake, Kafka, PostgreSQL, checkpoints | **Handing a clean environment** to the next group, and **switching modes** |

`start.sh` records the mode in `.env` (`COMPOSE_PROFILES`, `LAKEHOUSE`), so a later plain
`docker compose` command keeps it. Switching mode needs `--reset` — the Fluss servers' config
and the log tables differ — and `start.sh` stops with a message if you forget it:

```bash
./start.sh --reset --simple   # complete → simple
./start.sh --reset            # simple → complete
```

Then fire an outlier and watch its risk score flip: `curl -X POST http://localhost:9050/run/BOT`.

To stop:

```bash
docker compose down -v      # stop everything and delete its data
```

`./start.sh --reset` is the only clean restart — see [Teardown](#teardown) for why a plain
`down`/`up` is not.

---

## Step-by-Step Setup

### 1. Download JARs

```bash
./download-jars.sh
```

Downloads to `./lib/`. Safe to re-run — already-present JARs are skipped, and `start.sh` runs
it every time. Files needed:

- `fluss-flink-1.20-0.9.1-incubating.jar` — Fluss catalog + connector, including the union read
- `flink-sql-connector-kafka-3.3.0-1.20.jar` — Kafka source/sink
- `flink-sql-connector-postgres-cdc-3.2.1.jar` — PostgreSQL CDC source
- `flink-connector-jdbc-3.4.0-1.20.jar` — JDBC connector (for the PostgreSQL sink)
- `postgresql-42.7.4.jar` — PostgreSQL JDBC driver
- `fluss-lake-iceberg-0.9.1-incubating.jar`, `iceberg-aws-1.10.1.jar`, `iceberg-aws-bundle-1.10.1.jar`,
  `failsafe-3.3.2.jar` — Fluss ↔ Iceberg, with S3FileIO for MinIO (also copied into the Fluss
  servers' `plugins/iceberg/`)
- `iceberg-flink-runtime-1.20-1.10.1.jar` — the `iceberg` catalog, for time travel
- `hadoop-apache-3.3.5-2.jar` — Trino's relocated Hadoop, which Iceberg's code paths need; *not*
  `flink-shaded-hadoop-2-uber`, whose Avro 1.7 makes the tiering job restart-loop
- `tiering/fluss-flink-tiering-0.9.1-incubating.jar` — the tiering service. Not on any classpath:
  it is the main JAR of a Flink job (`scripts/tiering.sh`)

The Iceberg version must match the one inside `fluss-lake-iceberg` (1.10.1 for Fluss
0.9.1-incubating). Same versions as [streamhouse](https://github.com/campossalex/streamhouse).

### 2. Start Docker Compose

```bash
docker compose up -d
```

Services and their host ports:

| Service | URL / Port |
|---------|-----------|
| **Environment homepage** — start here; links to everything below, with live status | http://localhost |
| Pipeline Builder (interactive diagram) | http://localhost:8084/pipeline |
| Web SQL editor | http://localhost:8084 |
| Flink SQL Gateway (REST, behind the editor) | http://localhost:8083 |
| Flink Web UI | http://localhost:8081 |
| Inference API | http://localhost:9051 |
| Outliner UI | http://localhost:9050 |
| Grafana | http://localhost:3000 |
| Redpanda Console | http://localhost:8082 |
| Lakekeeper (Iceberg REST catalog, UI at `/ui`) | http://localhost:8181 |
| MinIO console (`admin` / `password`) | http://localhost:9001 |
| Redpanda (Kafka) | `localhost:19092` |
| Fluss | `localhost:9123` |
| PostgreSQL | `localhost:5432` |

### 3. Submit Flink SQL jobs

The automated path for complete mode (run by `start.sh`; the lines marked *complete* are
skipped by `--simple`, which runs 05 and 06 [reading Kafka](#simple-mode-jobs-read-kafka)):

```bash
# (complete) Tiering service: one Flink job for every table with 'table.datalake.enabled'
docker compose exec jobmanager /opt/tiering.sh

# DDL: register catalogs + create the Fluss PK table (the online feature store)
docker compose exec sql-client /opt/flink/bin/sql-client.sh \
  --init /opt/flink_sql/01_sources_and_catalogs.sql \
  -f    /opt/flink_sql/02_fluss_feature_store.sql

# (complete) Ingest: the four tiered Fluss log tables, then the Kafka topics → them
# (1 job) — before the feature jobs
docker compose exec sql-client /opt/flink/bin/sql-client.sh \
  --init /opt/flink_sql/01_sources_and_catalogs.sql \
  -f    /opt/flink_sql/03_ingest_to_fluss.sql

# CDC dimensions (Job A) — cold start snapshots both PostgreSQL tables
docker compose exec sql-client /opt/flink/bin/sql-client.sh \
  --init /opt/flink_sql/01_sources_and_catalogs.sql \
  -f    /opt/flink_sql/04_cdc_dimensions.sql

# Feature calculations (Jobs B1, B2, B3, C1, C2, D) — submits 6 jobs at once
docker compose exec sql-client /opt/flink/bin/sql-client.sh \
  --init /opt/flink_sql/01_sources_and_catalogs.sql \
  -f    /opt/flink_sql/05_feature_calculations.sql

# Risk scoring job (Pipeline 2)
docker compose exec sql-client /opt/flink/bin/sql-client.sh \
  --init /opt/flink_sql/01_sources_and_catalogs.sql \
  -f    /opt/flink_sql/06_risk_scoring_inference.sql

# (complete) Add avg_stake_7d: cancel risk-scoring-kafka first, then the ALTER, job E and
# risk-scoring-kafka-v2 — see "Add a feature to the online store"
docker compose exec sql-client /opt/flink/bin/sql-client.sh \
  --init /opt/flink_sql/01_sources_and_catalogs.sql \
  -f    /opt/flink_sql/08_add_feature_avg_stake.sql
```

#### Simple mode: jobs read Kafka

Simple mode has no Fluss log tables. The feature and scoring jobs read their events through
a temporary view each, and that view's one `FROM` line names the source:

```sql
CREATE TEMPORARY VIEW IF NOT EXISTS bets_live AS
SELECT *, PROCTIME() AS proc_time
FROM fluss.betting.bets /*+ OPTIONS('scan.startup.mode' = 'latest') */;     -- complete
-- FROM bets_kafka /*+ OPTIONS('scan.startup.mode' = 'latest-offset') */;   -- simple
```

`05_` and `06_` are written for complete mode. In simple mode `start.sh` and the SQL editor
swap that line — and only that one — for the Kafka table of `01_`
(`scripts/kafka-sources.sh` prints the swapped file). Everything after the view is the same
in both modes. By hand:

```bash
scripts/kafka-sources.sh 05_feature_calculations.sql \
  | docker compose exec -T sql-client sh -c 'cat > /tmp/job.sql && \
      /opt/flink/bin/sql-client.sh embedded \
        --init /opt/flink_sql/01_sources_and_catalogs.sql -f /tmp/job.sql'
```

On a memory-constrained host, submit `04_` on its own and hold off on `05_` — or submit `05_`
and cancel the jobs you don't need from the Flink UI. Each job is independent and writes
only its own columns.

Or interactively (paste statements directly — `EXECUTE FILE` only works for DML, not DDL):

```bash
docker compose exec -it sql-client /opt/flink/bin/sql-client.sh
# Inside the client, paste each CREATE CATALOG / CREATE TABLE statement directly.
# To open a session pre-loaded with catalogs and sources, use --init from the shell (above).
```

After successful submission, `http://localhost:8081` shows **12 running jobs** in complete
mode, **9** in simple mode:

| Job | Source file | Mode | Writes to |
|-----|-------------|------|-----------|
| `Fluss Lake Tiering Service - iceberg` | `scripts/tiering.sh` | complete | Iceberg — the four log tables, one snapshot per checkpoint |
| `ingest-kafka-to-fluss` | `03_` | complete | Fluss log tables `bets`, `bet_settlements`, `login_events`, `deposits` |
| `jobA-cdc-dimensions` | `04_` | both | Fluss — `kyc_tier`, `balance`, `lifetime_deposits` |
| `jobB1`, `jobB2`, `jobB3`, `jobC1`, `jobC2`, `jobD` | `05_` | both | Fluss — windowed feature columns (reading Fluss in complete mode, Kafka in simple) |
| `jobE-avg-stake-7d` | `08_` | complete | Fluss — `avg_stake_7d` |
| `risk-scoring-kafka` | `06_` | simple | Kafka `risk_scores` — bets ⋈ feature store |
| `risk-scoring-kafka-v2` | `08_` | complete | the same, plus `avg_stake_7d` and the `STAKE_SPIKE` rule |
| `risk-score-log-postgres` | `06_` | both | **PostgreSQL `public.risk_score_log`** |

The last two matter more than they look: `public.risk_score_log` is the only table the
Grafana dashboard queries (11 of its panels hit it), so without them the dashboard is blank
and nothing reaches PostgreSQL.

### Browser interfaces

Three services give the demo a browser front end, the same ones the `streaming-analytics`
lab uses:

- **`sql-gateway`** (port 8083) — Flink's SQL Gateway: the SQL Client's engine as a REST API.
  It appends the same `config.yaml` block as `sql-client` (checkpointing, restart strategy,
  `table.exec.source.idle-timeout`) because those are baked into a job by whoever submits it:
  without them a job deployed from the browser would run with checkpointing off and never
  restart. Sessions never expire (`idle-timeout=0`), so tables created in the browser survive
  a coffee break.
- **`sql-editor`** (port 8084) — a small Flask app in front of the gateway (which sends no CORS
  headers), serving two pages that share one gateway session, kept across page reloads:
  - **http://localhost:8084** — the SQL editor. The examples menu holds every file in
    `flink_sql/`: the DDL, each job on its own, each query in `07_`, and (complete mode) the
    *Add a feature* steps of `08_`. Tick *preload DDL*
    (or start with `PRELOAD_DDL=true`) to open sessions with `01_` already run, like
    `sql-client.sh --init`. The *Catalog* panel browses catalogs and the jobs on the cluster.
  - **http://localhost:8084/pipeline** — the Pipeline Builder: the architecture diagram as
    buttons. *Create* runs a table's `CREATE TABLE` (cut from `01_`/`02_`), *Deploy* runs one
    job's `INSERT` (cut from `03_`–`06_` at its `SET 'pipeline.name'`), *Start* runs the
    tiering service, *View data* queries the table, and the boxes turn green as jobs run and
    tables return rows. In complete mode a dashed area under the feature jobs holds the
    [Add a feature](#add-a-feature-to-the-online-store) walkthrough as three buttons; in simple
    mode the lake, the tiering service and that area are not drawn. A *Guide*
    mode walks through the core pipeline step by step. Every pop-up shows its SQL, editable
    before it runs.
- **`homepage`** (port 80) — a static page linking every UI with a reachability dot, the
  architecture, and the connection details.

Things worth knowing:

- Job boxes are matched to Flink jobs by `pipeline.name`, so jobs submitted by `start.sh`
  light up too. On page load the builder also creates, in the browser's session, the tables a
  running job uses — a full `./start.sh` creates them only in its own SQL Client sessions.
- The `fluss` and (complete mode) `iceberg` catalogs and the Kafka/CDC tables are session-scoped (Flink's
  in-memory default catalog); the Fluss tables themselves live in Fluss and outlive every
  session.
- *View data* on a CDC table reads through a replication slot of its own
  (`flink_view_*_slot`, dropped when the query stops), because PostgreSQL allows one reader
  per slot and Job A holds the pre-created ones.
- The gateway is one more JVM (~1 GB). Each streaming *View data* holds a task slot until
  its pop-up is closed.
- The tiering service's JAR is uploaded to the JobManager by `start.sh` in complete mode (listed
  under *Submit New Job* in the Flink UI); *Start* runs it with `scripts/tiering.args`.
  Uploaded JARs live in the JobManager's `/tmp`, so a JobManager restart forgets them — *Start*
  uploads it again when that happens.

### `--services-only`

```bash
./start.sh --services-only
```

Starts every container and installs the connector JARs, uploads the tiering service's JAR
without starting it, then stops — no DDL, no jobs. The Fluss database is left empty. Use it when you want to run the SQL by hand, demo the steps
live in the [Pipeline Builder](#browser-interfaces), or submit a subset. The exact commands for each step are printed at the end, and the
same list is printed on a normal run so you can re-run any single step.

Expect rows in `risk_score_log` to appear slowly at first. The lookup join is point-in-time
against Fluss, so a bet only scores once that player already has a feature vector; throughput
climbs as the feature store fills. Measured `enrichment_lag_ms` is ~50-60 ms end to end.

### Lakehouse tier and point-in-time queries

Complete mode only. `03_ingest_to_fluss.sql` creates the four event log tables in
`fluss.betting` with `'table.datalake.enabled' = 'true'`, then the ingest job that fills them. The **Fluss tiering service** — a Flink job shipped as a
JAR, one for every tiered table — copies their log into Iceberg tables of the same name in
the `lakehouse` warehouse of **Lakekeeper** (the Iceberg REST catalog), stored on **MinIO**,
committing one snapshot per Flink checkpoint. The setup follows
[streamhouse](https://github.com/campossalex/streamhouse): the same versions, the same
`datalake.*` server keys (in `docker-compose.yml`), the same `scripts/tiering.sh` and
`scripts/tiering.args`.

Three ways to read the result, all in `07_analytics_and_monitoring.sql`:

| Read | What you get |
|------|--------------|
| `fluss.betting.bets` | A **union read**: the Iceberg snapshot, then the Fluss log from exactly where it ends. Complete, never behind. |
| ``fluss.betting.`bets$lake` `` | The Iceberg side alone — about one checkpoint (60 s) behind. ``bets$lake$snapshots`` lists the commits. |
| `iceberg.betting.bets /*+ OPTIONS('as-of-timestamp' = '…') */` | **Time travel**, through Iceberg's own catalog (`01_`): the table exactly as it was at a past instant — reproducible, however much arrives since. |

The **point-in-time training set** query is the reason it is here: every bet, with the
features as they stood when it was placed — only earlier events counted, no leakage from the
future — and its settlement as the label. The feature store itself holds only the current
vector per player, so this history is what a model is trained on, and the same history is
what a new feature bootstraps from (next section).

Things to know:

- **Feature jobs read the Fluss tables from the latest offset** (`'scan.startup.mode' =
  'latest'`, through a temporary view per table in `05_`/`06_`). Without the hint a streaming
  read is a union read, and those jobs run on processing time: a replayed history would count
  as "now". A restore from a checkpoint still resumes from its saved offsets.
- **Commits happen on checkpoints.** The tiering job inherits the cluster's 60 s interval, so
  the lake is a minute behind whatever `table.datalake.freshness` (30 s) says.
- **Run one tiering job.** `scripts/tiering.sh` and the Pipeline Builder refuse to start a
  second; *Submit New Job* in the Flink UI does not check.
- **The feature store is not tiered.** It is current state. Fluss still copies the servers'
  `datalake.format` into it at `CREATE TABLE` time, which changes its key encoding — an earlier
  version of this repo removed the lake because pyfluss could not look such a table up. With
  the current pyfluss it can (verified: `/score` returns the row), but `requirements.txt` does
  not pin it, so if `/score` ever returns an empty feature dict, check this first.
- **Batch `OVER … RANGE INTERVAL` on a TIMESTAMP fails to compile** in Flink 1.20 ("Cannot cast
  TimestampData to java.lang.Long"). The training-set query orders by epoch seconds instead.

### Add a feature to the online store

The store has eleven features. `08_add_feature_avg_stake.sql` shows how a data engineer adds a
twelfth to it while it is live. Complete mode only — job E reads the lake. `./start.sh` runs it
after the core jobs; to walk through it yourself, start with `./start.sh --services-only` and
build the pipeline, or cancel job E and `risk-scoring-kafka-v2` and run the steps below. The feature is
**`avg_stake_7d`**, the player's average stake over 7 days, and its rule **`STAKE_SPIKE`**: a
bet more than 5× the player's own norm. A 7-day window is the case that matters: a streaming
job started now, or Kafka's retention, knows nothing about last week. The lake does.

1. **`ALTER TABLE fluss.betting.player_feature_store ADD avg_stake_7d DOUBLE`.** Fluss appends
   the column, nullable, while every writer keeps running — each job names only its own
   columns, so none of them notices. Readers see `NULL` until step 2 fills it; the scoring job
   and the inference service already treat `NULL` as unknown.
2. **Deploy job E** (`jobE-avg-stake-7d`). It reads `fluss.betting.bets` with **no startup
   hint** — a **union read**: Flink reads the Iceberg snapshot of the table first, then
   continues on the Fluss log from exactly the offset where that snapshot ends. No gap, no
   overlap, no switch-over to manage: the history bootstraps the feature and the same job
   carries on with the latest events. It must run on **event time** (`ORDER BY event_ts`): the
   bootstrap replays a week of bets in seconds, and on processing time they would all land in
   the same instant. It writes only `avg_stake_7d` — not the shared `last_updated_at`, which it
   would otherwise stamp with week-old times during the bootstrap. A second INSERT in the same
   job **expires** the feature: once a player's last bet leaves the 7-day window,
   `avg_stake_7d` goes back to `NULL` within about an hour (a HOP of 8 days sliding hourly).
   `NULL`, never 0: an average of 0 would make every next bet a `STAKE_SPIKE`. Both INSERTs are
   filtered on processing time — the average is written only for bets from the last 7 days,
   the expiry only for windows ending in the last 2 hours — so the replayed history can neither
   write a stale average nor overwrite a fresh one with an old NULL.
3. **Use it.** Stop `risk-scoring-kafka` and deploy `risk-scoring-kafka-v2`, the same job plus
   `avg_stake_7d` and the `STAKE_SPIKE` rule.

In the Pipeline Builder these are the three buttons in the dashed *New feature* area: **1 · ALTER**,
**2 · Deploy**, **3 · Use it**. From a terminal, cancel `risk-scoring-kafka` first, then:

```bash
docker compose exec sql-client /opt/flink/bin/sql-client.sh \
  --init /opt/flink_sql/01_sources_and_catalogs.sql \
  -f    /opt/flink_sql/08_add_feature_avg_stake.sql
```

To see the bootstrap, fire **Stake Spike** in the outlier generator *before* step 2: SPIKE_001's
four bets become history. Deploy job E, and SPIKE_001 has an `avg_stake_7d` straight away —
read back from Iceberg, for bets placed before the job existed. Fire it again after step 3: its
£400 bet scores as `STAKE_SPIKE`.

---

## Demo Walkthrough

### Trigger the fraud bot (BOT archetype)

```bash
# Via the outliner generator web UI
open http://localhost:9050

# Or via REST
curl -X POST http://localhost:9050/run/BOT
```

BOT_001 will place 20 bets in under 10 minutes. The feature store updates on every bet, within about a second.

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
| `02_fluss_feature_store.sql` | `CREATE TABLE` for the Fluss PK table (the online feature store) | Once (DDL is idempotent — uses `IF NOT EXISTS`) |
| `03_ingest_to_fluss.sql` | **Complete mode** — the four tiered Fluss log tables, and 1 streaming job: the Kafka topics → them | Once — runs until stopped |
| `04_cdc_dimensions.sql` | 1 streaming INSERT job (A) — PostgreSQL CDC → dimension columns | Once — runs until stopped |
| `05_feature_calculations.sql` | 6 streaming INSERT jobs (B1, B2, B3, C1, C2, D) — per-event features, from Fluss (complete) or Kafka (simple) | Once — runs until stopped |
| `06_risk_scoring_inference.sql` | Risk scoring → Kafka + PostgreSQL audit log | Once — runs until stopped |
| `07_analytics_and_monitoring.sql` | Ad-hoc queries; in complete mode the `iceberg` catalog and point-in-time queries | On-demand |
| `08_add_feature_avg_stake.sql` | **Complete mode** — ALTER TABLE + a union-read job adding `avg_stake_7d`, and scoring v2 | Once — `start.sh` runs it after `06_` |

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
| `./destroy.sh` | Containers, volumes (PostgreSQL, checkpoints, Fluss remote data, the Iceberg lake, Lakekeeper's catalog), network, locally built inference image |
| `./destroy.sh --data` | Also a `fluss-remote-data/` directory left on the host by older versions of this repo |
| `./destroy.sh --jars` | Also `lib/` — needs `./download-jars.sh` again (~300 MB) |
| `./destroy.sh --images` | Also the pulled images (~5 GB, shared with other OSS scenarios) |
| `./destroy.sh --all -y` | Everything, no prompt |

`docker compose down` without `-v` is **not** a pause. PostgreSQL, the Flink checkpoints, the
Iceberg lake and Lakekeeper's catalog live in volumes and survive it, but Redpanda, ZooKeeper
and the Fluss servers keep their data inside their containers: the Kafka topics, the Fluss
tables and every feature row are gone on the next `up`, while the lake still holds Iceberg
tables nothing tiers to any more. For a
consistent environment, restart with:

```bash
./start.sh --reset --services-only
```

That runs `docker compose down -v`, so everything starts from nothing.

---

## Timestamps and timezones

Every container pins `TZ=UTC`, and the Flink services (including `sql-client`) pin
`table.local-time-zone: UTC`. Keep it that way.

The generators emit `event_time` as a **naive** string from `datetime.now()`, so the container's zone is
what defines its meaning. Flink parses it with `TO_TIMESTAMP(...)` into a naive `TIMESTAMP(3)`, while
`CURRENT_TIMESTAMP` and `PROCTIME()` are `TIMESTAMP_LTZ`. `06_risk_scoring_inference.sql` mixes the two —
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

The per-event updates (the OVER windows in `05_feature_calculations.sql`) run on processing
time and do not wait for watermarks, so a slow *increase* points at the job itself (not
running, restarting, back-pressured). What still waits for the watermark is the **reset**
half of each job — the HOP window that writes the default once a player goes quiet — and
the risk scoring, Job C2's MATCH_RECOGNIZE and anything in `06`. Those are **event-time**
(`DESCRIPTOR(event_ts)` / `ORDER BY event_ts`), so they emit only once the watermark passes
— a starved watermark stalls them no matter how idle the CPU is.

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

After the fix, a reset lands within ~12 s of a player's window emptying: the HOP slide is
10 s, plus the 2 s watermark delay, plus the Fluss write. Also check the archetype's own pacing before calling it slow —
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

The coordinator also waits for `lakekeeper-warehouse` and `fluss-remote-init` to finish. If it
sits in *Created*, check those one-shots: `docker compose ps -a lakekeeper-warehouse fluss-remote-init`.

**Nothing reaches the lake (`bets$lake` stays empty)**

Three things must all hold: the tiering job is running, the table was created with
`'table.datalake.enabled' = 'true'` *after* the servers had `datalake.format` set, and new data
has arrived since the job started. Then look for commits and errors:

```bash
docker compose logs taskmanager | grep -E "Committed snapshot|Exception"
```

`WARN ... S3FileIO - Unclosed S3FileIO instance` lines are harmless noise from the Iceberg
reader. Tiering needs complete mode: simple mode has no log tables and its Fluss servers no
`datalake.format`, so `./start.sh --reset` to switch.

**Postgres CDC slot already exists**

If restarting after a crash, the replication slot may already exist. The `pg_cdc_setup.sql` script handles this with a `WHERE NOT EXISTS` guard. If the slot is stuck:
```bash
docker compose exec postgres psql -U root -d betting \
  -c "SELECT pg_drop_replication_slot('flink_cdc_betting_slot');"
```
Then restart the CDC jobs.

