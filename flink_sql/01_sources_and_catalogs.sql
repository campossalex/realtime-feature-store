-- ============================================================
-- Step 1 – Catalogs, CDC sources, Kafka sources & sinks
--
-- OSS Docker Compose variant of betting_features/flink_sql/01_sources_and_catalogs.sql
-- Addresses patched for internal Docker network hostnames.
--
-- Architecture:
--
--   PostgreSQL (players, player_wallets)
--       │  postgres-cdc connector
--       ▼
--   players_cdc, player_wallets_cdc  ─────────────────────┐
--                                                          │
--   Kafka topics (Redpanda)                                │  (feature engineering jobs in 03_)
--     bets ──────────────────────────────────────────────►│
--     bet_settlements ───────────────────────────────────►│
--     login_events ──────────────────────────────────────►│
--     deposits ──────────────────────────────────────────►│
--                                                          ▼
--                                          fluss.betting.player_feature_store
--                                                          │
--                                                          │  temporal lookup join (04_)
--                                                          ▼
--                                                     risk_scores (Kafka + PostgreSQL)
-- ============================================================


-- ── Session options ───────────────────────────────────────────────────────────

-- Without this, every event-time window job lags minutes behind wall clock.
--
-- Each Kafka topic has 4 partitions and the generators call producer.send() with no
-- key, so records round-robin across all 4. A Flink Kafka source emits the MINIMUM
-- watermark across the partitions it is assigned, so ONE quiet partition pins the
-- watermark for the whole job. login_events is the worst case — the baseline generator
-- emits a login only every 50 iterations (~1 per 10-20 s), which is ~1 per 40-90 s per
-- partition, so jobD-geo-mismatches was running 70-245 s behind and geo_mismatches_15min
-- took over two minutes to react to a GEO_001 trigger.
--
-- This tells Flink to mark a split idle after 5 s of no records and drop it from the
-- watermark minimum. Safe here because currentEmitEventTimeLag is ~2 ms — records always
-- carry near-current timestamps, so a partition waking up never produces late data.
--
-- MUST be set client-side. table.exec.* options are baked into the compiled plan by
-- whoever submits the job, so putting this in the jobmanager's FLINK_PROPERTIES does
-- nothing. This file is passed as --init on every submission, which is why it lives here.
--
-- Diagnose with:
--   curl -s '<jm>:8081/jobs/<id>/vertices/<source-id>/metrics?get=<n>.Source__login_events[70].watermarkLag'
SET 'table.exec.source.idle-timeout' = '5 s';


-- ── Catalogs ──────────────────────────────────────────────────────────────────

-- Fluss catalog — the online feature store (primary-key tables, sub-second lookups)
CREATE CATALOG fluss WITH (
  'type'              = 'fluss',
  'bootstrap.servers' = 'coordinator-server:9123'
);

CREATE DATABASE IF NOT EXISTS fluss.betting;

-- NOTE: no Iceberg catalog here. This scenario has no lakehouse tier — Fluss is both the
-- hot store and the historical one, so batch queries read Fluss directly (see
-- 06_analytics_and_monitoring.sql). See conf/fluss-server.yaml for why tiering is not
-- simply a config flag.

-- NOTE: no JDBC *catalog* here. flink-connector-jdbc (3.3.0 and 3.4.0) registers two
-- CatalogFactory classes that both claim the 'jdbc' identifier, so CREATE CATALOG fails
-- with "Multiple factories for identifier 'jdbc'". The jdbc *table* factory is
-- unaffected, so the PostgreSQL sink is declared explicitly as risk_score_log below.


-- ── PostgreSQL CDC sources ────────────────────────────────────────────────────
-- These tables stream changes from PostgreSQL into Flink as a CDC stream.
-- On first run: full snapshot of existing rows.
-- Ongoing: any INSERT / UPDATE / DELETE in PostgreSQL appears here within seconds.

CREATE TABLE players_cdc (
  player_id         STRING,
  username          STRING,
  kyc_tier          STRING,
  registration_date DATE,
  country           STRING,
  PRIMARY KEY (player_id) NOT ENFORCED
) WITH (
  'connector'                 = 'postgres-cdc',
  'hostname'                  = 'postgres',
  'port'                      = '5432',
  'username'                  = 'cdc_user',
  'password'                  = 'admin1',
  'database-name'             = 'betting',
  'schema-name'               = 'public',
  'table-name'                = 'players',
  -- Each postgres-cdc source needs its OWN slot; PostgreSQL allows only one active
  -- connection per replication slot. See pg_cdc_setup.sql.
  'slot.name'                 = 'flink_cdc_players_slot',
  'decoding.plugin.name'      = 'pgoutput',
  'debezium.publication.name' = 'betting_pub'
);

CREATE TABLE player_wallets_cdc (
  player_id            STRING,
  balance              DECIMAL(14,2),
  lifetime_deposits    DECIMAL(14,2),
  lifetime_withdrawals DECIMAL(14,2),
  PRIMARY KEY (player_id) NOT ENFORCED
) WITH (
  'connector'                 = 'postgres-cdc',
  'hostname'                  = 'postgres',
  'port'                      = '5432',
  'username'                  = 'cdc_user',
  'password'                  = 'admin1',
  'database-name'             = 'betting',
  'schema-name'               = 'public',
  'table-name'                = 'player_wallets',
  'slot.name'                 = 'flink_cdc_wallets_slot',
  'decoding.plugin.name'      = 'pgoutput',
  'debezium.publication.name' = 'betting_pub'
);


-- ── Kafka sources (Redpanda) ──────────────────────────────────────────────────
-- proc_time (PROCTIME()) is required by the Fluss temporal lookup join in step 04.

CREATE TABLE bets (
  bet_id      STRING,
  player_id   STRING,
  amount      DECIMAL(12,2),
  bet_type    STRING,      -- 'single' | 'accumulator' | 'live'
  sport       STRING,      -- 'football' | 'basketball' | 'tennis' | ...
  odds        DOUBLE,
  event_time  STRING,
  event_ts    AS TO_TIMESTAMP(event_time, 'yyyy-MM-dd HH:mm:ss.SSS'),
  proc_time   AS PROCTIME(),
  WATERMARK FOR event_ts AS event_ts - INTERVAL '2' SECOND
) WITH (
  'connector'                    = 'kafka',
  'format'                       = 'json',
  'json.ignore-parse-errors'     = 'true',
  'properties.bootstrap.servers' = 'redpanda:9092',
  'properties.group.id'          = 'flink-betting-features',
  'scan.startup.mode'            = 'latest-offset',
  'topic'                        = 'bets'
);

CREATE TABLE bet_settlements (
  settlement_id STRING,
  bet_id        STRING,
  player_id     STRING,
  outcome       STRING,        -- 'win' | 'loss' | 'void'
  stake         DECIMAL(12,2),
  payout_amount DECIMAL(12,2),
  event_time    STRING,
  event_ts      AS TO_TIMESTAMP(event_time, 'yyyy-MM-dd HH:mm:ss.SSS'),
  proc_time     AS PROCTIME(),
  WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND
) WITH (
  'connector'                    = 'kafka',
  'format'                       = 'json',
  'json.ignore-parse-errors'     = 'true',
  'properties.bootstrap.servers' = 'redpanda:9092',
  'properties.group.id'          = 'flink-betting-features',
  'scan.startup.mode'            = 'earliest-offset',
  'topic'                        = 'bet_settlements'
);

CREATE TABLE login_events (
  event_id    STRING,
  player_id   STRING,
  session_id  STRING,
  device_type STRING,
  ip_address  STRING,
  country     STRING,
  event_time  STRING,
  event_ts    AS TO_TIMESTAMP(event_time, 'yyyy-MM-dd HH:mm:ss.SSS'),
  proc_time   AS PROCTIME(),
  WATERMARK FOR event_ts AS event_ts - INTERVAL '2' SECOND
) WITH (
  'connector'                    = 'kafka',
  'format'                       = 'json',
  'json.ignore-parse-errors'     = 'true',
  'properties.bootstrap.servers' = 'redpanda:9092',
  'properties.group.id'          = 'flink-betting-features',
  'scan.startup.mode'            = 'earliest-offset',
  'topic'                        = 'login_events'
);

CREATE TABLE deposits (
  deposit_id  STRING,
  player_id   STRING,
  amount      DECIMAL(12,2),
  `method`    STRING,       -- 'card' | 'bank_transfer' | 'crypto' | 'ewallet'
  event_time  STRING,
  event_ts    AS TO_TIMESTAMP(event_time, 'yyyy-MM-dd HH:mm:ss.SSS'),
  proc_time   AS PROCTIME(),
  WATERMARK FOR event_ts AS event_ts - INTERVAL '2' SECOND
) WITH (
  'connector'                    = 'kafka',
  'format'                       = 'json',
  'json.ignore-parse-errors'     = 'true',
  'properties.bootstrap.servers' = 'redpanda:9092',
  'properties.group.id'          = 'flink-betting-features',
  'scan.startup.mode'            = 'earliest-offset',
  'topic'                        = 'deposits'
);


-- ── Kafka sink (risk scores output) ──────────────────────────────────────────

CREATE TABLE risk_scores (
  bet_id             STRING,
  player_id          STRING,
  risk_level         STRING,
  risk_reasons       STRING,
  event_ts           TIMESTAMP(3),
  enrichment_lag_ms  BIGINT,
  score_time         TIMESTAMP(3)
) WITH (
  'connector'                    = 'kafka',
  'format'                       = 'json',
  'properties.bootstrap.servers' = 'redpanda:9092',
  'topic'                        = 'risk_scores',
  'properties.group.id'          = 'flink-risk-scores-features',
  'scan.startup.mode'            = 'earliest-offset'
);


-- ── PostgreSQL sink (risk score audit trail) ─────────────────────────────────
-- Maps to public.risk_score_log, created by pg_betting_sink_ddl.sql at container init.
-- The PRIMARY KEY makes this an upsert sink, so replays overwrite rather than duplicate.

CREATE TABLE risk_score_log (
  bet_id             STRING,
  player_id          STRING,
  risk_level         STRING,
  risk_reasons       STRING,
  event_ts           TIMESTAMP(3),
  enrichment_lag_ms  BIGINT,
  score_time         TIMESTAMP(3),
  PRIMARY KEY (bet_id) NOT ENFORCED
) WITH (
  'connector'  = 'jdbc',
  'url'        = 'jdbc:postgresql://postgres:5432/betting',
  'table-name' = 'public.risk_score_log',
  'username'   = 'root',
  'password'   = 'admin1'
);
