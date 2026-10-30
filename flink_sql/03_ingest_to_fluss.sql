-- ============================================================
-- Step 3 – Ingest: Kafka topics → Fluss log tables (complete mode only)
--
-- Creates the four Fluss log tables, tiered to Iceberg, and one streaming job that
-- copies the four event topics into them. From here on every feature job reads Fluss,
-- not Kafka, and the tiering service copies the same tables on to Iceberg — so the
-- history every feature can be rebuilt from is the history the features were computed
-- from. Start the tiering service first (scripts/tiering.sh): it then picks up each
-- table as it is created.
--
-- Simple mode skips this file: no lake, no log tables, and the feature jobs read the
-- Kafka topics themselves (see 05_feature_calculations.sql).
--
-- A STATEMENT SET runs the four INSERTs as one Flink job (one slot): four sources, four
-- Fluss writers, nothing shared between them.
--
--   docker compose exec sql-client /opt/flink/bin/sql-client.sh \
--     --init /opt/flink_sql/01_sources_and_catalogs.sql \
--     -f    /opt/flink_sql/03_ingest_to_fluss.sql
-- ============================================================


-- ── Event log tables (tiered to Iceberg) ─────────────────────────────────────
-- Same columns as the Kafka topics, with event_time parsed once into event_ts by the
-- ingest job, and the same watermarks as the Kafka tables in 01.
--
-- 'table.datalake.enabled' = 'true' opts a table into tiering: the tiering service
-- (scripts/tiering.sh) copies its log into an Iceberg table of the same name in the
-- `lakehouse` warehouse, committing every Flink checkpoint (60 s). The servers'
-- datalake.format (docker-compose.yml, complete mode) is copied into the table here at
-- CREATE time. freshness is the target lag of the Iceberg copy; the checkpoint interval
-- is the floor.
--
-- bucket.key = player_id keeps one player's events in one bucket, in order.

CREATE TABLE IF NOT EXISTS fluss.betting.bets (
  bet_id      STRING,
  player_id   STRING,
  amount      DECIMAL(12,2),
  bet_type    STRING,      -- 'single' | 'accumulator' | 'live'
  sport       STRING,      -- 'football' | 'basketball' | 'tennis' | ...
  odds        DOUBLE,
  event_ts    TIMESTAMP(3),
  WATERMARK FOR event_ts AS event_ts - INTERVAL '2' SECOND
) WITH (
  'bucket.num'               = '4',
  'bucket.key'               = 'player_id',
  'table.datalake.enabled'   = 'true',
  'table.datalake.freshness' = '30s'
);

CREATE TABLE IF NOT EXISTS fluss.betting.bet_settlements (
  settlement_id STRING,
  bet_id        STRING,
  player_id     STRING,
  outcome       STRING,        -- 'win' | 'loss' | 'void'
  stake         DECIMAL(12,2),
  payout_amount DECIMAL(12,2),
  event_ts      TIMESTAMP(3),
  WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND
) WITH (
  'bucket.num'               = '4',
  'bucket.key'               = 'player_id',
  'table.datalake.enabled'   = 'true',
  'table.datalake.freshness' = '30s'
);

CREATE TABLE IF NOT EXISTS fluss.betting.login_events (
  event_id    STRING,
  player_id   STRING,
  session_id  STRING,
  device_type STRING,
  ip_address  STRING,
  country     STRING,
  event_ts    TIMESTAMP(3),
  WATERMARK FOR event_ts AS event_ts - INTERVAL '2' SECOND
) WITH (
  'bucket.num'               = '4',
  'bucket.key'               = 'player_id',
  'table.datalake.enabled'   = 'true',
  'table.datalake.freshness' = '30s'
);

CREATE TABLE IF NOT EXISTS fluss.betting.deposits (
  deposit_id  STRING,
  player_id   STRING,
  amount      DECIMAL(12,2),
  `method`    STRING,       -- 'card' | 'bank_transfer' | 'crypto' | 'ewallet'
  event_ts    TIMESTAMP(3),
  WATERMARK FOR event_ts AS event_ts - INTERVAL '2' SECOND
) WITH (
  'bucket.num'               = '4',
  'bucket.key'               = 'player_id',
  'table.datalake.enabled'   = 'true',
  'table.datalake.freshness' = '30s'
);



-- ── Ingest: Kafka → Fluss ─────────────────────────────────────────────────────
-- event_time (a string on the wire) is parsed once, by the Kafka tables in 01_, and
-- stored as event_ts. Run this before the feature jobs: they read the Fluss tables
-- from the latest offset, so whatever is ingested before they start is history only
-- (still in Fluss and Iceberg, for queries and for bootstrapping a new feature).

SET 'pipeline.name' = 'ingest-kafka-to-fluss';

EXECUTE STATEMENT SET
BEGIN

INSERT INTO fluss.betting.bets
SELECT bet_id, player_id, amount, bet_type, sport, odds, event_ts
FROM bets_kafka;

INSERT INTO fluss.betting.bet_settlements
SELECT settlement_id, bet_id, player_id, outcome, stake, payout_amount, event_ts
FROM bet_settlements_kafka;

INSERT INTO fluss.betting.login_events
SELECT event_id, player_id, session_id, device_type, ip_address, country, event_ts
FROM login_events_kafka;

INSERT INTO fluss.betting.deposits
SELECT deposit_id, player_id, amount, `method`, event_ts
FROM deposits_kafka;

END;
