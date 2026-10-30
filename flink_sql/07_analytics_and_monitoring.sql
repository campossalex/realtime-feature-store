-- ============================================================
-- Step 7 – Analytics & monitoring queries
--
-- Run these ad-hoc in the SQL Client to verify the pipeline is
-- working end-to-end. These are SELECT-only — no jobs to deploy.
--
-- Open an interactive SQL Client session with catalogs pre-loaded:
--   docker compose exec sql-client /opt/flink/bin/sql-client.sh \
--     --init /opt/flink_sql/01_sources_and_catalogs.sql
--   (--init processes DDL into the session; EXECUTE FILE only works for DML)
-- ============================================================


-- ── Feature store health ──────────────────────────────────────────────────────

-- How many players have feature vectors?
SELECT COUNT(*) AS players_in_feature_store
FROM fluss.betting.player_feature_store;

-- Spot-check a specific player (replace player_id)
SELECT *
FROM fluss.betting.player_feature_store
WHERE player_id = 'BOT_001';

-- Players currently flagged by at least one rule
SELECT
  player_id,
  bets_last_10min,
  live_bet_ratio_15min,
  current_loss_streak,
  geo_mismatches_15min,
  total_stake_30min,
  avg_odds_15min,
  distinct_sports_30min,
  kyc_tier,
  last_updated_at
FROM fluss.betting.player_feature_store
WHERE bets_last_10min > 8
   OR geo_mismatches_15min > 1
   OR current_loss_streak >= 6
   OR live_bet_ratio_15min > 0.5
   OR distinct_sports_30min >= 4
   OR (avg_odds_15min < 2 AND bets_last_10min > 5)
   OR total_stake_30min > 500
ORDER BY last_updated_at DESC
LIMIT 20;


-- ── Risk score stream analytics ───────────────────────────────────────────────

-- Risk distribution over last 10 minutes
SELECT
  risk_level,
  COUNT(*) AS cnt,
  window_start,
  window_end
FROM TABLE(TUMBLE(TABLE risk_scores, DESCRIPTOR(score_time), INTERVAL '10' MINUTE))
GROUP BY risk_level, window_start, window_end;

-- Average processing latency: time from bet event_ts to risk score emission
-- (measures end-to-end feature + inference latency)
SELECT
  AVG(
    TIMESTAMPDIFF(MILLISECOND, event_ts, score_time)
  ) AS avg_latency_ms,
  MAX(
    TIMESTAMPDIFF(MILLISECOND, event_ts, score_time)
  ) AS p99_latency_ms
FROM (
  SELECT b.event_ts, r.score_time
  FROM bets_kafka b
  JOIN risk_scores r ON b.bet_id = r.bet_id
);

-- Throughput: risk scores per second
SELECT
  COUNT(*) AS scores_in_window,
  window_start,
  window_end
FROM TABLE(TUMBLE(TABLE risk_scores, DESCRIPTOR(score_time), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end
ORDER BY window_start DESC;


-- ── Iceberg catalog ──────────────────────────────────────────────────────────
-- The same Lakekeeper warehouse the tiering service writes to, read directly. Not needed
-- for the union read (fluss.betting.bets already includes the tiered history); it is for
-- Iceberg's own features, such as the time travel at the end of this file. Complete mode
-- only. Session-scoped like every catalog: run it once in the session that runs them.
-- The S3 keys repeat the ones in docker-compose.yml.

CREATE CATALOG iceberg WITH (
  'type'                 = 'iceberg',
  'catalog-type'         = 'rest',
  'uri'                  = 'http://lakekeeper:8181/catalog',
  'warehouse'            = 'lakehouse',
  'io-impl'              = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint'          = 'http://minio:9000',
  's3.access-key-id'     = 'admin',
  's3.secret-access-key' = 'password',
  's3.path-style-access' = 'true',
  'client.region'        = 'us-east-1'
);


-- ── Point-in-time history (the Iceberg tier) ─────────────────────────────────
-- Complete mode only: simple mode has no lake. The four event log tables are tiered to
-- Iceberg (03_ingest_to_fluss.sql, scripts/tiering.sh). Reading
-- fluss.betting.bets plainly is a UNION READ — the Iceberg history plus the Fluss tail
-- not yet tiered — so these queries see every event since the stack started, not just
-- what Fluss still holds. `bets$lake` is the Iceberg side alone.
--
-- They run in batch mode: bounded reads that end. Switch back afterwards with
--   SET 'execution.runtime-mode' = 'streaming';

-- Lake vs union read: the lake trails by about one checkpoint (60 s), the union read does not.
SET 'execution.runtime-mode' = 'batch';
SELECT
  (SELECT COUNT(*) FROM fluss.betting.`bets$lake`) AS bets_in_iceberg,
  (SELECT COUNT(*) FROM fluss.betting.bets)        AS bets_union_read,
  (SELECT MAX(event_ts) FROM fluss.betting.`bets$lake`) AS iceberg_up_to,
  (SELECT MAX(event_ts) FROM fluss.betting.bets)        AS union_up_to;

-- Point-in-time training set: every bet, with the features AS THEY STOOD when it was
-- placed (only earlier events counted — no leakage from the future), and its outcome
-- as the label. This is what a model trained on these features should see.
--   ORDER BY a BIGINT of epoch seconds, not event_ts: in batch mode Flink 1.20 fails to
--   compile a RANGE ... INTERVAL window over a TIMESTAMP ("Cannot cast TimestampData to
--   java.lang.Long"). Second precision is plenty here.
SET 'execution.runtime-mode' = 'batch';
SELECT
  b.bet_id,
  b.player_id,
  b.event_ts,
  b.amount,
  COUNT(*)      OVER w10 - 1        AS bets_prev_10min,
  SUM(b.amount) OVER w30 - b.amount AS stake_prev_30min,
  s.outcome                         AS label
FROM (
  SELECT *, UNIX_TIMESTAMP(DATE_FORMAT(event_ts, 'yyyy-MM-dd HH:mm:ss')) AS ts_s
  FROM fluss.betting.bets
) AS b
LEFT JOIN fluss.betting.bet_settlements AS s ON s.bet_id = b.bet_id
WINDOW
  w10 AS (PARTITION BY b.player_id ORDER BY b.ts_s RANGE BETWEEN 600  PRECEDING AND CURRENT ROW),
  w30 AS (PARTITION BY b.player_id ORDER BY b.ts_s RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW)
LIMIT 100;

-- Iceberg snapshots: one per tiering commit. Pick a committed_at to travel back to.
SET 'execution.runtime-mode' = 'batch';
SELECT snapshot_id, committed_at, operation
FROM fluss.betting.`bets$lake$snapshots`
ORDER BY committed_at DESC;

-- Time travel: the table exactly as it was at a past instant, through Iceberg's own
-- catalog — create it first in this session (the Iceberg catalog section above).
-- Reproducible — rerunning gives the same rows, however many arrive since.
-- as-of-timestamp is epoch milliseconds and must be a literal: replace it with a moment
-- after the first snapshot above (or use 'snapshot-id' = '<id from above>' instead).
-- Get the value with: SELECT UNIX_TIMESTAMP() * 1000 - 600000;   -- 10 minutes ago
SET 'execution.runtime-mode' = 'batch';
SELECT COUNT(*) AS bets, MAX(event_ts) AS up_to
FROM iceberg.betting.bets /*+ OPTIONS('as-of-timestamp' = '1793300000000') */;
