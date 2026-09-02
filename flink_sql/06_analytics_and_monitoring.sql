-- ============================================================
-- Step 5 – Analytics & monitoring queries
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
  FROM bets b
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


-- ── Training data ─────────────────────────────────────────────────────────────
-- This scenario has no lakehouse tier, so there is no `FOR SYSTEM_TIME AS OF` time
-- travel and no point-in-time snapshot for training. A batch read of the Fluss table
-- gives the CURRENT feature vector per player, which is the whole history available:
SELECT *
FROM fluss.betting.player_feature_store
LIMIT 100;

-- If you need labelled training rows rather than current state, read the raw event
-- topics instead — `bets` and `bet_settlements` retain their full history in Redpanda:
--
--   SELECT player_id, event_ts, stake, odds FROM bets;
--
-- Reconstructing as-of-decision-time features from those events is exactly the job the
-- Iceberg tier used to do. See conf/fluss-server.yaml before re-adding it.
