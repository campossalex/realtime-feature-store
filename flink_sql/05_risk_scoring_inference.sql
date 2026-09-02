-- ============================================================
-- Step 4 – Real-time inference (Pipeline 2)
--
-- Architecture:
--   bets (Kafka/Redpanda)
--       │
--       │  temporal lookup join  ← fluss.betting.player_feature_store
--       ▼
--   enriched subquery  (feature vector attached to each incoming bet)
--       │
--       │  rule-based risk scoring
--       ▼
--   risk_scores (Kafka)   +   risk_score_log (PostgreSQL)
--
-- The temporal lookup join reads the feature store at exactly the processing
-- time of each bet — the same point-in-time correctness that an online feature
-- store provides. No stale features.
--
-- Rule categories:
--   FRAUD_HIGH   — high velocity or geo anomaly (automated/multi-account fraud)
--   RG_ALERT     — responsible gambling trigger (loss streak, impulsivity, chasing)
--   SHARP_BETTOR — value bettor detection (consistently low odds)
--   STAKE_LIMIT  — 30-min stake threshold exceeded
--   NORMAL       — no rules triggered
-- ============================================================


-- ── Rule-based scoring → Kafka topic 'risk_scores' ───────────────────────────
SET 'pipeline.name' = 'risk-scoring-kafka';

INSERT INTO risk_scores
SELECT
  bet_id,
  player_id,
  CASE
    -- Fraud: high bet velocity (>8 bets in 10 min) or geo anomaly
    WHEN bets_last_10min > 8 OR geo_mismatches_15min > 1
      THEN 'FRAUD_HIGH'
    -- Responsible gambling: loss streak, impulsive live betting, or sport chasing
    WHEN current_loss_streak >= 6
      OR live_bet_ratio_15min > 0.5
      OR distinct_sports_30min >= 4
      THEN 'RG_ALERT'
    -- Sharp bettor: consistently picks low-odds value bets
    WHEN avg_odds_15min < 2 AND bets_last_10min > 5
      THEN 'SHARP_BETTOR'
    -- Stake limit: 30-min wagered amount exceeds operator threshold
    WHEN total_stake_30min > 500.00
      THEN 'STAKE_LIMIT'
    ELSE 'NORMAL'
  END AS risk_level,
  -- Comma-separated rule codes for explainability / audit
  REGEXP_REPLACE(
    CONCAT_WS(',',
      CASE WHEN bets_last_10min > 8         THEN 'BOT_VELOCITY'       ELSE NULL END,
      CASE WHEN geo_mismatches_15min > 1    THEN 'GEO_MISMATCH'       ELSE NULL END,
      CASE WHEN current_loss_streak >= 6    THEN 'LOSS_STREAK'        ELSE NULL END,
      CASE WHEN live_bet_ratio_15min > 0.5  THEN 'HIGH_LIVE_RATIO'    ELSE NULL END,
      CASE WHEN distinct_sports_30min >= 4  THEN 'CHASING_LOSSES'     ELSE NULL END,
      CASE WHEN avg_odds_15min < 2          THEN 'SHARP_ODDS_PATTERN' ELSE NULL END,
      CASE WHEN total_stake_30min > 500     THEN 'DAILY_LIMIT'        ELSE NULL END
    ),
  '^,+|,+$|,,+', '') AS risk_reasons,
  event_ts                                     AS event_ts,
  enrichment_lag_ms                            AS enrichment_lag_ms,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3))      AS score_time
FROM (
  -- Enrich each incoming bet with the player's current feature vector.
  -- Temporal lookup join: reads feature store state at bet processing time.
  SELECT
    b.bet_id,
    b.player_id,
    b.amount,
    b.bet_type,
    b.sport,
    b.odds,
    b.event_ts,
    -- Milliseconds from bet event_ts to Flink processing time
    (CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 AS BIGINT) + CAST(EXTRACT(MILLISECOND FROM CURRENT_TIMESTAMP) AS BIGINT)) -
    (CAST(EXTRACT(EPOCH FROM b.event_ts)         * 1000 AS BIGINT) + CAST(EXTRACT(MILLISECOND FROM b.event_ts)         AS BIGINT))
      AS enrichment_lag_ms,
    COALESCE(f.bets_last_10min,       0)       AS bets_last_10min,
    COALESCE(f.total_stake_30min,     0.00)    AS total_stake_30min,
    COALESCE(f.live_bet_ratio_15min,  0.0)     AS live_bet_ratio_15min,
    COALESCE(f.avg_odds_15min,        2.0)     AS avg_odds_15min,
    -- Loss streak is event-driven (MATCH_RECOGNIZE fires only on new settlements).
    -- Decay to 0 if the feature row hasn't been updated in 60 min — player stopped betting.
    COALESCE(
      CASE WHEN f.last_updated_at < b.proc_time - INTERVAL '60' MINUTE
           THEN CAST(0 AS BIGINT)
           ELSE f.current_loss_streak
      END, 0
    )                                          AS current_loss_streak,
    COALESCE(f.geo_mismatches_15min,  0)       AS geo_mismatches_15min,
    COALESCE(f.distinct_sports_30min, 1)       AS distinct_sports_30min,
    COALESCE(f.net_pnl_30min,         0.00)    AS net_pnl_30min,
    COALESCE(f.kyc_tier,              'BASIC') AS kyc_tier,
    COALESCE(f.balance,               0.00)    AS balance
  FROM bets AS b
  LEFT JOIN fluss.betting.player_feature_store FOR SYSTEM_TIME AS OF b.proc_time AS f
    ON b.player_id = f.player_id
) AS enriched;


-- ── Risk scores → PostgreSQL (deploy as a separate job) ──────────────────────
-- Reads from the risk_scores Kafka topic — no re-computation, no threshold drift.
-- Filters NORMAL out so only actionable alerts reach the audit table.
SET 'pipeline.name' = 'risk-score-log-postgres';

INSERT INTO risk_score_log
SELECT bet_id, player_id, risk_level, risk_reasons,
  CAST(event_ts AS TIMESTAMP),
  enrichment_lag_ms,
  CAST(score_time AS TIMESTAMP)
FROM risk_scores
WHERE risk_level <> 'NORMAL';
