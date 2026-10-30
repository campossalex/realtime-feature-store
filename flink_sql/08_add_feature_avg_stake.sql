-- ============================================================
-- Step 8 (optional) – Add a feature to the online feature store: avg_stake_7d
--
-- How a data engineer adds a twelfth feature to a feature store that is already live —
-- seven jobs writing it, the scoring job reading it — without stopping anything and
-- without the new feature starting from zero.
--
-- The feature: avg_stake_7d, the player's average stake over the last 7 days. Used by a
-- new rule, STAKE_SPIKE: a bet more than 5× the player's own norm. A 7-day window is
-- exactly the kind of feature a fresh streaming job cannot compute on its own — Kafka
-- retention or a job started "now" would know nothing about last week.
--
-- Three steps, each its own section below (and its own button in the Pipeline Builder):
--
--   1. ALTER TABLE ... ADD the column. Fluss appends it, nullable, to the primary-key
--      table while every writer keeps running: each job names only its own columns, so
--      none of them notices. Readers see NULL until step 2 fills it — the scoring job and
--      the inference service already treat NULL as "unknown".
--
--   2. Deploy job E. It reads fluss.betting.bets with NO startup hint, which makes it a
--      UNION READ: Flink first reads the Iceberg snapshot of the table (the history the
--      tiering service has committed), then continues on the Fluss log from exactly the
--      offset where that snapshot ends — no gap, no overlap, no switch-over to manage.
--      So the job bootstraps from history and then keeps up with the latest events,
--      as one job.
--
--      It must run on EVENT TIME (ORDER BY event_ts). The bootstrap replays a week of
--      bets in seconds; on processing time they would all land "now" in one window.
--      On event time each bet is placed in its own past, and the watermark carries the
--      job from the replayed history into the live stream.
--
--      A second INSERT in the same statement set expires the feature: once a player's
--      last bet has left the 7-day window, avg_stake_7d goes back to NULL (never 0 — an
--      average of 0 would make every next bet a STAKE_SPIKE).
--
--   3. Use it: replace the scoring job with a version that reads avg_stake_7d and has
--      the STAKE_SPIKE rule. Cancel risk-scoring-kafka first (Flink UI or the editor's
--      Jobs panel) — two scorers would write every bet to risk_scores twice.
--
-- Complete mode only — job E reads the Iceberg history, which simple mode does not have.
-- ./start.sh (complete) runs this file after the core jobs, cancelling risk-scoring-kafka
-- first. To walk through it by hand instead, start with --services-only and build the
-- pipeline yourself, or run it from here:
--
--   docker compose exec sql-client /opt/flink/bin/sql-client.sh \
--     --init /opt/flink_sql/01_sources_and_catalogs.sql \
--     -f    /opt/flink_sql/08_add_feature_avg_stake.sql
--   # (that runs all three steps; cancel risk-scoring-kafka before, or straight after)
-- ============================================================


-- ── Step 1: add the column ───────────────────────────────────────────────────
-- Appended at the end, nullable — the only kind of column Fluss adds to a live table.
-- Run it once: a second run fails with "column already exists".

ALTER TABLE fluss.betting.player_feature_store ADD avg_stake_7d DOUBLE;


-- ── Job E: Average stake — union read, event-time OVER 7 days + expiry ──────
-- Bootstraps from Iceberg, then follows the live Fluss log (see the header). One job,
-- two INSERTs, both reading fluss.betting.bets as a union read:
--
--   1. Per bet: the player's average stake over the 7 days up to that bet.
--   2. Expiry: avg_stake_7d = NULL for every player whose last bet is more than 7 days
--      old. NULL, not 0: 0 is an average stake of nothing, and the STAKE_SPIKE rule would
--      flag the player's next bet whatever its size; NULL is "no norm", which it skips.
--      A HOP of 8 days sliding every hour (~192 windows per bet) sees a player in some
--      window after they go quiet; the NULL lands within about an hour of expiry.
--
-- Both are filtered on PROCESSING time (now = CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3))),
-- because the bootstrap replays history through two independent sources:
--   - (1) writes only bets from the last 7 days of wall-clock time. Older bets still
--     enter the OVER window's state, so averages stay exact, but a player last seen more
--     than 7 days ago never gets a value written — rather than a stale one.
--   - (2) writes only windows that end within the last 2 hours. During the replay it
--     would otherwise emit NULLs for past quiet spells, possibly after (1) has already
--     written the player's current average — and the last write wins.
--
-- Writes avg_stake_7d only — NOT last_updated_at, which the other jobs share. During the
-- bootstrap this job emits rows stamped with week-old event times, and the scoring job
-- reads last_updated_at to decide that a loss streak is stale.

SET 'pipeline.name' = 'jobE-avg-stake-7d';

EXECUTE STATEMENT SET
BEGIN

INSERT INTO fluss.betting.player_feature_store (player_id, avg_stake_7d)
SELECT player_id, avg_stake_7d
FROM (
  SELECT
    player_id,
    event_ts,
    CAST(AVG(amount) OVER (
      PARTITION BY player_id ORDER BY event_ts
      RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND CURRENT ROW
    ) AS DOUBLE) AS avg_stake_7d
  FROM fluss.betting.bets
)
WHERE event_ts > CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) - INTERVAL '7' DAY;

INSERT INTO fluss.betting.player_feature_store (player_id, avg_stake_7d)
SELECT player_id, CAST(NULL AS DOUBLE)
FROM (
  SELECT player_id, window_end
  FROM TABLE(HOP(TABLE fluss.betting.bets, DESCRIPTOR(event_ts), INTERVAL '1' HOUR, INTERVAL '8' DAY))
  GROUP BY player_id, window_start, window_end
  HAVING MAX(event_ts) <= window_end - INTERVAL '7' DAY
)
WHERE window_end > CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) - INTERVAL '2' HOUR;

END;


-- ── Use it: risk scoring with STAKE_SPIKE ─────────────────────────────────────
-- The scoring job of 06_, plus avg_stake_7d and the STAKE_SPIKE rule (marked NEW).
-- Cancel risk-scoring-kafka before deploying this one. A new name, so the Flink UI shows
-- which version runs.

CREATE TEMPORARY VIEW IF NOT EXISTS bets_live AS
SELECT *, PROCTIME() AS proc_time
FROM fluss.betting.bets /*+ OPTIONS('scan.startup.mode' = 'latest') */;

SET 'pipeline.name' = 'risk-scoring-kafka-v2';

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
    -- NEW: a bet far above the player's own 7-day norm (the new feature, avg_stake_7d)
    WHEN avg_stake_7d IS NOT NULL AND amount > 5 * avg_stake_7d
      THEN 'STAKE_SPIKE'
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
      CASE WHEN total_stake_30min > 500     THEN 'DAILY_LIMIT'        ELSE NULL END,
      CASE WHEN avg_stake_7d IS NOT NULL
            AND amount > 5 * avg_stake_7d   THEN 'STAKE_SPIKE'        ELSE NULL END
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
    COALESCE(f.balance,               0.00)    AS balance,
    f.avg_stake_7d                             AS avg_stake_7d   -- NEW; NULL until known
  FROM bets_live AS b
  LEFT JOIN fluss.betting.player_feature_store FOR SYSTEM_TIME AS OF b.proc_time AS f
    ON b.player_id = f.player_id
) AS enriched;
