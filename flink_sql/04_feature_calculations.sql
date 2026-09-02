-- ============================================================
-- Step 4 – Feature calculations (Pipeline 1, part 2)
--
-- Six streaming jobs: B1, B2, B3, C1, C2, D. Run 03_cdc_dimensions.sql first.
--
-- All windows use HOP (sliding window) so features are refreshed continuously —
-- meaningful within a 2-hour workshop session.
--
-- HOP syntax: HOP(TABLE source, DESCRIPTOR(col), slide, size)
--   slide = how often a new result is emitted
--   size  = total lookback window
--
-- Each INSERT is a separate streaming job, and each names only the columns it owns —
-- Fluss updates only those columns, leaving all others in the row untouched. That is
-- what lets six jobs write one row concurrently without overwriting each other.
--
--   docker compose exec sql-client /opt/flink/bin/sql-client.sh \
--     --init /opt/flink_sql/01_sources_and_catalogs.sql \
--     -f    /opt/flink_sql/04_feature_calculations.sql
--
-- MEMORY NOTE: this submits six jobs at once. On a constrained host, run
-- 03_cdc_dimensions.sql on its own first, then either submit this file and cancel the
-- jobs you do not need from the Flink UI (http://localhost:8081), or copy a single
-- SET + INSERT block into a scratch file and submit that instead.
-- ============================================================


-- ── Job B1: Bet velocity — HOP 10 min / slide 10 sec ────────────────────────
-- Refreshes every 10 seconds. bets_last_10min > 8 = likely bot.
--
-- Zero-fill: a 20-min wide window (same slide) acts as the left side of a LEFT
-- JOIN. Any player active in the past 20 min gets a row every minute.  Players
-- absent from the 10-min feature window receive COALESCE → 0, resetting the
-- feature after their window expires instead of leaving a stale non-zero value.

SET 'pipeline.name' = 'jobB1-bet-velocity';

INSERT INTO fluss.betting.player_feature_store (player_id, bets_last_10min, last_updated_at)
SELECT
  wide.player_id,
  COALESCE(feat.bets_last_10min, CAST(0 AS BIGINT)) AS bets_last_10min,
  wide.window_end                                    AS last_updated_at
FROM (
  SELECT player_id, window_end
  FROM TABLE(HOP(TABLE bets, DESCRIPTOR(event_ts), INTERVAL '10' SECONDS, INTERVAL '20' MINUTE))
  GROUP BY player_id, window_start, window_end
) AS wide
LEFT JOIN (
  SELECT player_id, CAST(COUNT(*) AS BIGINT) AS bets_last_10min, window_end
  FROM TABLE(HOP(TABLE bets, DESCRIPTOR(event_ts), INTERVAL '10' SECONDS, INTERVAL '10' MINUTE))
  GROUP BY player_id, window_start, window_end
) AS feat
ON wide.player_id = feat.player_id AND wide.window_end = feat.window_end;


-- ── Job B2: Live ratio + avg odds — HOP 15 min / slide 10 sec ───────────────
-- Refreshes every 10 seconds.
-- live_bet_ratio_15min > 0.5 = impulsive live betting (RG risk).
-- avg_odds_15min < 2 combined with high velocity = sharp bettor.
-- Zero-fill: 30-min wide window ensures inactive players reset to 0 / 2.0.

SET 'pipeline.name' = 'jobB2-live-ratio-avg-odds';

INSERT INTO fluss.betting.player_feature_store (player_id, live_bet_ratio_15min, avg_odds_15min, last_updated_at)
SELECT
  wide.player_id,
  COALESCE(feat.live_bet_ratio_15min, CAST(0.0  AS DOUBLE)) AS live_bet_ratio_15min,
  COALESCE(feat.avg_odds_15min,       CAST(2.0  AS DOUBLE)) AS avg_odds_15min,
  wide.window_end                                            AS last_updated_at
FROM (
  SELECT player_id, window_end
  FROM TABLE(HOP(TABLE bets, DESCRIPTOR(event_ts), INTERVAL '10' SECONDS, INTERVAL '30' MINUTE))
  GROUP BY player_id, window_start, window_end
) AS wide
LEFT JOIN (
  SELECT
    player_id,
    CAST(SUM(CASE WHEN bet_type = 'live' THEN 1 ELSE 0 END) AS DOUBLE)
      / CAST(COUNT(*) AS DOUBLE) AS live_bet_ratio_15min,
    AVG(odds)                    AS avg_odds_15min,
    window_end
  FROM TABLE(HOP(TABLE bets, DESCRIPTOR(event_ts), INTERVAL '10' SECONDS, INTERVAL '15' MINUTE))
  GROUP BY player_id, window_start, window_end
) AS feat
ON wide.player_id = feat.player_id AND wide.window_end = feat.window_end;


-- ── Job B3: Total stake + sport variety — HOP 30 min / slide 10 sec ─────────
-- Refreshes every 10 seconds.
-- total_stake_30min > 500 triggers the stake limit rule.
-- distinct_sports_30min > 8 signals chasing losses (all 8 sports covered).
-- Zero-fill: 60-min wide window ensures inactive players reset to 0/1.

SET 'pipeline.name' = 'jobB3-stake-sport-variety';

INSERT INTO fluss.betting.player_feature_store (player_id, total_stake_30min, distinct_sports_30min, last_updated_at)
SELECT
  wide.player_id,
  COALESCE(feat.total_stake_30min,     CAST(0.00 AS DECIMAL(14,2))) AS total_stake_30min,
  COALESCE(feat.distinct_sports_30min, CAST(1    AS BIGINT))        AS distinct_sports_30min,
  wide.window_end                                                    AS last_updated_at
FROM (
  SELECT player_id, window_end
  FROM TABLE(HOP(TABLE bets, DESCRIPTOR(event_ts), INTERVAL '10' SECONDS, INTERVAL '60' MINUTE))
  GROUP BY player_id, window_start, window_end
) AS wide
LEFT JOIN (
  SELECT
    player_id,
    CAST(SUM(amount)           AS DECIMAL(14,2)) AS total_stake_30min,
    CAST(COUNT(DISTINCT sport) AS BIGINT)        AS distinct_sports_30min,
    window_end
  FROM TABLE(HOP(TABLE bets, DESCRIPTOR(event_ts), INTERVAL '10' SECONDS, INTERVAL '30' MINUTE))
  GROUP BY player_id, window_start, window_end
) AS feat
ON wide.player_id = feat.player_id AND wide.window_end = feat.window_end;


-- ── Job C1: Net P&L — HOP 30 min / slide 10 sec ─────────────────────────────
-- Refreshes every 10 seconds.
-- Negative net_pnl_30min = player is losing money.
-- Zero-fill: 60-min wide window (source: bet_settlements) resets to 0.

SET 'pipeline.name' = 'jobC1-net-pnl';

INSERT INTO fluss.betting.player_feature_store (player_id, net_pnl_30min, last_updated_at)
SELECT
  wide.player_id,
  COALESCE(feat.net_pnl_30min, CAST(0.00 AS DECIMAL(14,2))) AS net_pnl_30min,
  wide.window_end                                            AS last_updated_at
FROM (
  SELECT player_id, window_end
  FROM TABLE(HOP(TABLE bet_settlements, DESCRIPTOR(event_ts), INTERVAL '10' SECONDS, INTERVAL '60' MINUTE))
  GROUP BY player_id, window_start, window_end
) AS wide
LEFT JOIN (
  SELECT
    player_id,
    CAST(SUM(payout_amount) - SUM(stake) AS DECIMAL(14,2)) AS net_pnl_30min,
    window_end
  FROM TABLE(HOP(TABLE bet_settlements, DESCRIPTOR(event_ts), INTERVAL '10' SECONDS, INTERVAL '30' MINUTE))
  GROUP BY player_id, window_start, window_end
) AS feat
ON wide.player_id = feat.player_id AND wide.window_end = feat.window_end;


-- ── Job C2: Loss streak — MATCH_RECOGNIZE ────────────────────────────────────
-- Event-driven — fires immediately after each settled bet.
-- current_loss_streak >= 6 triggers the RG cooling-off alert.

SET 'pipeline.name' = 'jobC2-loss-streak';

INSERT INTO fluss.betting.player_feature_store (player_id, current_loss_streak, last_updated_at)
SELECT
  player_id,
  CAST(streak_length AS BIGINT) AS current_loss_streak,
  last_event_ts                 AS last_updated_at
FROM bet_settlements
MATCH_RECOGNIZE (
  PARTITION BY player_id
  ORDER BY     event_ts
  MEASURES
    COUNT(L.player_id) AS streak_length,
    LAST(L.event_ts)   AS last_event_ts
  ONE ROW PER MATCH
  -- SKIP PAST LAST ROW: each completed streak emits exactly once (no overlapping re-matches).
  AFTER MATCH SKIP PAST LAST ROW
  -- W is mandatory (not W?) so L+ is never the last element — avoids the greedy-quantifier error.
  -- Trade-off: the feature updates only when the streak ends (a win/void follows).
  PATTERN (L+ W)
  DEFINE
    L AS outcome = 'loss',
    W AS outcome <> 'loss'
) AS mr;


-- ── Job D: Geo mismatches — HOP 15 min / slide 10 sec ───────────────────────
-- Refreshes every 10 seconds.
-- geo_mismatches_15min > 1 = same account in multiple countries within 15 min.
-- Zero-fill: 30-min wide window (source: login_events) resets to 0.

SET 'pipeline.name' = 'jobD-geo-mismatches';

INSERT INTO fluss.betting.player_feature_store (player_id, geo_mismatches_15min, last_updated_at)
SELECT
  wide.player_id,
  COALESCE(feat.geo_mismatches_15min, CAST(0 AS BIGINT)) AS geo_mismatches_15min,
  wide.window_end                                        AS last_updated_at
FROM (
  SELECT player_id, window_end
  FROM TABLE(HOP(TABLE login_events, DESCRIPTOR(event_ts), INTERVAL '10' SECONDS, INTERVAL '30' MINUTE))
  GROUP BY player_id, window_start, window_end
) AS wide
LEFT JOIN (
  SELECT player_id, CAST(COUNT(DISTINCT country) AS BIGINT) AS geo_mismatches_15min, window_end
  FROM TABLE(HOP(TABLE login_events, DESCRIPTOR(event_ts), INTERVAL '10' SECONDS, INTERVAL '15' MINUTE))
  GROUP BY player_id, window_start, window_end
) AS feat
ON wide.player_id = feat.player_id AND wide.window_end = feat.window_end;
