-- ============================================================
-- Step 4 – Feature calculations (Pipeline 1, part 2)
--
-- Six streaming jobs: B1, B2, B3, C1, C2, D. Run 03_cdc_dimensions.sql first.
--
-- Features update PER EVENT: every bet, settlement or login rewrites its player's
-- features the moment it arrives, so a player queried right after acting already
-- shows that action. Each windowed job (B1, B2, B3, C1, D) is one STATEMENT SET with
-- two INSERTs into the same columns, so it is still one Flink job:
--
--   1. Per-event: an OVER window on processing time —
--        AGG(...) OVER (PARTITION BY player_id ORDER BY proc_time
--                       RANGE BETWEEN INTERVAL 'N' MINUTE PRECEDING AND CURRENT ROW)
--      emits one row per incoming event, aggregating the player's last N minutes.
--      Processing time means no watermark to wait for (event time would hold each
--      row back by the 2 s watermark delay, more on a quiet partition).
--   2. Reset: an OVER window only fires when the player acts, so it never brings a
--      feature back down by itself. A HOP window (slide 10 s, size 2×N) writes the
--      default value (0 …) for every player whose latest event is older than N
--      minutes — players who went quiet reset within ~10 s of their window emptying.
--
-- Trade-offs of this design:
--   - Partial decay (9 bets → 3 as old bets age out) is applied at the player's next
--     event, not on a timer; only the full reset to the default runs on a timer.
--   - A player who acts again in the ~2 s between a reset window closing and its
--     row landing can have that reset overwrite the fresh value; their next event
--     corrects it.
--   - Windows follow arrival time, so replaying old topic data does not reproduce
--     event-time windows.
--   - The scoring job (05) looks the row up when a bet arrives, racing this job's
--     write for the same bet: the score may not yet count the bet it scores.
--
-- Each INSERT names only the columns it owns — Fluss updates only those columns,
-- leaving all others in the row untouched. That is what lets six jobs write one row
-- concurrently without overwriting each other.
--
--   docker compose exec sql-client /opt/flink/bin/sql-client.sh \
--     --init /opt/flink_sql/01_sources_and_catalogs.sql \
--     -f    /opt/flink_sql/04_feature_calculations.sql
--
-- MEMORY NOTE: this submits six jobs at once. On a constrained host, run
-- 03_cdc_dimensions.sql on its own first, then either submit this file and cancel the
-- jobs you do not need from the Flink UI (http://localhost:8081), or copy a single
-- SET + STATEMENT SET block into a scratch file and submit that instead.
-- ============================================================


-- ── Job B1: Bet velocity — per-event OVER 10 min + reset ────────────────────
-- bets_last_10min > 8 = likely bot. Updates on every bet.
-- Reset: players with no bet in the last 10 min (but one in the last 20) → 0.

SET 'pipeline.name' = 'jobB1-bet-velocity';

EXECUTE STATEMENT SET
BEGIN

INSERT INTO fluss.betting.player_feature_store (player_id, bets_last_10min, last_updated_at)
SELECT
  player_id,
  CAST(COUNT(*) OVER w AS BIGINT) AS bets_last_10min,
  event_ts                        AS last_updated_at
FROM bets
WINDOW w AS (
  PARTITION BY player_id ORDER BY proc_time
  RANGE BETWEEN INTERVAL '10' MINUTE PRECEDING AND CURRENT ROW
);

INSERT INTO fluss.betting.player_feature_store (player_id, bets_last_10min, last_updated_at)
SELECT player_id, CAST(0 AS BIGINT), window_end
FROM TABLE(HOP(TABLE bets, DESCRIPTOR(event_ts), INTERVAL '10' SECONDS, INTERVAL '20' MINUTE))
GROUP BY player_id, window_start, window_end
HAVING MAX(event_ts) <= window_end - INTERVAL '10' MINUTE;

END;


-- ── Job B2: Live ratio + avg odds — per-event OVER 15 min + reset ───────────
-- live_bet_ratio_15min > 0.5 = impulsive live betting (RG risk).
-- avg_odds_15min < 2 combined with high velocity = sharp bettor.
-- Reset: players with no bet in the last 15 min (but one in the last 30) → 0 / 2.0.

SET 'pipeline.name' = 'jobB2-live-ratio-avg-odds';

EXECUTE STATEMENT SET
BEGIN

INSERT INTO fluss.betting.player_feature_store (player_id, live_bet_ratio_15min, avg_odds_15min, last_updated_at)
SELECT
  player_id,
  CAST(SUM(CASE WHEN bet_type = 'live' THEN 1 ELSE 0 END) OVER w AS DOUBLE)
    / CAST(COUNT(*) OVER w AS DOUBLE)  AS live_bet_ratio_15min,
  AVG(odds) OVER w                     AS avg_odds_15min,
  event_ts                             AS last_updated_at
FROM bets
WINDOW w AS (
  PARTITION BY player_id ORDER BY proc_time
  RANGE BETWEEN INTERVAL '15' MINUTE PRECEDING AND CURRENT ROW
);

INSERT INTO fluss.betting.player_feature_store (player_id, live_bet_ratio_15min, avg_odds_15min, last_updated_at)
SELECT player_id, CAST(0.0 AS DOUBLE), CAST(2.0 AS DOUBLE), window_end
FROM TABLE(HOP(TABLE bets, DESCRIPTOR(event_ts), INTERVAL '10' SECONDS, INTERVAL '30' MINUTE))
GROUP BY player_id, window_start, window_end
HAVING MAX(event_ts) <= window_end - INTERVAL '15' MINUTE;

END;


-- ── Job B3: Total stake + sport variety — per-event OVER 30 min + reset ─────
-- total_stake_30min > 500 triggers the stake limit rule.
-- distinct_sports_30min > 8 signals chasing losses (all 8 sports covered).
-- Reset: players with no bet in the last 30 min (but one in the last 60) → 0 / 1.

SET 'pipeline.name' = 'jobB3-stake-sport-variety';

EXECUTE STATEMENT SET
BEGIN

INSERT INTO fluss.betting.player_feature_store (player_id, total_stake_30min, distinct_sports_30min, last_updated_at)
SELECT
  player_id,
  CAST(SUM(amount) OVER w           AS DECIMAL(14,2)) AS total_stake_30min,
  CAST(COUNT(DISTINCT sport) OVER w AS BIGINT)        AS distinct_sports_30min,
  event_ts                                            AS last_updated_at
FROM bets
WINDOW w AS (
  PARTITION BY player_id ORDER BY proc_time
  RANGE BETWEEN INTERVAL '30' MINUTE PRECEDING AND CURRENT ROW
);

INSERT INTO fluss.betting.player_feature_store (player_id, total_stake_30min, distinct_sports_30min, last_updated_at)
SELECT player_id, CAST(0.00 AS DECIMAL(14,2)), CAST(1 AS BIGINT), window_end
FROM TABLE(HOP(TABLE bets, DESCRIPTOR(event_ts), INTERVAL '10' SECONDS, INTERVAL '60' MINUTE))
GROUP BY player_id, window_start, window_end
HAVING MAX(event_ts) <= window_end - INTERVAL '30' MINUTE;

END;


-- ── Job C1: Net P&L — per-event OVER 30 min + reset ─────────────────────────
-- Negative net_pnl_30min = player is losing money. Updates on every settlement.
-- Reset: players with no settlement in the last 30 min (but one in the last 60) → 0.

SET 'pipeline.name' = 'jobC1-net-pnl';

EXECUTE STATEMENT SET
BEGIN

INSERT INTO fluss.betting.player_feature_store (player_id, net_pnl_30min, last_updated_at)
SELECT
  player_id,
  CAST(SUM(payout_amount) OVER w - SUM(stake) OVER w AS DECIMAL(14,2)) AS net_pnl_30min,
  event_ts                                                             AS last_updated_at
FROM bet_settlements
WINDOW w AS (
  PARTITION BY player_id ORDER BY proc_time
  RANGE BETWEEN INTERVAL '30' MINUTE PRECEDING AND CURRENT ROW
);

INSERT INTO fluss.betting.player_feature_store (player_id, net_pnl_30min, last_updated_at)
SELECT player_id, CAST(0.00 AS DECIMAL(14,2)), window_end
FROM TABLE(HOP(TABLE bet_settlements, DESCRIPTOR(event_ts), INTERVAL '10' SECONDS, INTERVAL '60' MINUTE))
GROUP BY player_id, window_start, window_end
HAVING MAX(event_ts) <= window_end - INTERVAL '30' MINUTE;

END;


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


-- ── Job D: Geo mismatches — per-event OVER 15 min + reset ───────────────────
-- geo_mismatches_15min > 1 = same account in multiple countries within 15 min.
-- Updates on every login.
-- Reset: players with no login in the last 15 min (but one in the last 30) → 0.

SET 'pipeline.name' = 'jobD-geo-mismatches';

EXECUTE STATEMENT SET
BEGIN

INSERT INTO fluss.betting.player_feature_store (player_id, geo_mismatches_15min, last_updated_at)
SELECT
  player_id,
  CAST(COUNT(DISTINCT country) OVER w AS BIGINT) AS geo_mismatches_15min,
  event_ts                                       AS last_updated_at
FROM login_events
WINDOW w AS (
  PARTITION BY player_id ORDER BY proc_time
  RANGE BETWEEN INTERVAL '15' MINUTE PRECEDING AND CURRENT ROW
);

INSERT INTO fluss.betting.player_feature_store (player_id, geo_mismatches_15min, last_updated_at)
SELECT player_id, CAST(0 AS BIGINT), window_end
FROM TABLE(HOP(TABLE login_events, DESCRIPTOR(event_ts), INTERVAL '10' SECONDS, INTERVAL '30' MINUTE))
GROUP BY player_id, window_start, window_end
HAVING MAX(event_ts) <= window_end - INTERVAL '15' MINUTE;

END;
