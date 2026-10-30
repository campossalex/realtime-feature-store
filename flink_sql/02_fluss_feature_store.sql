-- ============================================================
-- Step 2 – Create the Fluss online feature store
--
-- Identical to betting_features/flink_sql/02_fluss_feature_store.sql.
-- Run after 01_sources_and_catalogs.sql in the same SQL Client session.
--
-- Window sizes are tuned for a 2-hour workshop. Each windowed feature is rewritten on
-- every event (a processing-time OVER window) and reset to its default by a HOP window
-- once the player goes quiet — see 04_feature_calculations.sql:
--   10 min  → bet velocity
--   15 min  → live ratio, avg odds, geo mismatches
--   30 min  → stake, sport variety, net P&L
-- ============================================================

CREATE TABLE IF NOT EXISTS fluss.betting.player_feature_store (
  player_id            STRING,

  -- Fraud / bot detection (per event, 10-min OVER window)
  bets_last_10min      BIGINT,

  -- Responsible gambling (per event, 30-min OVER window)
  total_stake_30min    DECIMAL(14,2),
  distinct_sports_30min BIGINT,

  -- Loss streak (event-driven, MATCH_RECOGNIZE)
  current_loss_streak  BIGINT,

  -- RG + fraud: live bet impulsivity (per event, 15-min OVER window)
  live_bet_ratio_15min DOUBLE,

  -- Sharp bettor detection (per event, 15-min OVER window)
  avg_odds_15min       DOUBLE,

  -- Fraud / multi-accounting (per event, 15-min OVER window)
  geo_mismatches_15min BIGINT,

  -- Operator P&L exposure (per event, 30-min OVER window)
  net_pnl_30min        DECIMAL(14,2),

  -- Dimensions from CDC
  kyc_tier             STRING,
  balance              DECIMAL(14,2),
  lifetime_deposits    DECIMAL(14,2),

  last_updated_at      TIMESTAMP(3),

  PRIMARY KEY (player_id) NOT ENFORCED
) WITH (
  'bucket.num' = '4'
);
