-- ============================================================
-- Step 2 – Create the Fluss online feature store
--
-- Identical to betting_features/flink_sql/02_fluss_feature_store.sql.
-- Run after 01_sources_and_catalogs.sql in the same SQL Client session.
--
-- Window sizes are tuned for a 2-hour workshop:
--   10-min HOP / 1-min slide  → bet velocity
--   15-min HOP / 2-min slide  → live ratio, avg odds, geo mismatches
--   30-min HOP / 2-min slide  → stake, sport variety, net P&L
-- ============================================================

CREATE TABLE IF NOT EXISTS fluss.betting.player_feature_store (
  player_id            STRING,

  -- Fraud / bot detection (10-min HOP, 10-sec slide)
  bets_last_10min      BIGINT,

  -- Responsible gambling (30-min HOP, 10-sec slide)
  total_stake_30min    DECIMAL(14,2),
  distinct_sports_30min BIGINT,

  -- Loss streak (event-driven, MATCH_RECOGNIZE)
  current_loss_streak  BIGINT,

  -- RG + fraud: live bet impulsivity (15-min HOP, 10-sec slide)
  live_bet_ratio_15min DOUBLE,

  -- Sharp bettor detection (15-min HOP, 10-sec slide)
  avg_odds_15min       DOUBLE,

  -- Fraud / multi-accounting (15-min HOP, 10-sec slide)
  geo_mismatches_15min BIGINT,

  -- Operator P&L exposure (30-min HOP, 10-sec slide)
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
