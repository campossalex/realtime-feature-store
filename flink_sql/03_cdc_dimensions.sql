-- ============================================================
-- Step 3 – CDC dimensions (Pipeline 1, part 1)
--
-- One streaming job. Streams player and wallet dimensions from PostgreSQL into the
-- Fluss feature store via the postgres-cdc connector.
--
-- Run this BEFORE 04_feature_calculations.sql: it populates the dimension columns
-- (kyc_tier, balance, lifetime_deposits) that the risk rules read alongside the
-- windowed features.
--
-- On first run this takes a full snapshot of both PostgreSQL tables before any row
-- reaches Fluss, so expect a delay proportional to the row count. Snapshot size is set
-- by num_players in datagen/configuration.ini.
--
-- The INSERT names only the columns this job owns — Fluss updates just those columns
-- and leaves the rest of the row untouched.
--
--   docker compose exec sql-client /opt/flink/bin/sql-client.sh \
--     --init /opt/flink_sql/01_sources_and_catalogs.sql \
--     -f    /opt/flink_sql/03_cdc_dimensions.sql
-- ============================================================


-- ── Job A: CDC dimensions ─────────────────────────────────────────────────────
-- Fires on every change to players or player_wallets tables.

SET 'pipeline.name' = 'jobA-cdc-dimensions';

INSERT INTO fluss.betting.player_feature_store (
  player_id,
  kyc_tier, balance, lifetime_deposits,
  bets_last_10min,
  total_stake_30min,
  distinct_sports_30min,
  current_loss_streak,
  live_bet_ratio_15min,
  avg_odds_15min,
  geo_mismatches_15min,
  net_pnl_30min,
  last_updated_at
)
SELECT
  p.player_id,
  p.kyc_tier,
  w.balance,
  w.lifetime_deposits,
  CAST(0    AS BIGINT)        AS bets_last_10min,
  CAST(0.00 AS DECIMAL(14,2)) AS total_stake_30min,
  CAST(0    AS BIGINT)        AS distinct_sports_30min,
  CAST(0    AS BIGINT)        AS current_loss_streak,
  CAST(0.0  AS DOUBLE)        AS live_bet_ratio_15min,
  CAST(0.0  AS DOUBLE)        AS avg_odds_15min,
  CAST(0    AS BIGINT)        AS geo_mismatches_15min,
  CAST(0.00 AS DECIMAL(14,2)) AS net_pnl_30min,
  CURRENT_TIMESTAMP           AS last_updated_at
FROM players_cdc AS p
JOIN player_wallets_cdc AS w ON p.player_id = w.player_id;
