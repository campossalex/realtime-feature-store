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
-- Writes ONLY the dimension columns. A player with no bet yet has NULL windowed
-- features, which the readers (05's COALESCEs, the inference service's _f()) treat as
-- their defaults. Writing zeros here instead would wipe live features: the windowed
-- jobs update per event, so a zero from a wallet change (or this job's snapshot
-- finishing after bets started) would stick until the player's next event.

SET 'pipeline.name' = 'jobA-cdc-dimensions';

INSERT INTO fluss.betting.player_feature_store (
  player_id,
  kyc_tier, balance, lifetime_deposits,
  last_updated_at
)
SELECT
  p.player_id,
  p.kyc_tier,
  w.balance,
  w.lifetime_deposits,
  CURRENT_TIMESTAMP           AS last_updated_at
FROM players_cdc AS p
JOIN player_wallets_cdc AS w ON p.player_id = w.player_id;
