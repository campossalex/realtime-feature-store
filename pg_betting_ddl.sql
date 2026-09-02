-- PostgreSQL schema for the betting_features_oss scenario.
-- Applied automatically as /docker-entrypoint-initdb.d/01_schema.sql on first
-- container start, i.e. already connected to the 'betting' database.
--
-- Deliberately narrower than scenarios/betting_features/pg_betting_ddl.sql (the VVP
-- variant, which is run by hand against a bare-metal PostgreSQL):
--
--   * No CREATE USER root / CREATE DATABASE betting — the postgres:15 image creates
--     both from POSTGRES_USER / POSTGRES_DB, and initdb scripts already run inside the
--     target database, so the \c and \gexec meta-commands are unnecessary here.
--
--   * No replication slot, publication, or cdc_user. All CDC setup lives in
--     pg_cdc_setup.sql (03_cdc.sql), which runs after this file.
--
-- Do NOT copy the CDC sections back in from the VVP version. Because initdb scripts run
-- in alphabetical order, a 'betting_pub FOR ALL TABLES' created here would satisfy the
-- IF NOT EXISTS guard in pg_cdc_setup.sql and silently suppress its narrower per-table
-- publication — and the extra flink_cdc_betting_slot would then sit unused, pinning WAL
-- forever because nothing consumes it.

-- Player profiles — CDC source table
CREATE TABLE IF NOT EXISTS public.players (
  player_id         VARCHAR(50)  PRIMARY KEY,
  username          VARCHAR(100) NOT NULL,
  kyc_tier          VARCHAR(20)  NOT NULL DEFAULT 'BASIC',  -- BASIC | STANDARD | ENHANCED
  registration_date DATE         NOT NULL,
  country           VARCHAR(10)  NOT NULL
);

ALTER TABLE public.players OWNER TO root;

-- Player wallets — CDC source table (balance updated by settlement payouts)
CREATE TABLE IF NOT EXISTS public.player_wallets (
  player_id            VARCHAR(50)   PRIMARY KEY REFERENCES public.players(player_id),
  balance              DECIMAL(14,2) NOT NULL DEFAULT 0.00,
  lifetime_deposits    DECIMAL(14,2) NOT NULL DEFAULT 0.00,
  lifetime_withdrawals DECIMAL(14,2) NOT NULL DEFAULT 0.00,
  updated_at           TIMESTAMP     NOT NULL DEFAULT NOW()
);

ALTER TABLE public.player_wallets OWNER TO root;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO root;
