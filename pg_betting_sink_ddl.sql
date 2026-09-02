-- PostgreSQL sink tables for betting_features scenario
-- Run as: sudo -i -u postgres psql -a -w -f /pg_betting_sink_ddl.sql

\c betting

-- Audit trail for risk decisions — written by the Flink inference job
CREATE TABLE IF NOT EXISTS public.risk_score_log (
  bet_id            VARCHAR(100) PRIMARY KEY,
  player_id         VARCHAR(50),
  risk_level        VARCHAR(20),   -- FRAUD_HIGH | RG_ALERT | SHARP_BETTOR | STAKE_LIMIT | NORMAL
  risk_reasons      TEXT,          -- comma-separated rule codes
  event_ts          TIMESTAMP,     -- when the original bet event occurred
  enrichment_lag_ms BIGINT,        -- ms from bet event to risk score (pipeline latency)
  score_time        TIMESTAMP      -- when the risk score was produced
);

-- For already-deployed environments: add column without dropping the table
ALTER TABLE public.risk_score_log
  ADD COLUMN IF NOT EXISTS enrichment_lag_ms BIGINT;

ALTER TABLE public.risk_score_log OWNER TO root;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO root;
