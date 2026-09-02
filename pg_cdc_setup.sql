-- CDC prerequisites for PostgreSQL Flink connector.
-- Runs automatically via docker-entrypoint-initdb.d after the schema DDL.
-- Creates the replication slot, publication, and CDC user used by
-- 01_sources_and_catalogs.sql (postgres-cdc connector).

-- Replication user
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'cdc_user') THEN
    CREATE ROLE cdc_user WITH LOGIN REPLICATION PASSWORD 'admin1';
  END IF;
END $$;

GRANT CONNECT ON DATABASE betting TO cdc_user;
GRANT USAGE ON SCHEMA public TO cdc_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO cdc_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO cdc_user;

-- Publication for all tables Flink CDC needs
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_publication WHERE pubname = 'betting_pub') THEN
    CREATE PUBLICATION betting_pub FOR TABLE players, player_wallets;
  END IF;
END $$;

-- Logical replication slots — ONE PER CDC SOURCE TABLE.
-- PostgreSQL permits a single active connection per slot, so sharing a slot between
-- players_cdc and player_wallets_cdc makes whichever starts second fail with:
--   PSQLException: ERROR: replication slot "..." is active for PID <n>
-- Because both sources feed the same job (Job A), that kills the job outright.
SELECT pg_create_logical_replication_slot('flink_cdc_players_slot', 'pgoutput')
WHERE NOT EXISTS (
  SELECT FROM pg_replication_slots WHERE slot_name = 'flink_cdc_players_slot'
);

SELECT pg_create_logical_replication_slot('flink_cdc_wallets_slot', 'pgoutput')
WHERE NOT EXISTS (
  SELECT FROM pg_replication_slots WHERE slot_name = 'flink_cdc_wallets_slot'
);
