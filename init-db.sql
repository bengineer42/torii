-- Initialize PostgreSQL database for Torii indexer
-- This script will be run when the container starts

-- Create extensions that might be useful for indexing
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

-- Set some PostgreSQL settings optimized for indexing workloads
ALTER SYSTEM SET synchronous_commit = off;
ALTER SYSTEM SET wal_level = minimal;
ALTER SYSTEM SET archive_mode = off;
ALTER SYSTEM SET max_wal_senders = 0;

-- Create the main database (if not already created by POSTGRES_DB)
-- The database 'torii' should already be created by the environment variable

-- You can add any additional initialization here
SELECT 'PostgreSQL database initialized for Torii indexer' as status;