-- Migration 011: Improve ingest_runs schema for production operations

-- 1. Ensure pgcrypto extension exists for UUID generation
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- 2. Add validation and defaults to ingest_runs
ALTER TABLE ingest_runs 
    ALTER COLUMN status SET DEFAULT 'running',
    ALTER COLUMN status SET NOT NULL,
    ALTER COLUMN started_at SET NOT NULL;

-- 3. Add performance index for source history queries
CREATE INDEX IF NOT EXISTS idx_ingest_runs_source_started 
    ON ingest_runs (source_id, started_at DESC);
