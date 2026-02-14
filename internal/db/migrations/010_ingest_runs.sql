-- Migration 006: Add ingest_runs table for observability

CREATE TABLE IF NOT EXISTS ingest_runs (
    run_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id TEXT NOT NULL,
    started_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    completed_at TIMESTAMP WITH TIME ZONE,
    items_found INT DEFAULT 0,
    items_saved INT DEFAULT 0,
    errors INT DEFAULT 0,
    status TEXT CHECK (status IN ('running', 'completed', 'failed')),
    details JSONB -- specific error info or config snapshot
);

CREATE INDEX IF NOT EXISTS idx_ingest_runs_source_id ON ingest_runs(source_id);
CREATE INDEX IF NOT EXISTS idx_ingest_runs_started_at ON ingest_runs(started_at DESC);
