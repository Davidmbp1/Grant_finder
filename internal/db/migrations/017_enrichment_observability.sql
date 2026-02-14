-- Migration 017: enrichment staleness + fetch observability fields

ALTER TABLE opportunities
    ADD COLUMN IF NOT EXISTS last_enriched_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS fetch_last_status_code INTEGER,
    ADD COLUMN IF NOT EXISTS fetch_last_bytes INTEGER,
    ADD COLUMN IF NOT EXISTS fetch_last_duration_ms INTEGER,
    ADD COLUMN IF NOT EXISTS fetch_blocked_detected BOOLEAN NOT NULL DEFAULT false;

CREATE INDEX IF NOT EXISTS idx_opp_last_enriched_at
ON opportunities (last_enriched_at)
WHERE normalized_status IN ('open', 'needs_review', 'upcoming');
