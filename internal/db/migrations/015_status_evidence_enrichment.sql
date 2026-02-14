-- Migration 015: evidence-backed status fields for adapter/enrichment flow

ALTER TABLE opportunities
    ADD COLUMN IF NOT EXISTS source_evidence_json JSONB,
    ADD COLUMN IF NOT EXISTS status_confidence DOUBLE PRECISION NOT NULL DEFAULT 0.0;

CREATE INDEX IF NOT EXISTS idx_opp_status_confidence
ON opportunities (status_confidence ASC)
WHERE normalized_status IN ('open', 'upcoming', 'needs_review');
