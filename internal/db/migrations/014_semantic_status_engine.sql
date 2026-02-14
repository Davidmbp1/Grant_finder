-- Migration 014: Semantic status model for reliable grant lifecycle classification

DO $$
BEGIN
    CREATE TYPE normalized_status_enum AS ENUM ('open', 'upcoming', 'closed', 'archived', 'needs_review');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

ALTER TABLE opportunities
    ADD COLUMN IF NOT EXISTS source_status_raw TEXT,
    ADD COLUMN IF NOT EXISTS normalized_status normalized_status_enum NOT NULL DEFAULT 'needs_review',
    ADD COLUMN IF NOT EXISTS status_reason TEXT,
    ADD COLUMN IF NOT EXISTS next_deadline_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS expiration_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS close_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS open_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS deadlines JSONB,
    ADD COLUMN IF NOT EXISTS is_results_page BOOLEAN NOT NULL DEFAULT false;

-- Backfill new consistent open_at from historical open_date when available
UPDATE opportunities
SET open_at = open_date
WHERE open_at IS NULL AND open_date IS NOT NULL;

-- Initialize normalized status conservatively for existing rows
UPDATE opportunities
SET normalized_status = CASE
    WHEN COALESCE(opp_status, 'posted') IN ('closed', 'funded') THEN 'closed'::normalized_status_enum
    WHEN COALESCE(opp_status, 'posted') = 'archived' THEN 'archived'::normalized_status_enum
    WHEN is_rolling = true THEN 'open'::normalized_status_enum
    WHEN deadline_at IS NOT NULL AND deadline_at >= NOW() THEN 'open'::normalized_status_enum
    WHEN deadline_at IS NOT NULL AND deadline_at < NOW() THEN 'closed'::normalized_status_enum
    ELSE 'needs_review'::normalized_status_enum
END
WHERE normalized_status = 'needs_review';

-- Keep next_deadline aligned when we only have legacy deadline_at
UPDATE opportunities
SET next_deadline_at = deadline_at
WHERE next_deadline_at IS NULL AND deadline_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_opp_normalized_status ON opportunities (normalized_status);

CREATE INDEX IF NOT EXISTS idx_opp_open_next_deadline
ON opportunities (next_deadline_at ASC, updated_at DESC)
WHERE normalized_status = 'open';

CREATE INDEX IF NOT EXISTS idx_opp_open_rolling
ON opportunities (updated_at DESC)
WHERE normalized_status = 'open' AND is_rolling = true;
