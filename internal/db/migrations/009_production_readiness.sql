-- Phase 1: Production Readiness Constraints & Enums

-- 1. Create status ENUM
DO $$ BEGIN
    CREATE TYPE opp_status_enum AS ENUM ('posted', 'closed', 'forthcoming', 'unknown', 'archived');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- 2. Clean up opportunities table for constraints
-- Ensure no NULL source_domains (redundant check, handled by prep script)
DELETE FROM opportunities WHERE source_domain IS NULL;

-- 3. Alter table to add constraints and new fields
ALTER TABLE opportunities
    -- Constraint: source_domain NOT NULL
    ALTER COLUMN source_domain SET NOT NULL,
    -- Add new fields
    ADD COLUMN IF NOT EXISTS source_run_id TEXT,
    ADD COLUMN IF NOT EXISTS canonical_url TEXT,
    ADD COLUMN IF NOT EXISTS raw_url TEXT,
    ADD COLUMN IF NOT EXISTS content_type TEXT DEFAULT 'html',
    ADD COLUMN IF NOT EXISTS data_quality_score JSONB DEFAULT '{}'::jsonb,
    -- Change amount to NUMERIC (safer for money)
    ALTER COLUMN amount_min TYPE NUMERIC USING amount_min::numeric,
    ALTER COLUMN amount_max TYPE NUMERIC USING amount_max::numeric;

-- 4. Constraint: UNIQUE (source_domain, source_id)
-- We drop the old index if it exists and create a proper UNIQUE constraint
DROP INDEX IF EXISTS idx_opportunities_source_id;
DROP INDEX IF EXISTS opportunities_source_uq; -- Clean up from 006 if replacing

DO $$ BEGIN
    ALTER TABLE opportunities 
        ADD CONSTRAINT uq_opportunities_source_domain_id UNIQUE (source_domain, source_id);
EXCEPTION
    WHEN duplicate_object THEN null;
    WHEN duplicate_table THEN null; -- Handle SQLSTATE 42P07
    WHEN OTHERS THEN
        -- Check specific error code for "relation already exists" just in case
        IF SQLSTATE = '42P07' THEN null;
        ELSE RAISE;
        END IF;
END $$;

-- 5. Add index for "Open" queries (performance)
CREATE INDEX IF NOT EXISTS idx_opportunities_status_deadline 
    ON opportunities (opp_status, deadline_at)
    WHERE opp_status != 'closed';
