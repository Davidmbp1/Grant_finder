-- Migration 012: Align status domain with application logic
-- Application code and historical data use 'funded' as a terminal status.

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_type WHERE typname = 'opp_status_enum') THEN
        ALTER TYPE opp_status_enum ADD VALUE IF NOT EXISTS 'funded';
    END IF;
END $$;
