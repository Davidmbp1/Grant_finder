-- Add source_id column
ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS source_id TEXT;

-- Populate source_id for existing records (fallback to hash of external_url or uuid)
-- For now, let's just make sure it's not null for future constraints if we wanted constraint NOT NULL.
-- But unique index allows nulls? We want (source_domain, source_id) to be unique.
-- Existing records might not have source_id. Let's try to backfill with UUID or URL hash if needed.
-- For this migration, we'll leave it nullable but creating the index.

-- Drop old unique constraint on external_url if it exists (it was ON CONFLICT(external_url))
-- We might need to drop the constraint by name. Usually "opportunities_external_url_key".
ALTER TABLE opportunities DROP CONSTRAINT IF EXISTS opportunities_external_url_key;

-- Create new unique index
CREATE UNIQUE INDEX IF NOT EXISTS uniq_source_domain_id ON opportunities(source_domain, source_id);
