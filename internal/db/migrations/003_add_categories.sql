ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS categories text[];
ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS eligibility text[];

CREATE INDEX IF NOT EXISTS idx_opportunities_categories ON opportunities USING GIN (categories);
CREATE INDEX IF NOT EXISTS idx_opportunities_eligibility ON opportunities USING GIN (eligibility);
