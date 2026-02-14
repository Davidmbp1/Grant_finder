-- Add search_vector column for Full Text Search
ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS search_vector tsvector;

-- Create GIN index for fast keyword search
CREATE INDEX IF NOT EXISTS idx_opportunities_search_vector ON opportunities USING GIN (search_vector);

-- Create a function to update the search_vector automatically
CREATE OR REPLACE FUNCTION opportunities_search_vector_update() RETURNS trigger AS $$
BEGIN
    NEW.search_vector :=
        setweight(to_tsvector('english', COALESCE(NEW.title, '')), 'A') ||
        setweight(to_tsvector('english', COALESCE(NEW.description_html, '')), 'B') ||
        setweight(to_tsvector('english', COALESCE(NEW.summary, '')), 'C') ||
        setweight(to_tsvector('english', COALESCE(NEW.agency_name, '')), 'B');
    RETURN NEW;
END
$$ LANGUAGE plpgsql;

-- Create a trigger to call the function on INSERT or UPDATE
DROP TRIGGER IF EXISTS tsvectorupdate ON opportunities;
CREATE TRIGGER tsvectorupdate BEFORE INSERT OR UPDATE
    ON opportunities FOR EACH ROW EXECUTE PROCEDURE opportunities_search_vector_update();

-- Backfill existing rows
UPDATE opportunities SET search_vector =
    setweight(to_tsvector('english', COALESCE(title, '')), 'A') ||
    setweight(to_tsvector('english', COALESCE(description_html, '')), 'B') ||
    setweight(to_tsvector('english', COALESCE(summary, '')), 'C') ||
    setweight(to_tsvector('english', COALESCE(agency_name, '')), 'B');
