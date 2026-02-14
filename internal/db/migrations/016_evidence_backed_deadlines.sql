-- Migration 016: evidence-backed deadlines + strict rolling evidence

ALTER TABLE opportunities
    ADD COLUMN IF NOT EXISTS rolling_evidence BOOLEAN NOT NULL DEFAULT false;

-- Convert legacy deadlines string array into evidence objects when needed.
UPDATE opportunities o
SET deadlines = converted.payload
FROM (
    SELECT id,
           (
             SELECT COALESCE(
               jsonb_agg(
                 jsonb_build_object(
                   'source', 'legacy',
                   'url', external_url,
                   'snippet', value,
                   'parsed_date_iso', value,
                   'label', 'legacy_deadline',
                   'confidence', 0.5
                 )
               ),
               '[]'::jsonb
             )
             FROM jsonb_array_elements_text(deadlines)
           ) AS payload
    FROM opportunities
    WHERE deadlines IS NOT NULL
      AND jsonb_typeof(deadlines) = 'array'
      AND jsonb_array_length(deadlines) > 0
      AND jsonb_typeof(deadlines->0) = 'string'
) converted
WHERE o.id = converted.id;

CREATE INDEX IF NOT EXISTS idx_opp_open_strict
ON opportunities (next_deadline_at, close_at, updated_at DESC)
WHERE normalized_status = 'open'
  AND is_results_page = false;

CREATE INDEX IF NOT EXISTS idx_opp_rolling_evidence
ON opportunities (rolling_evidence)
WHERE rolling_evidence = true;
