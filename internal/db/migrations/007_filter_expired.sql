-- Migration 007: Filter and mark expired grants
-- This migration marks grants as closed/archived based on their deadlines and dates

-- 1. Mark grants with past deadlines as 'closed'
UPDATE opportunities
SET opp_status = 'closed'
WHERE deadline_at IS NOT NULL 
  AND deadline_at < NOW()
  AND opp_status NOT IN ('closed', 'archived', 'funded');

-- 2. Mark very old grants without deadlines as 'archived'
-- Grants from 2016-2018 that don't have deadlines and aren't rolling
UPDATE opportunities
SET opp_status = 'archived'
WHERE deadline_at IS NULL
  AND is_rolling = false
  AND (
    (open_date IS NOT NULL AND open_date < NOW() - INTERVAL '2 years') OR
    (open_date IS NULL AND created_at < NOW() - INTERVAL '2 years')
  )
  AND opp_status NOT IN ('closed', 'archived', 'funded');

-- 3. More aggressive: Archive grants created before 2020 (very old)
UPDATE opportunities
SET opp_status = 'archived'
WHERE created_at < '2020-01-01'
  AND opp_status NOT IN ('closed', 'archived', 'funded');

-- 4. Archive grants with open_date before 2020
UPDATE opportunities
SET opp_status = 'archived'
WHERE open_date IS NOT NULL
  AND open_date < '2020-01-01'
  AND opp_status NOT IN ('closed', 'archived', 'funded');

-- 3. Optional: Delete very old grants (2016-2018) if they have minimal information
-- Uncomment if you want to delete instead of archive
-- DELETE FROM opportunities
-- WHERE created_at < '2019-01-01'
--   AND opp_status IN ('archived', 'closed')
--   AND (summary IS NULL OR summary = '')
--   AND (description_html IS NULL OR description_html = '');
