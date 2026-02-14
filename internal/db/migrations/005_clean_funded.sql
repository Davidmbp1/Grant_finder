-- Remove records that are clearly funded awards or have 'award' doc_type
DELETE FROM opportunities 
WHERE opp_status = 'funded' 
   OR doc_type = 'award'
   OR source_domain IN ('nsf.gov', 'nih.gov', 'openalex.org');
