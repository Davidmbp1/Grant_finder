-- Migration 008: Auto-update status trigger
-- This trigger automatically updates opp_status to 'closed' when deadline passes

-- Function to update status when deadline passes
CREATE OR REPLACE FUNCTION update_opportunity_status_on_deadline()
RETURNS TRIGGER AS $$
BEGIN
    -- If deadline has passed and status is not already closed/archived/funded
    IF NEW.deadline_at IS NOT NULL 
       AND NEW.deadline_at < NOW() 
       AND NEW.opp_status NOT IN ('closed', 'archived', 'funded') THEN
        NEW.opp_status := 'closed';
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger that runs before INSERT or UPDATE
DROP TRIGGER IF EXISTS trigger_update_status_on_deadline ON opportunities;
CREATE TRIGGER trigger_update_status_on_deadline
    BEFORE INSERT OR UPDATE OF deadline_at, opp_status
    ON opportunities
    FOR EACH ROW
    EXECUTE FUNCTION update_opportunity_status_on_deadline();

-- Also create a function to periodically update status for existing records
-- This can be called via a cron job or scheduled task
CREATE OR REPLACE FUNCTION update_expired_opportunities_status()
RETURNS INTEGER AS $$
DECLARE
    updated_count INTEGER;
BEGIN
    UPDATE opportunities
    SET opp_status = 'closed'
    WHERE deadline_at IS NOT NULL
      AND deadline_at < NOW()
      AND opp_status NOT IN ('closed', 'archived', 'funded');
    
    GET DIAGNOSTICS updated_count = ROW_COUNT;
    RETURN updated_count;
END;
$$ LANGUAGE plpgsql;
