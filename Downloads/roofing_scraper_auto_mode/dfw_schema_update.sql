-- DFW Schema Update Script
-- Adds DFW boolean column to all lead tables for geographic filtering

-- Add DFW column to zillow_leads
ALTER TABLE zillow_leads 
ADD COLUMN IF NOT EXISTS dfw BOOLEAN DEFAULT FALSE;

-- Add DFW column to redfin_leads  
ALTER TABLE redfin_leads
ADD COLUMN IF NOT EXISTS dfw BOOLEAN DEFAULT FALSE;

-- Add DFW column to cad_leads
ALTER TABLE cad_leads
ADD COLUMN IF NOT EXISTS dfw BOOLEAN DEFAULT FALSE;

-- Add DFW column to permit_leads
ALTER TABLE permit_leads
ADD COLUMN IF NOT EXISTS dfw BOOLEAN DEFAULT FALSE;

-- Add DFW column to storm_events
ALTER TABLE storm_events
ADD COLUMN IF NOT EXISTS dfw BOOLEAN DEFAULT FALSE;

-- Create index on DFW column for faster filtering across all tables
CREATE INDEX IF NOT EXISTS idx_zillow_leads_dfw ON zillow_leads(dfw);
CREATE INDEX IF NOT EXISTS idx_redfin_leads_dfw ON redfin_leads(dfw);
CREATE INDEX IF NOT EXISTS idx_cad_leads_dfw ON cad_leads(dfw);
CREATE INDEX IF NOT EXISTS idx_permit_leads_dfw ON permit_leads(dfw);
CREATE INDEX IF NOT EXISTS idx_storm_events_dfw ON storm_events(dfw);

-- Create composite indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_zillow_leads_dfw_score ON zillow_leads(dfw, lead_score);
CREATE INDEX IF NOT EXISTS idx_redfin_leads_dfw_score ON redfin_leads(dfw, lead_score);
CREATE INDEX IF NOT EXISTS idx_cad_leads_dfw_score ON cad_leads(dfw, lead_score);
CREATE INDEX IF NOT EXISTS idx_permit_leads_dfw_priority ON permit_leads(dfw, lead_score);

-- Create views for DFW-only data
CREATE OR REPLACE VIEW dfw_zillow_leads AS
SELECT * FROM zillow_leads WHERE dfw = TRUE;

CREATE OR REPLACE VIEW dfw_redfin_leads AS
SELECT * FROM redfin_leads WHERE dfw = TRUE;

CREATE OR REPLACE VIEW dfw_cad_leads AS
SELECT * FROM cad_leads WHERE dfw = TRUE;

CREATE OR REPLACE VIEW dfw_permit_leads AS
SELECT * FROM permit_leads WHERE dfw = TRUE;

CREATE OR REPLACE VIEW dfw_storm_events AS
SELECT * FROM storm_events WHERE dfw = TRUE;

-- Create comprehensive DFW leads view combining all sources
CREATE OR REPLACE VIEW dfw_all_leads AS
SELECT 
    'zillow' as source,
    id,
    address_text as address,
    city,
    state,
    zip_code,
    county,
    price::numeric as value,
    lead_score,
    scraped_at,
    dfw
FROM zillow_leads WHERE dfw = TRUE

UNION ALL

SELECT 
    'redfin' as source,
    id,
    address_text as address,
    city,
    state,
    zip_code,
    county,
    price::numeric as value,
    lead_score,
    scraped_at,
    dfw
FROM redfin_leads WHERE dfw = TRUE

UNION ALL

SELECT 
    'cad' as source,
    id,
    property_address as address,
    city,
    state,
    zipcode as zip_code,
    county,
    market_value::numeric as value,
    lead_score,
    scraped_at,
    dfw
FROM cad_leads WHERE dfw = TRUE

UNION ALL

SELECT 
    'permit' as source,
    id,
    address,
    city,
    state,
    zipcode as zip_code,
    county,
    permit_value::numeric as value,
    lead_score,
    created_at as scraped_at,
    dfw
FROM permit_leads WHERE dfw = TRUE;

-- Add comments to document the schema changes
COMMENT ON COLUMN zillow_leads.dfw IS 'Boolean flag indicating if lead is in Dallas-Fort Worth metropolitan area';
COMMENT ON COLUMN redfin_leads.dfw IS 'Boolean flag indicating if lead is in Dallas-Fort Worth metropolitan area';
COMMENT ON COLUMN cad_leads.dfw IS 'Boolean flag indicating if lead is in Dallas-Fort Worth metropolitan area';
COMMENT ON COLUMN permit_leads.dfw IS 'Boolean flag indicating if lead is in Dallas-Fort Worth metropolitan area';
COMMENT ON COLUMN storm_events.dfw IS 'Boolean flag indicating if storm event affects Dallas-Fort Worth metropolitan area';

-- Create function to get DFW lead counts
CREATE OR REPLACE FUNCTION get_dfw_lead_counts()
RETURNS TABLE(
    source TEXT,
    total_leads BIGINT,
    dfw_leads BIGINT,
    dfw_percentage NUMERIC
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        'zillow'::TEXT as source,
        COUNT(*) as total_leads,
        COUNT(*) FILTER (WHERE dfw = TRUE) as dfw_leads,
        ROUND((COUNT(*) FILTER (WHERE dfw = TRUE)::NUMERIC / COUNT(*)) * 100, 2) as dfw_percentage
    FROM zillow_leads
    
    UNION ALL
    
    SELECT 
        'redfin'::TEXT as source,
        COUNT(*) as total_leads,
        COUNT(*) FILTER (WHERE dfw = TRUE) as dfw_leads,
        ROUND((COUNT(*) FILTER (WHERE dfw = TRUE)::NUMERIC / COUNT(*)) * 100, 2) as dfw_percentage
    FROM redfin_leads
    
    UNION ALL
    
    SELECT 
        'cad'::TEXT as source,
        COUNT(*) as total_leads,
        COUNT(*) FILTER (WHERE dfw = TRUE) as dfw_leads,
        ROUND((COUNT(*) FILTER (WHERE dfw = TRUE)::NUMERIC / COUNT(*)) * 100, 2) as dfw_percentage
    FROM cad_leads
    
    UNION ALL
    
    SELECT 
        'permit'::TEXT as source,
        COUNT(*) as total_leads,
        COUNT(*) FILTER (WHERE dfw = TRUE) as dfw_leads,
        ROUND((COUNT(*) FILTER (WHERE dfw = TRUE)::NUMERIC / COUNT(*)) * 100, 2) as dfw_percentage
    FROM permit_leads
    
    UNION ALL
    
    SELECT 
        'storm'::TEXT as source,
        COUNT(*) as total_leads,
        COUNT(*) FILTER (WHERE dfw = TRUE) as dfw_leads,
        ROUND((COUNT(*) FILTER (WHERE dfw = TRUE)::NUMERIC / COUNT(*)) * 100, 2) as dfw_percentage
    FROM storm_events;
END;
$$ LANGUAGE plpgsql;

-- Usage example:
-- SELECT * FROM get_dfw_lead_counts();