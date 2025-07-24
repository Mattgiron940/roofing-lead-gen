-- Supabase Table Creation Scripts for Roofing Lead Scrapers
-- Generated: 2024-12-19
-- Tables: zillow_leads, redfin_leads, cad_leads, permit_leads, storm_events

-- Enable RLS (Row Level Security) for all tables
-- Note: Configure RLS policies as needed for your application

-- =====================================================
-- ZILLOW LEADS TABLE
-- =====================================================
CREATE TABLE IF NOT EXISTS zillow_leads (
    id BIGSERIAL PRIMARY KEY,
    address_text TEXT,
    city TEXT,
    state TEXT,
    zip_code TEXT,
    county TEXT,
    price BIGINT,
    num_bedrooms INTEGER,
    num_bathrooms DECIMAL,
    square_feet INTEGER,
    year_built INTEGER,
    property_type TEXT,
    lot_size_sqft INTEGER,
    sold_date TEXT,
    days_on_zillow INTEGER,
    zpid TEXT,
    price_per_sqft TEXT,
    zillow_url TEXT,
    lead_score INTEGER,
    hoa_fee TEXT,
    parking_spaces TEXT,
    lead_status TEXT,
    priority TEXT,
    routing_tags TEXT,
    next_action TEXT,
    assigned_to TEXT,
    score_breakdown JSONB,
    last_routed_at TIMESTAMP WITH TIME ZONE,
    last_contacted_at TIMESTAMP WITH TIME ZONE,
    contact_attempts INTEGER DEFAULT 0,
    notes TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Add indexes for common queries
CREATE INDEX IF NOT EXISTS idx_zillow_leads_zip_code ON zillow_leads(zip_code);
CREATE INDEX IF NOT EXISTS idx_zillow_leads_city ON zillow_leads(city);
CREATE INDEX IF NOT EXISTS idx_zillow_leads_lead_score ON zillow_leads(lead_score);
CREATE INDEX IF NOT EXISTS idx_zillow_leads_price ON zillow_leads(price);
CREATE INDEX IF NOT EXISTS idx_zillow_leads_year_built ON zillow_leads(year_built);

-- Enable RLS
ALTER TABLE zillow_leads ENABLE ROW LEVEL SECURITY;

-- =====================================================
-- REDFIN LEADS TABLE
-- =====================================================
CREATE TABLE IF NOT EXISTS redfin_leads (
    id BIGSERIAL PRIMARY KEY,
    address_text TEXT,
    city TEXT,
    state TEXT,
    zip_code TEXT,
    county TEXT,
    price BIGINT,
    num_bedrooms INTEGER,
    num_bathrooms DECIMAL,
    square_feet INTEGER,
    year_built INTEGER,
    property_type TEXT,
    lot_size_sqft INTEGER,
    sold_date TEXT,
    days_on_redfin INTEGER,
    mls_number TEXT,
    price_per_sqft TEXT,
    redfin_url TEXT,
    lead_score INTEGER,
    hoa_fee TEXT,
    parking_spaces TEXT,
    lead_status TEXT,
    priority TEXT,
    routing_tags TEXT,
    next_action TEXT,
    assigned_to TEXT,
    score_breakdown JSONB,
    last_routed_at TIMESTAMP WITH TIME ZONE,
    last_contacted_at TIMESTAMP WITH TIME ZONE,
    contact_attempts INTEGER DEFAULT 0,
    notes TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Add indexes for common queries
CREATE INDEX IF NOT EXISTS idx_redfin_leads_zip_code ON redfin_leads(zip_code);
CREATE INDEX IF NOT EXISTS idx_redfin_leads_city ON redfin_leads(city);
CREATE INDEX IF NOT EXISTS idx_redfin_leads_lead_score ON redfin_leads(lead_score);
CREATE INDEX IF NOT EXISTS idx_redfin_leads_price ON redfin_leads(price);
CREATE INDEX IF NOT EXISTS idx_redfin_leads_year_built ON redfin_leads(year_built);
CREATE INDEX IF NOT EXISTS idx_redfin_leads_mls_number ON redfin_leads(mls_number);

-- Enable RLS
ALTER TABLE redfin_leads ENABLE ROW LEVEL SECURITY;

-- =====================================================
-- CAD LEADS TABLE (Texas County Appraisal Districts)
-- =====================================================
CREATE TABLE IF NOT EXISTS cad_leads (
    id BIGSERIAL PRIMARY KEY,
    account_number TEXT,
    owner_name TEXT,
    address_text TEXT,
    city TEXT,
    county TEXT,
    zip_code TEXT,
    property_type TEXT,
    year_built INTEGER,
    square_feet INTEGER,
    lot_size_acres DECIMAL,
    appraised_value BIGINT,
    market_value BIGINT,
    homestead_exemption BOOLEAN,
    last_sale_date TEXT,
    last_sale_price BIGINT,
    cad_url TEXT,
    lead_score INTEGER,
    lead_status TEXT,
    priority TEXT,
    routing_tags TEXT,
    next_action TEXT,
    assigned_to TEXT,
    score_breakdown JSONB,
    last_routed_at TIMESTAMP WITH TIME ZONE,
    last_contacted_at TIMESTAMP WITH TIME ZONE,
    contact_attempts INTEGER DEFAULT 0,
    notes TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Add indexes for common queries
CREATE INDEX IF NOT EXISTS idx_cad_leads_zip_code ON cad_leads(zip_code);
CREATE INDEX IF NOT EXISTS idx_cad_leads_city ON cad_leads(city);
CREATE INDEX IF NOT EXISTS idx_cad_leads_county ON cad_leads(county);
CREATE INDEX IF NOT EXISTS idx_cad_leads_lead_score ON cad_leads(lead_score);
CREATE INDEX IF NOT EXISTS idx_cad_leads_appraised_value ON cad_leads(appraised_value);
CREATE INDEX IF NOT EXISTS idx_cad_leads_year_built ON cad_leads(year_built);
CREATE INDEX IF NOT EXISTS idx_cad_leads_account_number ON cad_leads(account_number);
CREATE INDEX IF NOT EXISTS idx_cad_leads_owner_name ON cad_leads(owner_name);

-- Enable RLS
ALTER TABLE cad_leads ENABLE ROW LEVEL SECURITY;

-- =====================================================
-- PERMIT LEADS TABLE
-- =====================================================
CREATE TABLE IF NOT EXISTS permit_leads (
    id BIGSERIAL PRIMARY KEY,
    permit_id TEXT,
    address_text TEXT,
    city TEXT,
    zip_code TEXT,
    permit_type TEXT,
    work_description TEXT,
    date_filed TEXT,
    permit_value TEXT,
    contractor_name TEXT,
    status TEXT,
    lead_priority INTEGER,
    lead_status TEXT,
    priority TEXT,
    routing_tags TEXT,
    next_action TEXT,
    assigned_to TEXT,
    score_breakdown JSONB,
    last_routed_at TIMESTAMP WITH TIME ZONE,
    last_contacted_at TIMESTAMP WITH TIME ZONE,
    contact_attempts INTEGER DEFAULT 0,
    notes TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Add indexes for common queries
CREATE INDEX IF NOT EXISTS idx_permit_leads_zip_code ON permit_leads(zip_code);
CREATE INDEX IF NOT EXISTS idx_permit_leads_city ON permit_leads(city);
CREATE INDEX IF NOT EXISTS idx_permit_leads_permit_type ON permit_leads(permit_type);
CREATE INDEX IF NOT EXISTS idx_permit_leads_lead_priority ON permit_leads(lead_priority);
CREATE INDEX IF NOT EXISTS idx_permit_leads_permit_id ON permit_leads(permit_id);
CREATE INDEX IF NOT EXISTS idx_permit_leads_contractor_name ON permit_leads(contractor_name);
CREATE INDEX IF NOT EXISTS idx_permit_leads_date_filed ON permit_leads(date_filed);

-- Enable RLS
ALTER TABLE permit_leads ENABLE ROW LEVEL SECURITY;

-- =====================================================
-- STORM EVENTS TABLE
-- =====================================================
CREATE TABLE IF NOT EXISTS storm_events (
    id BIGSERIAL PRIMARY KEY,
    event_id TEXT UNIQUE,
    event_type TEXT,
    event_date TEXT,
    event_time TEXT,
    severity TEXT,
    hail_size TEXT,
    wind_speed TEXT,
    affected_counties TEXT,
    affected_zipcodes TEXT,
    damage_estimate TEXT,
    insurance_claims_expected TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Add indexes for common queries
CREATE INDEX IF NOT EXISTS idx_storm_events_event_id ON storm_events(event_id);
CREATE INDEX IF NOT EXISTS idx_storm_events_event_date ON storm_events(event_date);
CREATE INDEX IF NOT EXISTS idx_storm_events_event_type ON storm_events(event_type);
CREATE INDEX IF NOT EXISTS idx_storm_events_severity ON storm_events(severity);
CREATE INDEX IF NOT EXISTS idx_storm_events_affected_zipcodes ON storm_events USING GIN (to_tsvector('english', affected_zipcodes));

-- Enable RLS
ALTER TABLE storm_events ENABLE ROW LEVEL SECURITY;

-- =====================================================
-- UTILITY VIEWS
-- =====================================================

-- Combined leads view (union of all property leads)
CREATE OR REPLACE VIEW all_property_leads AS
SELECT 
    'zillow' as source,
    id,
    address_text,
    city,
    state,
    zip_code,
    county,
    price,
    num_bedrooms,
    num_bathrooms,
    square_feet,
    year_built,
    property_type,
    lead_score,
    created_at
FROM zillow_leads
UNION ALL
SELECT 
    'redfin' as source,
    id,
    address_text,
    city,
    state,
    zip_code,
    county,
    price,
    num_bedrooms,
    num_bathrooms,
    square_feet,
    year_built,
    property_type,
    lead_score,
    created_at
FROM redfin_leads
UNION ALL
SELECT 
    'cad' as source,
    id,
    address_text,
    city,
    'TX' as state,
    zip_code,
    county,
    appraised_value as price,
    NULL as num_bedrooms,
    NULL as num_bathrooms,
    square_feet,
    year_built,
    property_type,
    lead_score,
    created_at
FROM cad_leads;

-- High priority leads view (lead_score >= 8)
CREATE OR REPLACE VIEW high_priority_leads AS
SELECT * FROM all_property_leads 
WHERE lead_score >= 8
ORDER BY lead_score DESC, created_at DESC;

-- Recent leads view (last 30 days)
CREATE OR REPLACE VIEW recent_leads AS
SELECT * FROM all_property_leads 
WHERE created_at >= NOW() - INTERVAL '30 days'
ORDER BY created_at DESC;

-- Leads by city summary
CREATE OR REPLACE VIEW leads_by_city AS
SELECT 
    city,
    COUNT(*) as total_leads,
    AVG(lead_score) as avg_lead_score,
    COUNT(CASE WHEN lead_score >= 8 THEN 1 END) as high_priority_count,
    MAX(created_at) as latest_scrape
FROM all_property_leads
GROUP BY city
ORDER BY total_leads DESC;

-- Storm-affected areas summary
CREATE OR REPLACE VIEW storm_affected_areas AS
SELECT 
    event_id,
    event_type,
    event_date,
    severity,
    hail_size,
    string_to_array(affected_zipcodes, ',') as zip_array,
    string_to_array(affected_counties, ',') as county_array,
    created_at
FROM storm_events
ORDER BY event_date DESC;

-- =====================================================
-- SAMPLE RLS POLICIES (CUSTOMIZE AS NEEDED)
-- =====================================================

-- Example: Allow all authenticated users to read all leads
-- CREATE POLICY "Allow authenticated read access" ON zillow_leads FOR SELECT USING (auth.role() = 'authenticated');
-- CREATE POLICY "Allow authenticated read access" ON redfin_leads FOR SELECT USING (auth.role() = 'authenticated');
-- CREATE POLICY "Allow authenticated read access" ON cad_leads FOR SELECT USING (auth.role() = 'authenticated');
-- CREATE POLICY "Allow authenticated read access" ON permit_leads FOR SELECT USING (auth.role() = 'authenticated');
-- CREATE POLICY "Allow authenticated read access" ON storm_events FOR SELECT USING (auth.role() = 'authenticated');

-- Example: Allow service role to insert/update
-- CREATE POLICY "Allow service role full access" ON zillow_leads FOR ALL USING (auth.role() = 'service_role');
-- CREATE POLICY "Allow service role full access" ON redfin_leads FOR ALL USING (auth.role() = 'service_role');
-- CREATE POLICY "Allow service role full access" ON cad_leads FOR ALL USING (auth.role() = 'service_role');
-- CREATE POLICY "Allow service role full access" ON permit_leads FOR ALL USING (auth.role() = 'service_role');
-- CREATE POLICY "Allow service role full access" ON storm_events FOR ALL USING (auth.role() = 'service_role');

-- =====================================================
-- TRIGGERS FOR UPDATED_AT TIMESTAMPS
-- =====================================================

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Apply triggers to all tables
CREATE TRIGGER update_zillow_leads_updated_at BEFORE UPDATE ON zillow_leads FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_redfin_leads_updated_at BEFORE UPDATE ON redfin_leads FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_cad_leads_updated_at BEFORE UPDATE ON cad_leads FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_permit_leads_updated_at BEFORE UPDATE ON permit_leads FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_storm_events_updated_at BEFORE UPDATE ON storm_events FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- =====================================================
-- SETUP COMPLETE
-- =====================================================

-- To use these tables:
-- 1. Run this SQL file in your Supabase SQL editor
-- 2. Configure RLS policies based on your security requirements
-- 3. Update your scraper .env files with SUPABASE_URL and SUPABASE_KEY
-- 4. Test scrapers to ensure data is inserting correctly

-- Useful queries for monitoring:
-- SELECT COUNT(*) FROM zillow_leads;
-- SELECT COUNT(*) FROM redfin_leads;
-- SELECT COUNT(*) FROM cad_leads;
-- SELECT COUNT(*) FROM permit_leads;
-- SELECT COUNT(*) FROM storm_events;
-- SELECT * FROM leads_by_city LIMIT 10;
-- SELECT * FROM high_priority_leads LIMIT 10;