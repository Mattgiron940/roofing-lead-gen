-- COMPREHENSIVE SCHEMA FIX
-- Addresses all schema mismatches between scrapers and database
-- Generated: 2025-07-24

-- =====================================================
-- SCHEMA ALIGNMENTS & MISSING COLUMNS
-- =====================================================

-- Add missing columns that scrapers expect
ALTER TABLE zillow_leads ADD COLUMN IF NOT EXISTS address TEXT;
ALTER TABLE zillow_leads ADD COLUMN IF NOT EXISTS dfw BOOLEAN DEFAULT false;
ALTER TABLE zillow_leads ADD COLUMN IF NOT EXISTS source TEXT DEFAULT 'zillow';
ALTER TABLE zillow_leads ADD COLUMN IF NOT EXISTS source_url TEXT;
ALTER TABLE zillow_leads ADD COLUMN IF NOT EXISTS scraped_at TIMESTAMP WITH TIME ZONE DEFAULT NOW();
ALTER TABLE zillow_leads ADD COLUMN IF NOT EXISTS property_url TEXT;
ALTER TABLE zillow_leads ADD COLUMN IF NOT EXISTS phone TEXT;
ALTER TABLE zillow_leads ADD COLUMN IF NOT EXISTS email TEXT;

ALTER TABLE redfin_leads ADD COLUMN IF NOT EXISTS address TEXT;
ALTER TABLE redfin_leads ADD COLUMN IF NOT EXISTS dfw BOOLEAN DEFAULT false;
ALTER TABLE redfin_leads ADD COLUMN IF NOT EXISTS source TEXT DEFAULT 'redfin';
ALTER TABLE redfin_leads ADD COLUMN IF NOT EXISTS source_url TEXT;
ALTER TABLE redfin_leads ADD COLUMN IF NOT EXISTS scraped_at TIMESTAMP WITH TIME ZONE DEFAULT NOW();
ALTER TABLE redfin_leads ADD COLUMN IF NOT EXISTS property_url TEXT;
ALTER TABLE redfin_leads ADD COLUMN IF NOT EXISTS phone TEXT;
ALTER TABLE redfin_leads ADD COLUMN IF NOT EXISTS email TEXT;

ALTER TABLE cad_leads ADD COLUMN IF NOT EXISTS address TEXT;
ALTER TABLE cad_leads ADD COLUMN IF NOT EXISTS dfw BOOLEAN DEFAULT false;
ALTER TABLE cad_leads ADD COLUMN IF NOT EXISTS source TEXT DEFAULT 'cad';
ALTER TABLE cad_leads ADD COLUMN IF NOT EXISTS source_url TEXT;
ALTER TABLE cad_leads ADD COLUMN IF NOT EXISTS scraped_at TIMESTAMP WITH TIME ZONE DEFAULT NOW();
ALTER TABLE cad_leads ADD COLUMN IF NOT EXISTS property_url TEXT;
ALTER TABLE cad_leads ADD COLUMN IF NOT EXISTS phone TEXT;
ALTER TABLE cad_leads ADD COLUMN IF NOT EXISTS email TEXT;

ALTER TABLE permit_leads ADD COLUMN IF NOT EXISTS address TEXT;
ALTER TABLE permit_leads ADD COLUMN IF NOT EXISTS dfw BOOLEAN DEFAULT false;
ALTER TABLE permit_leads ADD COLUMN IF NOT EXISTS source TEXT DEFAULT 'permit';
ALTER TABLE permit_leads ADD COLUMN IF NOT EXISTS source_url TEXT;
ALTER TABLE permit_leads ADD COLUMN IF NOT EXISTS scraped_at TIMESTAMP WITH TIME ZONE DEFAULT NOW();
ALTER TABLE permit_leads ADD COLUMN IF NOT EXISTS property_url TEXT;
ALTER TABLE permit_leads ADD COLUMN IF NOT EXISTS phone TEXT;
ALTER TABLE permit_leads ADD COLUMN IF NOT EXISTS email TEXT;

ALTER TABLE storm_events ADD COLUMN IF NOT EXISTS phone TEXT;
ALTER TABLE storm_events ADD COLUMN IF NOT EXISTS email TEXT;

-- =====================================================
-- COLUMN ALIASES - Copy data from address_text to address
-- =====================================================

-- Copy existing address_text data to new address column
UPDATE zillow_leads SET address = address_text WHERE address IS NULL AND address_text IS NOT NULL;
UPDATE redfin_leads SET address = address_text WHERE address IS NULL AND address_text IS NOT NULL;
UPDATE cad_leads SET address = address_text WHERE address IS NULL AND address_text IS NOT NULL;
UPDATE permit_leads SET address = address_text WHERE address IS NULL AND address_text IS NOT NULL;

-- =====================================================
-- ADD MISSING SCRAPER-SPECIFIC COLUMNS
-- =====================================================

-- Zillow-specific columns
ALTER TABLE zillow_leads ADD COLUMN IF NOT EXISTS bedrooms INTEGER;
ALTER TABLE zillow_leads ADD COLUMN IF NOT EXISTS bathrooms DECIMAL;
ALTER TABLE zillow_leads ADD COLUMN IF NOT EXISTS zip_code TEXT;

-- Copy existing data to new column names
UPDATE zillow_leads SET bedrooms = num_bedrooms WHERE bedrooms IS NULL AND num_bedrooms IS NOT NULL;
UPDATE zillow_leads SET bathrooms = num_bathrooms WHERE bathrooms IS NULL AND num_bathrooms IS NOT NULL;

-- Redfin-specific columns
ALTER TABLE redfin_leads ADD COLUMN IF NOT EXISTS bedrooms INTEGER;
ALTER TABLE redfin_leads ADD COLUMN IF NOT EXISTS bathrooms DECIMAL;

-- Copy existing data to new column names
UPDATE redfin_leads SET bedrooms = num_bedrooms WHERE bedrooms IS NULL AND num_bedrooms IS NOT NULL;
UPDATE redfin_leads SET bathrooms = num_bathrooms WHERE bathrooms IS NULL AND num_bathrooms IS NOT NULL;

-- =====================================================
-- PERFORMANCE INDEXES FOR NEW COLUMNS
-- =====================================================

-- DFW filtering indexes
CREATE INDEX IF NOT EXISTS idx_zillow_dfw ON zillow_leads(dfw) WHERE dfw = true;
CREATE INDEX IF NOT EXISTS idx_redfin_dfw ON redfin_leads(dfw) WHERE dfw = true;
CREATE INDEX IF NOT EXISTS idx_cad_dfw ON cad_leads(dfw) WHERE dfw = true;
CREATE INDEX IF NOT EXISTS idx_permit_dfw ON permit_leads(dfw) WHERE dfw = true;

-- Address indexes for deduplication
CREATE INDEX IF NOT EXISTS idx_zillow_address ON zillow_leads(address);
CREATE INDEX IF NOT EXISTS idx_redfin_address ON redfin_leads(address);
CREATE INDEX IF NOT EXISTS idx_cad_address ON cad_leads(address);
CREATE INDEX IF NOT EXISTS idx_permit_address ON permit_leads(address);

-- Contact field indexes
CREATE INDEX IF NOT EXISTS idx_zillow_phone ON zillow_leads(phone) WHERE phone IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_zillow_email ON zillow_leads(email) WHERE email IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_redfin_phone ON redfin_leads(phone) WHERE phone IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_redfin_email ON redfin_leads(email) WHERE email IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_cad_phone ON cad_leads(phone) WHERE phone IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_cad_email ON cad_leads(email) WHERE email IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_permit_phone ON permit_leads(phone) WHERE phone IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_permit_email ON permit_leads(email) WHERE email IS NOT NULL;

-- Source and scraping indexes
CREATE INDEX IF NOT EXISTS idx_zillow_source ON zillow_leads(source);
CREATE INDEX IF NOT EXISTS idx_redfin_source ON redfin_leads(source);
CREATE INDEX IF NOT EXISTS idx_cad_source ON cad_leads(source);
CREATE INDEX IF NOT EXISTS idx_permit_source ON permit_leads(source);

CREATE INDEX IF NOT EXISTS idx_zillow_scraped_at ON zillow_leads(scraped_at);
CREATE INDEX IF NOT EXISTS idx_redfin_scraped_at ON redfin_leads(scraped_at);
CREATE INDEX IF NOT EXISTS idx_cad_scraped_at ON cad_leads(scraped_at);
CREATE INDEX IF NOT EXISTS idx_permit_scraped_at ON permit_leads(scraped_at);

-- =====================================================
-- DEDUPLICATION CONSTRAINTS (OPTIONAL)
-- =====================================================

-- Add unique constraints for better deduplication
-- Uncomment if you want database-level deduplication
-- ALTER TABLE zillow_leads ADD CONSTRAINT unique_zillow_address_zip UNIQUE (address, zip_code);
-- ALTER TABLE redfin_leads ADD CONSTRAINT unique_redfin_address_zip UNIQUE (address, zip_code);
-- ALTER TABLE cad_leads ADD CONSTRAINT unique_cad_address_zip UNIQUE (address, zip_code);
-- ALTER TABLE permit_leads ADD CONSTRAINT unique_permit_address_zip UNIQUE (address, zip_code);

-- =====================================================
-- COLUMN COMMENTS FOR DOCUMENTATION
-- =====================================================

COMMENT ON COLUMN zillow_leads.dfw IS 'DFW area filter flag (true for Dallas-Fort Worth properties)';
COMMENT ON COLUMN zillow_leads.phone IS 'Contact phone number extracted from property listing or agent info';
COMMENT ON COLUMN zillow_leads.email IS 'Contact email extracted from property listing or agent info';
COMMENT ON COLUMN zillow_leads.address IS 'Property address (primary field used by scrapers)';
COMMENT ON COLUMN zillow_leads.source IS 'Data source identifier (zillow)';
COMMENT ON COLUMN zillow_leads.scraped_at IS 'Timestamp when data was scraped';

COMMENT ON COLUMN redfin_leads.dfw IS 'DFW area filter flag (true for Dallas-Fort Worth properties)';
COMMENT ON COLUMN redfin_leads.phone IS 'Contact phone number extracted from property listing or agent info';
COMMENT ON COLUMN redfin_leads.email IS 'Contact email extracted from property listing or agent info';
COMMENT ON COLUMN redfin_leads.address IS 'Property address (primary field used by scrapers)';
COMMENT ON COLUMN redfin_leads.source IS 'Data source identifier (redfin)';
COMMENT ON COLUMN redfin_leads.scraped_at IS 'Timestamp when data was scraped';

COMMENT ON COLUMN cad_leads.dfw IS 'DFW area filter flag (true for Dallas-Fort Worth properties)';
COMMENT ON COLUMN cad_leads.phone IS 'Property owner phone number from public records';
COMMENT ON COLUMN cad_leads.email IS 'Property owner email from public records';
COMMENT ON COLUMN cad_leads.address IS 'Property address (primary field used by scrapers)';
COMMENT ON COLUMN cad_leads.source IS 'Data source identifier (cad)';
COMMENT ON COLUMN cad_leads.scraped_at IS 'Timestamp when data was scraped';

COMMENT ON COLUMN permit_leads.dfw IS 'DFW area filter flag (true for Dallas-Fort Worth properties)';
COMMENT ON COLUMN permit_leads.phone IS 'Permit applicant or contractor phone number';
COMMENT ON COLUMN permit_leads.email IS 'Permit applicant or contractor email';
COMMENT ON COLUMN permit_leads.address IS 'Property address (primary field used by scrapers)';
COMMENT ON COLUMN permit_leads.source IS 'Data source identifier (permit)';
COMMENT ON COLUMN permit_leads.scraped_at IS 'Timestamp when data was scraped';

COMMENT ON COLUMN storm_events.phone IS 'Contact phone for storm-affected properties';
COMMENT ON COLUMN storm_events.email IS 'Contact email for storm-affected properties';

-- =====================================================
-- VALIDATION QUERIES
-- =====================================================

-- Run these to verify the schema fix worked:
-- SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'zillow_leads' ORDER BY column_name;
-- SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'redfin_leads' ORDER BY column_name;
-- SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'cad_leads' ORDER BY column_name;
-- SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'permit_leads' ORDER BY column_name;

-- Test data insertion:
-- INSERT INTO zillow_leads (address, city, state, zip_code, dfw, phone, email, source, scraped_at) 
-- VALUES ('123 Test St', 'Dallas', 'TX', '75201', true, '+1-555-123-4567', 'test@example.com', 'zillow', NOW());

-- =====================================================
-- SCHEMA FIX COMPLETE
-- =====================================================

-- Summary of changes:
-- ✅ Added missing 'address' column to all tables
-- ✅ Added missing 'dfw' boolean flag
-- ✅ Added missing 'source', 'source_url', 'scraped_at' columns
-- ✅ Added missing 'property_url' column
-- ✅ Added 'phone' and 'email' contact fields
-- ✅ Added alternative column names (bedrooms, bathrooms, zip_code)
-- ✅ Created performance indexes for all new columns
-- ✅ Added column documentation
-- ✅ Copied existing data from address_text to address
-- ✅ Ready for Apify actor integration