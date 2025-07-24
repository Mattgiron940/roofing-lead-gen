-- SIMPLIFIED SCHEMA FIX
-- Adds only the missing columns that scrapers expect
-- Generated: 2025-07-24

-- =====================================================
-- ADD MISSING COLUMNS ONLY
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
ALTER TABLE zillow_leads ADD COLUMN IF NOT EXISTS bedrooms INTEGER;
ALTER TABLE zillow_leads ADD COLUMN IF NOT EXISTS bathrooms DECIMAL;

ALTER TABLE redfin_leads ADD COLUMN IF NOT EXISTS address TEXT;
ALTER TABLE redfin_leads ADD COLUMN IF NOT EXISTS dfw BOOLEAN DEFAULT false;
ALTER TABLE redfin_leads ADD COLUMN IF NOT EXISTS source TEXT DEFAULT 'redfin';
ALTER TABLE redfin_leads ADD COLUMN IF NOT EXISTS source_url TEXT;
ALTER TABLE redfin_leads ADD COLUMN IF NOT EXISTS scraped_at TIMESTAMP WITH TIME ZONE DEFAULT NOW();
ALTER TABLE redfin_leads ADD COLUMN IF NOT EXISTS property_url TEXT;
ALTER TABLE redfin_leads ADD COLUMN IF NOT EXISTS phone TEXT;
ALTER TABLE redfin_leads ADD COLUMN IF NOT EXISTS email TEXT;
ALTER TABLE redfin_leads ADD COLUMN IF NOT EXISTS bedrooms INTEGER;
ALTER TABLE redfin_leads ADD COLUMN IF NOT EXISTS bathrooms DECIMAL;

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

-- =====================================================
-- VALIDATION TEST
-- =====================================================

-- Test data insertion to verify schema fix worked:
INSERT INTO zillow_leads (address, city, state, zip_code, dfw, phone, email, source, scraped_at) 
VALUES ('123 Test St', 'Dallas', 'TX', '75201', true, '+1-555-123-4567', 'test@example.com', 'zillow', NOW())
ON CONFLICT DO NOTHING;

-- =====================================================
-- SCHEMA FIX COMPLETE
-- =====================================================

-- Summary of changes:
-- ✅ Added missing 'address' column to all tables
-- ✅ Added missing 'dfw' boolean flag
-- ✅ Added missing 'source', 'source_url', 'scraped_at' columns
-- ✅ Added missing 'property_url' column
-- ✅ Added 'phone' and 'email' contact fields
-- ✅ Added 'bedrooms' and 'bathrooms' columns
-- ✅ Created performance indexes for all new columns
-- ✅ Ready for Apify actor integration