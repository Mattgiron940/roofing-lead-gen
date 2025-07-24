-- Add phone and email columns to all lead tables
-- Migration: Add contact fields for enhanced lead generation

-- Add contact fields to redfin_leads table
ALTER TABLE redfin_leads ADD COLUMN IF NOT EXISTS phone TEXT;
ALTER TABLE redfin_leads ADD COLUMN IF NOT EXISTS email TEXT;

-- Add contact fields to zillow_leads table  
ALTER TABLE zillow_leads ADD COLUMN IF NOT EXISTS phone TEXT;
ALTER TABLE zillow_leads ADD COLUMN IF NOT EXISTS email TEXT;

-- Add contact fields to cad_leads table
ALTER TABLE cad_leads ADD COLUMN IF NOT EXISTS phone TEXT;
ALTER TABLE cad_leads ADD COLUMN IF NOT EXISTS email TEXT;

-- Add contact fields to permit_leads table
ALTER TABLE permit_leads ADD COLUMN IF NOT EXISTS phone TEXT;
ALTER TABLE permit_leads ADD COLUMN IF NOT EXISTS email TEXT;

-- Add contact fields to storm_events table (if exists)
ALTER TABLE storm_events ADD COLUMN IF NOT EXISTS phone TEXT;
ALTER TABLE storm_events ADD COLUMN IF NOT EXISTS email TEXT;

-- Create indexes for better query performance on contact fields
CREATE INDEX IF NOT EXISTS idx_redfin_leads_phone ON redfin_leads(phone) WHERE phone IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_redfin_leads_email ON redfin_leads(email) WHERE email IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_zillow_leads_phone ON zillow_leads(phone) WHERE phone IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_zillow_leads_email ON zillow_leads(email) WHERE email IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_cad_leads_phone ON cad_leads(phone) WHERE phone IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_cad_leads_email ON cad_leads(email) WHERE email IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_permit_leads_phone ON permit_leads(phone) WHERE phone IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_permit_leads_email ON permit_leads(email) WHERE email IS NOT NULL;

-- Add comment for documentation
COMMENT ON COLUMN redfin_leads.phone IS 'Contact phone number extracted from property listing or agent info';
COMMENT ON COLUMN redfin_leads.email IS 'Contact email extracted from property listing or agent info';

COMMENT ON COLUMN zillow_leads.phone IS 'Contact phone number extracted from property listing or agent info';
COMMENT ON COLUMN zillow_leads.email IS 'Contact email extracted from property listing or agent info';

COMMENT ON COLUMN cad_leads.phone IS 'Property owner phone number from public records';
COMMENT ON COLUMN cad_leads.email IS 'Property owner email from public records';

COMMENT ON COLUMN permit_leads.phone IS 'Permit applicant or contractor phone number';
COMMENT ON COLUMN permit_leads.email IS 'Permit applicant or contractor email';

COMMENT ON COLUMN storm_events.phone IS 'Contact phone for storm-affected properties';
COMMENT ON COLUMN storm_events.email IS 'Contact email for storm-affected properties';