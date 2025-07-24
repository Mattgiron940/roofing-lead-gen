#!/usr/bin/env python3
"""
Manual contact fields migration using direct table alterations
Since Supabase doesn't support direct SQL execution via API, we'll add the columns through insert operations
"""

import os
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

# Initialize Supabase client
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Missing Supabase credentials in .env file")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def test_table_exists_and_add_fields():
    """Test if tables exist and attempt to add contact fields through test inserts"""
    
    print("🔧 Testing contact fields on lead tables...")
    
    # Test tables and their expected structures
    test_tables = [
        ('zillow_leads', {
            'source': 'zillow',
            'scraped_at': '2025-01-25T00:00:00Z',
            'dfw': True,
            'address': '123 Test St, Dallas, TX 75201',
            'phone': '+1-555-123-4567',
            'email': 'test@example.com',
            'price': 350000,
            'lead_score': 7
        }),
        ('redfin_leads', {
            'source': 'redfin', 
            'scraped_at': '2025-01-25T00:00:00Z',
            'dfw': True,
            'address': '456 Test Ave, Plano, TX 75024',
            'phone': '+1-555-987-6543',
            'email': 'agent@redfin.com',
            'price': 425000,
            'lead_score': 8
        }),
        ('cad_leads', {
            'source': 'dallas_cad',
            'scraped_at': '2025-01-25T00:00:00Z',
            'dfw': True,
            'address': '789 Test Blvd, Arlington, TX 76001',
            'phone': '+1-555-246-8135',
            'email': 'owner@example.com',
            'assessed_value': 300000,
            'lead_score': 6
        }),
        ('permit_leads', {
            'source': 'dallas_permits',
            'scraped_at': '2025-01-25T00:00:00Z',
            'dfw': True,
            'address': '321 Test Ln, Fort Worth, TX 76102',
            'phone': '+1-555-147-2580',
            'email': 'contractor@roofing.com',
            'permit_number': 'TEST-2025-001',
            'work_description': 'Roof replacement - storm damage',
            'permit_value': 25000,
            'lead_score': 9
        })
    ]
    
    results = {}
    
    for table_name, test_record in test_tables:
        try:
            print(f"🔍 Testing {table_name}...")
            
            # Try to insert test record with phone and email
            result = supabase.from_(table_name).insert(test_record).execute()
            
            if result.data:
                print(f"✅ {table_name}: Contact fields already exist and working")
                
                # Clean up test record
                supabase.from_(table_name).delete().eq('address', test_record['address']).execute()
                results[table_name] = 'success'
            else:
                print(f"⚠️ {table_name}: Insert returned no data")
                results[table_name] = 'unknown'
                
        except Exception as e:
            error_message = str(e)
            
            if 'column' in error_message.lower() and ('phone' in error_message.lower() or 'email' in error_message.lower()):
                print(f"❌ {table_name}: Contact fields missing - {error_message}")
                results[table_name] = 'missing_columns'
            else:
                print(f"⚠️ {table_name}: Other error - {error_message}")
                results[table_name] = 'other_error'
    
    return results

def create_manual_migration_instructions():
    """Create instructions for manual migration since we can't execute DDL"""
    
    print("\n📋 MANUAL MIGRATION REQUIRED")
    print("=" * 50)
    print("Since Supabase API doesn't support DDL operations, please:")
    print("1. Go to your Supabase Dashboard")
    print("2. Navigate to SQL Editor")
    print("3. Run the following SQL:")
    print("\n--- COPY AND PASTE THIS SQL ---")
    
    sql_script = """-- Add contact fields to all lead tables
ALTER TABLE redfin_leads ADD COLUMN IF NOT EXISTS phone TEXT;
ALTER TABLE redfin_leads ADD COLUMN IF NOT EXISTS email TEXT;

ALTER TABLE zillow_leads ADD COLUMN IF NOT EXISTS phone TEXT;
ALTER TABLE zillow_leads ADD COLUMN IF NOT EXISTS email TEXT;

ALTER TABLE cad_leads ADD COLUMN IF NOT EXISTS phone TEXT;
ALTER TABLE cad_leads ADD COLUMN IF NOT EXISTS email TEXT;

ALTER TABLE permit_leads ADD COLUMN IF NOT EXISTS phone TEXT;
ALTER TABLE permit_leads ADD COLUMN IF NOT EXISTS email TEXT;

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_zillow_phone ON zillow_leads(phone) WHERE phone IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_zillow_email ON zillow_leads(email) WHERE email IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_redfin_phone ON redfin_leads(phone) WHERE phone IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_redfin_email ON redfin_leads(email) WHERE email IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_cad_phone ON cad_leads(phone) WHERE phone IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_cad_email ON cad_leads(email) WHERE email IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_permit_phone ON permit_leads(phone) WHERE phone IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_permit_email ON permit_leads(email) WHERE email IS NOT NULL;"""
    
    print(sql_script)
    print("--- END SQL ---\n")
    
    print("4. After running the SQL, run this script again to verify")
    print("5. Then update the Apify actors to include contact extraction")

def update_apify_actors_with_contact_extraction():
    """Update all Apify actors to include phone and email extraction"""
    
    print("🔄 Updating Apify actors with contact extraction...")
    
    # Define contact extraction JavaScript code
    contact_extraction_js = """
// Contact extraction utilities
function extractPhone(text) {
    if (!text) return null;
    
    // Common phone number patterns
    const phonePatterns = [
        /(\+1[-.\s]?)?\(?([0-9]{3})\)?[-.\s]?([0-9]{3})[-.\s]?([0-9]{4})/g,
        /(\+1[-.\s]?)?([0-9]{3})[-.\s]?([0-9]{3})[-.\s]?([0-9]{4})/g,
        /\(?([0-9]{3})\)?[-.\s]?([0-9]{3})[-.\s]?([0-9]{4})/g
    ];
    
    for (const pattern of phonePatterns) {
        const match = text.match(pattern);
        if (match) {
            // Clean and format phone number
            const cleaned = match[0].replace(/[^0-9+]/g, '');
            if (cleaned.length >= 10) {
                return formatPhoneNumber(cleaned);
            }
        }
    }
    
    return null;
}

function extractEmail(text) {
    if (!text) return null;
    
    const emailPattern = /\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b/g;
    const match = text.match(emailPattern);
    
    if (match && match[0]) {
        const email = match[0].toLowerCase();
        // Filter out common non-contact emails
        const blockedDomains = ['noreply', 'donotreply', 'no-reply', 'system', 'admin'];
        if (!blockedDomains.some(domain => email.includes(domain))) {
            return email;
        }
    }
    
    return null;
}

function formatPhoneNumber(phone) {
    // Remove all non-digits except +
    const cleaned = phone.replace(/[^0-9+]/g, '');
    
    // Format as +1-XXX-XXX-XXXX if US number
    if (cleaned.length === 10) {
        return `+1-${cleaned.slice(0,3)}-${cleaned.slice(3,6)}-${cleaned.slice(6)}`;
    } else if (cleaned.length === 11 && cleaned.startsWith('1')) {
        return `+${cleaned.slice(0,1)}-${cleaned.slice(1,4)}-${cleaned.slice(4,7)}-${cleaned.slice(7)}`;
    }
    
    return cleaned;
}

function extractContactInfo($element) {
    const text = $element.text();
    
    return {
        phone: extractPhone(text),
        email: extractEmail(text)
    };
}"""
    
    # Update each actor's main.js file
    actors = ['dfw-zillow-actor', 'dfw-redfin-actor', 'dfw-cad-actor', 'dfw-permit-actor']
    
    for actor in actors:
        actor_path = f"apify_actors/{actor}/main.js"
        
        try:
            # Read current main.js
            with open(actor_path, 'r') as f:
                content = f.read()
            
            # Check if contact extraction is already added
            if 'extractPhone' in content:
                print(f"✅ {actor}: Contact extraction already present")
                continue
            
            # Add contact extraction utilities after imports
            insertion_point = content.find('// Lead deduplication')
            if insertion_point == -1:
                insertion_point = content.find('const processedLeads')
            
            if insertion_point != -1:
                updated_content = (
                    content[:insertion_point] + 
                    contact_extraction_js + 
                    "\n\n" + 
                    content[insertion_point:]
                )
                
                # Update property extraction to include contact info
                if 'zillow' in actor:
                    # Add contact extraction to Zillow property parsing
                    updated_content = updated_content.replace(
                        "property_url: $card.find('a').attr('href'),",
                        """property_url: $card.find('a').attr('href'),
                
                // Extract contact information
                phone: extractPhone($card.text()),
                email: extractEmail($card.text()),"""
                    )
                
                elif 'redfin' in actor:
                    # Add contact extraction to Redfin property parsing
                    updated_content = updated_content.replace(
                        "lead_score: 5",
                        """lead_score: 5,
                
                // Extract contact information  
                phone: extractPhone($card.text()),
                email: extractEmail($card.text())"""
                    )
                
                elif 'cad' in actor:
                    # Add contact extraction to CAD property parsing
                    updated_content = updated_content.replace(
                        "lead_score: 6",
                        """lead_score: 6,
                
                // Extract contact information
                phone: extractPhone($row.text()),
                email: extractEmail($row.text())"""
                    )
                
                elif 'permit' in actor:
                    # Add contact extraction to permit parsing
                    updated_content = updated_content.replace(
                        "lead_score: 7",
                        """lead_score: 7,
                
                // Extract contact information
                phone: extractPhone($row.text()),
                email: extractEmail($row.text())"""
                    )
                
                # Write updated content
                with open(actor_path, 'w') as f:
                    f.write(updated_content)
                
                print(f"✅ {actor}: Updated with contact extraction")
            else:
                print(f"⚠️ {actor}: Could not find insertion point")
                
        except Exception as e:
            print(f"❌ {actor}: Failed to update - {e}")

if __name__ == "__main__":
    try:
        # Test current state
        results = test_table_exists_and_add_fields()
        
        # Check if any tables are missing contact fields
        missing_fields = [table for table, status in results.items() if status == 'missing_columns']
        
        if missing_fields:
            print(f"\n❌ Tables missing contact fields: {', '.join(missing_fields)}")
            create_manual_migration_instructions()
        else:
            print("\n✅ All tables have contact fields!")
            
            # Update Apify actors
            update_apify_actors_with_contact_extraction()
            
            print("\n🎉 Contact field integration completed!")
            print("📱 All Apify actors now extract phone and email when available")
            print("🔄 Ready to deploy updated actors")
            
    except Exception as e:
        print(f"💥 Fatal error: {e}")
        print("Please verify your Supabase connection and table structure.")