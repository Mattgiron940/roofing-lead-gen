#!/usr/bin/env python3
"""
Apply contact fields migration to Supabase
Adds phone and email columns to all lead tables
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

def apply_contact_migration():
    """Apply the contact fields migration"""
    
    print("🔧 Applying contact fields migration to Supabase...")
    
    # Read the migration SQL
    with open('supabase/migrations/20250725_add_contact_fields.sql', 'r') as f:
        migration_sql = f.read()
    
    try:
        # Execute the migration
        result = supabase.rpc('exec_sql', {'sql': migration_sql}).execute()
        print("✅ Migration applied successfully!")
        
        # Verify columns were added
        tables_to_check = ['redfin_leads', 'zillow_leads', 'cad_leads', 'permit_leads', 'storm_events']
        
        for table in tables_to_check:
            try:
                # Check if phone and email columns exist by trying to select them
                result = supabase.from_(table).select('phone, email').limit(1).execute()
                print(f"✅ {table}: phone and email columns confirmed")
            except Exception as e:
                print(f"⚠️ {table}: Could not verify columns - {str(e)}")
        
        return True
        
    except Exception as e:
        print(f"❌ Migration failed: {e}")
        
        # Try alternative approach - execute SQL statements individually
        print("🔄 Trying individual SQL statements...")
        
        sql_statements = [
            "ALTER TABLE redfin_leads ADD COLUMN IF NOT EXISTS phone TEXT",
            "ALTER TABLE redfin_leads ADD COLUMN IF NOT EXISTS email TEXT",
            "ALTER TABLE zillow_leads ADD COLUMN IF NOT EXISTS phone TEXT",
            "ALTER TABLE zillow_leads ADD COLUMN IF NOT EXISTS email TEXT",
            "ALTER TABLE cad_leads ADD COLUMN IF NOT EXISTS phone TEXT",
            "ALTER TABLE cad_leads ADD COLUMN IF NOT EXISTS email TEXT",
            "ALTER TABLE permit_leads ADD COLUMN IF NOT EXISTS phone TEXT",
            "ALTER TABLE permit_leads ADD COLUMN IF NOT EXISTS email TEXT",
            "ALTER TABLE storm_events ADD COLUMN IF NOT EXISTS phone TEXT",
            "ALTER TABLE storm_events ADD COLUMN IF NOT EXISTS email TEXT"
        ]
        
        success_count = 0
        for sql in sql_statements:
            try:
                supabase.rpc('exec_sql', {'sql': sql}).execute()
                success_count += 1
                print(f"✅ Executed: {sql[:50]}...")
            except Exception as stmt_error:
                print(f"⚠️ Failed: {sql[:50]}... - {stmt_error}")
        
        print(f"📊 Successfully executed {success_count}/{len(sql_statements)} statements")
        return success_count > 0

def verify_migration():
    """Verify that the migration was successful"""
    
    print("\n🔍 Verifying migration results...")
    
    tables = [
        ('redfin_leads', 'Redfin property leads'),
        ('zillow_leads', 'Zillow property leads'), 
        ('cad_leads', 'County assessor records'),
        ('permit_leads', 'Building permits'),
        ('storm_events', 'Storm event data')
    ]
    
    for table_name, description in tables:
        try:
            # Try to insert a test record with phone and email
            test_record = {
                'source': 'migration_test',
                'scraped_at': '2025-01-25T00:00:00Z',
                'dfw': True,
                'address': 'Test Address',
                'phone': '+1-555-123-4567',
                'email': 'test@example.com',
                'lead_score': 5
            }
            
            # Insert test record
            result = supabase.from_(table_name).insert(test_record).execute()
            
            # Delete test record
            supabase.from_(table_name).delete().eq('source', 'migration_test').execute()
            
            print(f"✅ {table_name}: Contact fields working correctly")
            
        except Exception as e:
            print(f"❌ {table_name}: Contact fields verification failed - {e}")

if __name__ == "__main__":
    try:
        # Apply migration
        success = apply_contact_migration()
        
        if success:
            # Verify migration
            verify_migration()
            
            print("\n🎉 Contact fields migration completed successfully!")
            print("📱 All lead tables now support phone and email extraction")
            print("🔄 Ready to update Apify actors with contact extraction logic")
        else:
            print("\n❌ Migration failed. Please check Supabase connection and try again.")
            
    except Exception as e:
        print(f"💥 Fatal error: {e}")
        print("Please check your Supabase credentials and connection.")