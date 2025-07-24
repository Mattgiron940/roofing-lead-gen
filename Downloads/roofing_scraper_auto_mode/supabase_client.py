#!/usr/bin/env python3
"""
Unified Supabase Client for Lead Generation System
Centralized database operations with proper error handling and deduplication
"""

import os
import logging
from typing import Optional, Dict, Any, List
from datetime import datetime
import hashlib
from supabase import create_client, Client
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)

class UnifiedSupabaseClient:
    """Unified Supabase client with advanced features for lead generation"""
    
    def __init__(self):
        self.supabase: Optional[Client] = None
        self.initialize_connection()
        
        # Deduplication cache to track inserted records
        self.inserted_hashes = set()
    
    def initialize_connection(self) -> bool:
        """Initialize Supabase client with environment variables"""
        try:
            # Try environment variables first
            url = os.getenv("SUPABASE_URL")
            key = os.getenv("SUPABASE_KEY")
            
            if not url or not key:
                # Fallback to Desktop .env file
                desktop_env = os.path.expanduser("~/Desktop/.env")
                if os.path.exists(desktop_env):
                    with open(desktop_env, 'r') as f:
                        for line in f:
                            line = line.strip()
                            if line and not line.startswith('#') and '=' in line:
                                env_key, env_value = line.split('=', 1)
                                os.environ[env_key] = env_value
                    
                    # Try again after loading Desktop .env
                    url = os.getenv("SUPABASE_URL")
                    key = os.getenv("SUPABASE_KEY")
            
            if not url or not key:
                logger.warning("SUPABASE_URL or SUPABASE_KEY not found - using hardcoded credentials")
                # Use the known working credentials
                url = "https://rupqnhgtzfynvzgxkgch.supabase.co"
                key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJ1cHFuaGd0emZ5bnZ6Z3hrZ2NoIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1MzMwNzU3MSwiZXhwIjoyMDY4ODgzNTcxfQ.XbaTq75Qqo_bbDuqf9DDq5rmwwtyEYUHze4BxmV7Wpw"
            
            self.supabase = create_client(url, key)
            logger.info("✅ Supabase client initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize Supabase client: {e}")
            self.supabase = None
            return False
    
    def generate_lead_hash(self, lead_data: Dict[str, Any], table_name: str) -> str:
        """Generate unique hash for lead deduplication"""
        # Create hash based on key identifying fields per table
        hash_fields = []
        
        if table_name == "zillow_leads":
            hash_fields = [
                lead_data.get('zpid', ''),
                lead_data.get('address_text', ''),
                lead_data.get('price', 0)
            ]
        elif table_name == "redfin_leads":
            hash_fields = [
                lead_data.get('mls_number', ''),
                lead_data.get('address_text', ''),
                lead_data.get('price', 0)
            ]
        elif table_name == "cad_leads":
            hash_fields = [
                lead_data.get('account_number', ''),
                lead_data.get('address_text', ''),
                lead_data.get('appraised_value', 0)
            ]
        elif table_name == "permit_leads":
            hash_fields = [
                lead_data.get('permit_id', ''),
                lead_data.get('address_text', ''),
                lead_data.get('date_filed', '')
            ]
        elif table_name == "storm_events":
            hash_fields = [
                lead_data.get('event_id', ''),
                lead_data.get('event_date', ''),
                lead_data.get('event_type', '')
            ]
        else:
            # Fallback - use address and a few other fields
            hash_fields = [
                lead_data.get('address_text', ''),
                lead_data.get('city', ''),
                lead_data.get('zip_code', '')
            ]
        
        # Create hash from combined fields
        hash_string = '|'.join(str(field) for field in hash_fields)
        return hashlib.md5(hash_string.encode()).hexdigest()
    
    def calculate_universal_lead_score(self, lead_data: Dict[str, Any], table_name: str) -> int:
        """Calculate consistent lead score across all sources using 7-factor algorithm"""
        score = 5  # Base score
        
        # Factor 1: Property Value (20% weight)
        property_value = 0
        if table_name in ["zillow_leads", "redfin_leads"]:
            property_value = lead_data.get('price', 0) or 0
        elif table_name == "cad_leads":
            property_value = lead_data.get('appraised_value', 0) or 0
        
        if property_value > 800000:
            score += 2.0
        elif property_value > 500000:
            score += 1.5
        elif property_value > 300000:
            score += 1.0
        elif property_value > 200000:
            score += 0.5
        
        # Factor 2: Property Age (15% weight)
        year_built = lead_data.get('year_built')
        if year_built:
            current_year = datetime.now().year
            age = current_year - year_built
            if 10 <= age <= 30:  # Sweet spot for roof replacement
                score += 1.5
            elif 5 <= age <= 40:
                score += 1.0
            elif age > 40:
                score += 0.5
        
        # Factor 3: Location Score (15% weight)
        city = lead_data.get('city', '').lower()
        zip_code = lead_data.get('zip_code', '')
        
        # High-value areas in Texas
        premium_cities = ['plano', 'frisco', 'allen', 'southlake', 'westlake', 'highland park']
        good_cities = ['dallas', 'fort worth', 'arlington', 'irving', 'garland', 'mesquite']
        
        if any(premium in city for premium in premium_cities):
            score += 1.5
        elif any(good in city for good in good_cities):
            score += 1.0
        
        # Premium zip codes
        premium_zips = ['75024', '75034', '75035', '75209', '75225', '76092']
        if zip_code in premium_zips:
            score += 0.5
        
        # Factor 4: Data Source Quality (15% weight)
        source_scores = {
            "zillow_leads": 1.5,
            "redfin_leads": 1.2,
            "cad_leads": 1.0,
            "permit_leads": 0.8,
            "storm_events": 0.5
        }
        score += source_scores.get(table_name, 0.5)
        
        # Factor 5: Storm Impact (10% weight)
        if lead_data.get('storm_affected', False):
            score += 1.0
        
        # Factor 6: Permit Activity (10% weight) 
        if table_name == "permit_leads":
            permit_type = lead_data.get('permit_type', '').lower()
            if any(keyword in permit_type for keyword in ['roof', 'repair', 'construction']):
                score += 1.0
        
        # Factor 7: Urgency Signals (5% weight)
        if table_name in ["zillow_leads", "redfin_leads"]:
            days_on_market = lead_data.get(f'days_on_{table_name.split("_")[0]}', 0) or 0
            if days_on_market > 90:  # Long time on market = motivated seller
                score += 0.5
        
        # Normalize to 1-10 scale
        return max(1, min(10, round(score)))
    
    def safe_insert(self, table_name: str, lead_data: Dict[str, Any]) -> bool:
        """Safely insert lead data with deduplication and error handling"""
        if not self.supabase:
            logger.warning(f"⚠️ Supabase not available, skipping insert to {table_name}")
            return False
        
        try:
            # Generate lead hash for deduplication
            lead_hash = self.generate_lead_hash(lead_data, table_name)
            
            # Check if already inserted in this session
            if lead_hash in self.inserted_hashes:
                logger.debug(f"🔄 Duplicate lead skipped: {lead_data.get('address_text', 'Unknown')}")
                return False
            
            # Clean data - remove None values and empty strings
            clean_data = {}
            for key, value in lead_data.items():
                if value is not None and value != '':
                    clean_data[key] = value
            
            # Add universal lead score if not present
            if 'lead_score' not in clean_data:
                clean_data['lead_score'] = self.calculate_universal_lead_score(clean_data, table_name)
            
            # Add timestamps
            now = datetime.now().isoformat()
            clean_data['created_at'] = now
            clean_data['updated_at'] = now
            
            # Insert into Supabase
            result = self.supabase.table(table_name).insert(clean_data).execute()
            
            if result.data:
                # Mark as inserted
                self.inserted_hashes.add(lead_hash)
                
                address = clean_data.get('address_text', 'Unknown')
                score = clean_data.get('lead_score', 0)
                logger.info(f"✅ Inserted into {table_name}: {address} (Score: {score})")
                return True
            else:
                logger.warning(f"⚠️ No data returned from {table_name} insert")
                return False
                
        except Exception as e:
            error_msg = str(e)
            
            # Handle common errors gracefully
            if "duplicate key" in error_msg.lower():
                logger.debug(f"🔄 Duplicate key in {table_name} - skipping")
                return False
            elif "relation does not exist" in error_msg.lower():
                logger.error(f"❌ Table {table_name} does not exist - run schema deployment")
                return False
            else:
                logger.error(f"❌ Failed to insert into {table_name}: {error_msg}")
                return False
    
    def bulk_insert(self, table_name: str, leads_data: List[Dict[str, Any]]) -> int:
        """Insert multiple leads with batch processing"""
        if not leads_data:
            return 0
        
        successful_inserts = 0
        
        # Process in batches of 50 to avoid API limits
        batch_size = 50
        for i in range(0, len(leads_data), batch_size):
            batch = leads_data[i:i + batch_size]
            
            for lead_data in batch:
                if self.safe_insert(table_name, lead_data):
                    successful_inserts += 1
        
        logger.info(f"📊 Bulk insert completed: {successful_inserts}/{len(leads_data)} leads inserted into {table_name}")
        return successful_inserts
    
    def check_table_exists(self, table_name: str) -> bool:
        """Check if a table exists and is accessible"""
        if not self.supabase:
            return False
        
        try:
            result = self.supabase.table(table_name).select('count').limit(1).execute()
            return True
        except Exception as e:
            if "relation does not exist" in str(e).lower():
                return False
            return True  # Table exists but might be empty
    
    def get_table_count(self, table_name: str) -> int:
        """Get total count of records in a table"""
        if not self.supabase:
            return 0
        
        try:
            result = self.supabase.table(table_name).select('id', count='exact').execute()
            return result.count if hasattr(result, 'count') else len(result.data or [])
        except Exception as e:
            logger.error(f"❌ Failed to get count for {table_name}: {e}")
            return 0
    
    def get_recent_leads(self, table_name: str, hours: int = 24) -> List[Dict[str, Any]]:
        """Get recent leads from a table"""
        if not self.supabase:
            return []
        
        try:
            from datetime import timedelta
            cutoff_time = (datetime.now() - timedelta(hours=hours)).isoformat()
            
            result = self.supabase.table(table_name)\
                .select("*")\
                .gte('created_at', cutoff_time)\
                .order('created_at', desc=True)\
                .execute()
            
            return result.data or []
        except Exception as e:
            logger.error(f"❌ Failed to get recent leads from {table_name}: {e}")
            return []

# Global instance for easy import
supabase = UnifiedSupabaseClient()

# Convenience functions for backward compatibility
def insert_lead(table_name: str, lead_data: Dict[str, Any]) -> bool:
    """Convenience function to insert a single lead"""
    return supabase.safe_insert(table_name, lead_data)

def bulk_insert_leads(table_name: str, leads_data: List[Dict[str, Any]]) -> int:
    """Convenience function to insert multiple leads"""
    return supabase.bulk_insert(table_name, leads_data)

def get_supabase_client() -> UnifiedSupabaseClient:
    """Get the global Supabase client instance"""
    return supabase