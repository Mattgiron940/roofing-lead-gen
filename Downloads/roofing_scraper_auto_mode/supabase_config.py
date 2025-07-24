#!/usr/bin/env python3
"""
Shared Supabase Configuration Module
Centralized connection and utilities for all scrapers
"""

import os
import logging
from typing import Optional, Dict, Any
from supabase import create_client, Client
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)

class SupabaseConnection:
    """Centralized Supabase connection manager"""
    
    def __init__(self):
        self.supabase: Optional[Client] = None
        self.initialize_connection()
    
    def initialize_connection(self) -> bool:
        """Initialize Supabase client with environment variables"""
        try:
            url = os.getenv("SUPABASE_URL")
            key = os.getenv("SUPABASE_KEY")
            
            if not url or not key:
                logger.warning("SUPABASE_URL or SUPABASE_KEY not found in environment variables")
                logger.info("Using fallback hardcoded credentials for development")
                
                # Fallback to hardcoded values for development
                url = "https://rupqnhgtzfynvzgxkgch.supabase.co"
                key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJ1cHFuaGd0emZ5bnZ6Z3hrZ2NoIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTMzMDc1NzEsImV4cCI6MjA2ODg4MzU3MX0.kVIh0HhG2BUjqptokZM_ci9G0cFeCPNtv3wUxRxts0c"
            
            self.supabase = create_client(url, key)
            logger.info("✅ Supabase client initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize Supabase client: {e}")
            self.supabase = None
            return False
    
    def get_client(self) -> Optional[Client]:
        """Get the Supabase client instance"""
        return self.supabase
    
    def is_connected(self) -> bool:
        """Check if Supabase connection is available"""
        return self.supabase is not None

    def safe_insert(self, table_name: str, data: Dict[str, Any]) -> bool:
        """Safely insert data into a Supabase table with error handling"""
        if not self.supabase:
            logger.warning(f"⚠️ Supabase not available, skipping insert to {table_name}")
            return False
        
        try:
            # Remove None values and empty strings
            clean_data = {k: v for k, v in data.items() if v is not None and v != ''}
            
            result = self.supabase.table(table_name).insert(clean_data).execute()
            
            if result.data:
                logger.debug(f"✅ Inserted record into {table_name}: {data.get('address', 'Unknown')}")
                return True
            else:
                logger.warning(f"⚠️ No data returned from {table_name} insert")
                return False
                
        except Exception as e:
            logger.error(f"❌ Failed to insert into {table_name}: {e}")
            return False

# Global instance for easy import
supabase_conn = SupabaseConnection()

def get_supabase_client() -> Optional[Client]:
    """Convenience function to get Supabase client"""
    return supabase_conn.get_client()

def insert_lead(table_name: str, lead_data: Dict[str, Any]) -> bool:
    """Convenience function to insert a lead into any table"""
    return supabase_conn.safe_insert(table_name, lead_data)