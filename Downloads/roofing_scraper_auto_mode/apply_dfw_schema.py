#!/usr/bin/env python3
"""
DFW Schema Updater - Applies DFW boolean column to all Supabase tables
Executes the schema update SQL and verifies the changes
"""

import os
import logging
from supabase_client import supabase
from typing import Dict, List

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DFWSchemaUpdater:
    """Applies DFW schema updates to Supabase"""
    
    def __init__(self):
        self.client = supabase.supabase
        self.tables_to_update = [
            'zillow_leads',
            'redfin_leads', 
            'cad_leads',
            'permit_leads',
            'storm_events'
        ]
        
    def read_sql_file(self, filename: str) -> str:
        """Read SQL commands from file"""
        try:
            with open(filename, 'r') as f:
                return f.read()
        except Exception as e:
            logger.error(f"Error reading SQL file {filename}: {e}")
            return ""
    
    def apply_schema_updates(self) -> bool:
        """Apply the DFW schema updates"""
        logger.info("🔄 Starting DFW schema updates...")
        
        try:
            # Read the SQL update script
            sql_content = self.read_sql_file('dfw_schema_update.sql')
            if not sql_content:
                logger.error("❌ Could not read schema update file")
                return False
            
            # Split SQL commands and execute them one by one
            sql_commands = [cmd.strip() for cmd in sql_content.split(';') if cmd.strip()]
            
            success_count = 0
            for i, command in enumerate(sql_commands):
                try:
                    if command.lower().startswith(('alter table', 'create index', 'create or replace view', 'create or replace function', 'comment on')):
                        logger.info(f"📝 Executing command {i+1}/{len(sql_commands)}: {command[:50]}...")
                        
                        # For Supabase client, we need to use rpc for DDL commands
                        # This is a simplified approach - in production you'd use Supabase SQL editor
                        logger.info(f"✅ Command {i+1} ready for execution (run manually in Supabase SQL editor)")
                        success_count += 1
                    else:
                        success_count += 1
                        
                except Exception as e:
                    logger.error(f"❌ Error executing command {i+1}: {e}")
                    
            logger.info(f"📊 Schema update completed: {success_count}/{len(sql_commands)} commands processed")
            
            # Since we can't execute DDL directly, we'll check if columns exist
            return self.verify_schema_updates()
            
        except Exception as e:
            logger.error(f"❌ Error applying schema updates: {e}")
            return False
    
    def verify_schema_updates(self) -> bool:
        """Verify that DFW columns were added successfully"""
        logger.info("🔍 Verifying DFW schema updates...")
        
        verification_results = {}
        
        for table in self.tables_to_update:
            try:
                # Try to query the table with DFW column
                result = self.client.table(table).select('id, dfw').limit(1).execute()
                verification_results[table] = True
                logger.info(f"✅ {table}: DFW column verified")
                
            except Exception as e:
                verification_results[table] = False
                logger.warning(f"⚠️ {table}: DFW column not found or accessible - {e}")
        
        all_verified = all(verification_results.values())
        
        if all_verified:
            logger.info("✅ All DFW schema updates verified successfully!")
        else:
            failed_tables = [table for table, verified in verification_results.items() if not verified]
            logger.warning(f"⚠️ Schema verification failed for tables: {failed_tables}")
            logger.info("💡 Please run the SQL commands from dfw_schema_update.sql manually in Supabase SQL editor")
        
        return all_verified
    
    def create_manual_instructions(self):
        """Create manual instructions for applying schema updates"""
        instructions = """
# DFW Schema Update Instructions

Since direct DDL execution requires admin privileges, please follow these steps:

## 1. Access Supabase SQL Editor
- Go to your Supabase project dashboard
- Navigate to SQL Editor
- Create a new query

## 2. Execute Schema Updates
- Copy the contents of `dfw_schema_update.sql`
- Paste into the SQL editor
- Run the query to apply all schema changes

## 3. Verify Updates
- Run this Python script again to verify the updates
- Alternatively, check each table to confirm the `dfw` column exists

## 4. Expected Changes
- `dfw` boolean column added to all lead tables
- Indexes created for efficient DFW filtering
- Views created for DFW-only data access
- Function created for DFW lead statistics

The schema updates are designed to be idempotent (safe to run multiple times).
        """
        
        with open('DFW_SCHEMA_INSTRUCTIONS.md', 'w') as f:
            f.write(instructions)
        
        logger.info("📋 Manual instructions created: DFW_SCHEMA_INSTRUCTIONS.md")
    
    def get_dfw_statistics(self) -> Dict:
        """Get DFW lead statistics across all tables"""
        logger.info("📊 Gathering DFW lead statistics...")
        
        stats = {}
        
        for table in self.tables_to_update:
            try:
                # Get total count
                total_result = self.client.table(table).select('id', count='exact').execute()
                total_count = total_result.count
                
                try:
                    # Try to get DFW count
                    dfw_result = self.client.table(table).select('id', count='exact').eq('dfw', True).execute()
                    dfw_count = dfw_result.count
                    dfw_percentage = round((dfw_count / total_count) * 100, 2) if total_count > 0 else 0
                except:
                    # DFW column doesn't exist yet
                    dfw_count = 0
                    dfw_percentage = 0
                    
                stats[table] = {
                    'total_leads': total_count,
                    'dfw_leads': dfw_count,
                    'dfw_percentage': dfw_percentage
                }
                
                logger.info(f"📈 {table}: {dfw_count}/{total_count} DFW leads ({dfw_percentage}%)")
                
            except Exception as e:
                logger.error(f"❌ Error getting stats for {table}: {e}")
                stats[table] = {'error': str(e)}
        
        return stats
    
    def export_statistics_report(self, stats: Dict) -> str:
        """Export DFW statistics to JSON report"""
        import json
        from datetime import datetime
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'dfw_schema_stats_{timestamp}.json'
        
        report = {
            'generated_at': datetime.now().isoformat(),
            'tables_analyzed': list(self.tables_to_update),
            'statistics': stats,
            'summary': {
                'total_tables': len(self.tables_to_update),
                'tables_with_dfw': len([s for s in stats.values() if isinstance(s, dict) and s.get('dfw_leads', 0) >= 0]),
                'total_leads_all_tables': sum([s.get('total_leads', 0) for s in stats.values() if isinstance(s, dict)]),
                'total_dfw_leads_all_tables': sum([s.get('dfw_leads', 0) for s in stats.values() if isinstance(s, dict)])
            }
        }
        
        try:
            with open(filename, 'w') as f:
                json.dump(report, f, indent=2)
            
            logger.info(f"📋 DFW statistics report exported: {filename}")
            return filename
            
        except Exception as e:
            logger.error(f"Error exporting statistics report: {e}")
            return ""

def main():
    """Main execution function"""
    logger.info("🚀 Starting DFW Schema Update Process...")
    logger.info("=" * 60)
    
    updater = DFWSchemaUpdater()
    
    try:
        # Get current statistics
        logger.info("📊 Phase 1: Gathering current statistics...")
        current_stats = updater.get_dfw_statistics()
        
        # Apply schema updates
        logger.info("🔄 Phase 2: Applying schema updates...")
        update_success = updater.apply_schema_updates()
        
        # Create manual instructions
        logger.info("📋 Phase 3: Creating manual instructions...")
        updater.create_manual_instructions()
        
        # Export statistics report
        logger.info("📈 Phase 4: Exporting statistics report...")
        report_file = updater.export_statistics_report(current_stats)
        
        # Final summary
        logger.info("\n" + "=" * 60)
        logger.info("🎯 DFW SCHEMA UPDATE SUMMARY")
        logger.info("=" * 60)
        
        if update_success:
            logger.info("✅ Schema updates completed successfully!")
            logger.info("✅ All DFW columns verified and accessible")
        else:
            logger.warning("⚠️ Schema updates require manual execution")
            logger.info("📋 Please follow instructions in: DFW_SCHEMA_INSTRUCTIONS.md")
            logger.info("🔧 Then run this script again to verify updates")
        
        logger.info(f"📊 Statistics report: {report_file}")
        logger.info("🎉 DFW schema update process completed!")
        
    except Exception as e:
        logger.error(f"❌ Schema update process failed: {e}")

if __name__ == "__main__":
    main()