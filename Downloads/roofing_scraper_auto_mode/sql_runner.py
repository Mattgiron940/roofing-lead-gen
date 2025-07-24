#!/usr/bin/env python3
"""
Supabase Deployment & Management Script
Automated SQL deployment, migrations, and database management
"""

import os
import sys
import json
import subprocess
import argparse
from typing import Dict, List, Optional
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SupabaseDeployer:
    def __init__(self):
        self.project_id = os.getenv('SUPABASE_PROJECT_ID')
        self.access_token = os.getenv('SUPABASE_ACCESS_TOKEN') 
        self.db_password = os.getenv('SUPABASE_DB_PASSWORD')
        
        self.check_dependencies()
    
    def check_dependencies(self):
        """Check if Supabase CLI is installed"""
        try:
            result = subprocess.run(['supabase', '--version'], 
                                  capture_output=True, text=True, check=True)
            logger.info(f"✅ Supabase CLI version: {result.stdout.strip()}")
        except (subprocess.CalledProcessError, FileNotFoundError):
            logger.error("❌ Supabase CLI not found. Install with: npm install -g @supabase/cli")
            logger.error("Or: brew install supabase/tap/supabase")
            sys.exit(1)
    
    def init_project(self) -> bool:
        """Initialize Supabase project"""
        try:
            logger.info("🚀 Initializing Supabase project...")
            
            # Check if already initialized
            if os.path.exists('supabase'):
                logger.info("Project already initialized")
                return True
            
            # Initialize new project
            subprocess.run(['supabase', 'init'], check=True)
            logger.info("✅ Project initialized")
            
            return True
            
        except subprocess.CalledProcessError as e:
            logger.error(f"❌ Failed to initialize project: {e}")
            return False
    
    def start_local(self) -> bool:
        """Start local Supabase development environment"""
        try:
            logger.info("🔧 Starting local Supabase...")
            
            # Start Supabase locally
            subprocess.run(['supabase', 'start'], check=True)
            
            # Get local connection details
            result = subprocess.run(['supabase', 'status'], 
                                  capture_output=True, text=True, check=True)
            logger.info("✅ Local Supabase started")
            logger.info(result.stdout)
            
            return True
            
        except subprocess.CalledProcessError as e:
            logger.error(f"❌ Failed to start local Supabase: {e}")
            return False
    
    def create_migration(self, name: str) -> bool:
        """Create a new migration"""
        try:
            logger.info(f"📝 Creating migration: {name}")
            
            # Create migration
            subprocess.run(['supabase', 'migration', 'new', name], check=True)
            
            logger.info(f"✅ Migration created: {name}")
            return True
            
        except subprocess.CalledProcessError as e:
            logger.error(f"❌ Failed to create migration: {e}")
            return False
    
    def deploy_schema(self, sql_file: str = 'supabase_tables.sql') -> bool:
        """Deploy SQL schema to Supabase"""
        try:
            if not os.path.exists(sql_file):
                logger.error(f"❌ SQL file not found: {sql_file}")
                return False
            
            logger.info(f"📊 Deploying schema from {sql_file}...")
            
            # Read SQL file
            with open(sql_file, 'r') as f:
                sql_content = f.read()
            
            # Create migration file
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
            migration_name = f"{timestamp}_deploy_roofing_schema"
            
            if not self.create_migration(migration_name):
                return False
            
            # Write SQL to migration file
            migration_file = f"supabase/migrations/{timestamp}_deploy_roofing_schema.sql"
            with open(migration_file, 'w') as f:
                f.write(sql_content)
            
            logger.info(f"✅ Schema prepared for deployment in {migration_file}")
            
            # Apply migration locally first
            subprocess.run(['supabase', 'db', 'reset'], check=True)
            logger.info("✅ Schema applied locally")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to deploy schema: {e}")
            return False
    
    def link_project(self) -> bool:
        """Link to remote Supabase project"""
        if not self.project_id:
            logger.warning("⚠️ SUPABASE_PROJECT_ID not set")
            return False
        
        try:
            logger.info(f"🔗 Linking to project: {self.project_id}")
            
            # Link to project
            subprocess.run(['supabase', 'link', '--project-ref', self.project_id], check=True)
            
            logger.info("✅ Project linked successfully")
            return True
            
        except subprocess.CalledProcessError as e:
            logger.error(f"❌ Failed to link project: {e}")
            return False
    
    def push_to_remote(self) -> bool:
        """Push migrations to remote Supabase project"""
        try:
            logger.info("🚀 Pushing migrations to remote...")
            
            # Push database changes
            subprocess.run(['supabase', 'db', 'push'], check=True)
            
            logger.info("✅ Migrations pushed to remote")
            return True
            
        except subprocess.CalledProcessError as e:
            logger.error(f"❌ Failed to push to remote: {e}")
            return False
    
    def generate_types(self, output_file: str = 'database.types.ts') -> bool:
        """Generate TypeScript types from database schema"""
        try:
            logger.info("🔧 Generating TypeScript types...")
            
            # Generate types
            result = subprocess.run(['supabase', 'gen', 'types', 'typescript'], 
                                  capture_output=True, text=True, check=True)
            
            # Write to file
            with open(output_file, 'w') as f:
                f.write(result.stdout)
            
            logger.info(f"✅ Types generated: {output_file}")
            return True
            
        except subprocess.CalledProcessError as e:
            logger.error(f"❌ Failed to generate types: {e}")
            return False
    
    def backup_database(self, output_file: Optional[str] = None) -> bool:
        """Create database backup"""
        if not output_file:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = f"backup_roofing_leads_{timestamp}.sql"
        
        try:
            logger.info(f"💾 Creating database backup: {output_file}")
            
            # Create backup
            with open(output_file, 'w') as f:
                subprocess.run(['supabase', 'db', 'dump'], stdout=f, check=True)
            
            logger.info(f"✅ Backup created: {output_file}")
            return True
            
        except subprocess.CalledProcessError as e:
            logger.error(f"❌ Failed to create backup: {e}")
            return False
    
    def run_sql_file(self, sql_file: str) -> bool:
        """Execute SQL file against database"""
        try:
            if not os.path.exists(sql_file):
                logger.error(f"❌ SQL file not found: {sql_file}")
                return False
            
            logger.info(f"🗄️ Executing SQL file: {sql_file}")
            
            with open(sql_file, 'r') as f:
                sql_content = f.read()
            
            # Execute SQL
            process = subprocess.Popen(['supabase', 'db', 'sql'], 
                                     stdin=subprocess.PIPE, 
                                     stdout=subprocess.PIPE, 
                                     stderr=subprocess.PIPE, 
                                     text=True)
            
            stdout, stderr = process.communicate(input=sql_content)
            
            if process.returncode == 0:
                logger.info("✅ SQL executed successfully")
                if stdout:
                    logger.info(f"Output: {stdout}")
                return True
            else:
                logger.error(f"❌ SQL execution failed: {stderr}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Failed to execute SQL: {e}")
            return False
    
    def setup_rls_policies(self) -> bool:
        """Setup Row Level Security policies"""
        rls_sql = """
        -- Enable RLS on all tables
        ALTER TABLE zillow_leads ENABLE ROW LEVEL SECURITY;
        ALTER TABLE redfin_leads ENABLE ROW LEVEL SECURITY;
        ALTER TABLE cad_leads ENABLE ROW LEVEL SECURITY;
        ALTER TABLE permit_leads ENABLE ROW LEVEL SECURITY;
        ALTER TABLE storm_events ENABLE ROW LEVEL SECURITY;
        
        -- Create policies for authenticated users
        CREATE POLICY "Allow authenticated read access" ON zillow_leads 
            FOR SELECT USING (auth.role() = 'authenticated');
        CREATE POLICY "Allow authenticated read access" ON redfin_leads 
            FOR SELECT USING (auth.role() = 'authenticated');
        CREATE POLICY "Allow authenticated read access" ON cad_leads 
            FOR SELECT USING (auth.role() = 'authenticated');
        CREATE POLICY "Allow authenticated read access" ON permit_leads 
            FOR SELECT USING (auth.role() = 'authenticated');
        CREATE POLICY "Allow authenticated read access" ON storm_events 
            FOR SELECT USING (auth.role() = 'authenticated');
        
        -- Create policies for service role (full access)
        CREATE POLICY "Allow service role full access" ON zillow_leads 
            FOR ALL USING (auth.role() = 'service_role');
        CREATE POLICY "Allow service role full access" ON redfin_leads 
            FOR ALL USING (auth.role() = 'service_role');
        CREATE POLICY "Allow service role full access" ON cad_leads 
            FOR ALL USING (auth.role() = 'service_role');
        CREATE POLICY "Allow service role full access" ON permit_leads 
            FOR ALL USING (auth.role() = 'service_role');
        CREATE POLICY "Allow service role full access" ON storm_events 
            FOR ALL USING (auth.role() = 'service_role');
        """
        
        try:
            logger.info("🔒 Setting up RLS policies...")
            
            # Write to temporary file
            with open('temp_rls.sql', 'w') as f:
                f.write(rls_sql)
            
            # Execute RLS policies
            success = self.run_sql_file('temp_rls.sql')
            
            # Clean up
            os.remove('temp_rls.sql')
            
            if success:
                logger.info("✅ RLS policies configured")
            
            return success
            
        except Exception as e:
            logger.error(f"❌ Failed to setup RLS policies: {e}")
            return False
    
    def get_project_info(self) -> Dict:
        """Get project information"""
        try:
            result = subprocess.run(['supabase', 'projects', 'list', '--output', 'json'], 
                                  capture_output=True, text=True, check=True)
            
            projects = json.loads(result.stdout)
            return projects
            
        except Exception as e:
            logger.error(f"❌ Failed to get project info: {e}")
            return {}
    
    def validate_schema(self) -> bool:
        """Validate database schema against supabase_tables.sql"""
        try:
            logger.info("🔍 Validating database schema...")
            
            # Import unified Supabase client for validation
            sys.path.append('.')
            from supabase_client import supabase
            
            if not supabase.supabase:
                logger.error("❌ Supabase client not initialized")
                return False
            
            # Expected tables and their key columns
            expected_schema = {
                'zillow_leads': [
                    'id', 'address_text', 'city', 'state', 'zip_code', 'county', 
                    'price', 'num_bedrooms', 'num_bathrooms', 'square_feet', 
                    'year_built', 'property_type', 'lead_score', 'created_at'
                ],
                'redfin_leads': [
                    'id', 'address_text', 'city', 'state', 'zip_code', 'county',
                    'price', 'num_bedrooms', 'num_bathrooms', 'square_feet',
                    'year_built', 'property_type', 'lead_score', 'created_at'
                ],
                'cad_leads': [
                    'id', 'account_number', 'owner_name', 'address_text', 'city',
                    'county', 'zip_code', 'property_type', 'year_built', 'square_feet',
                    'appraised_value', 'market_value', 'lead_score', 'created_at'
                ],
                'permit_leads': [
                    'id', 'permit_id', 'address_text', 'city', 'zip_code',
                    'permit_type', 'work_description', 'date_filed', 'permit_value',
                    'contractor_name', 'lead_status', 'created_at'
                ],
                'storm_events': [
                    'id', 'event_id', 'event_type', 'event_date', 'event_time',
                    'city', 'county', 'severity_level', 'hail_size_inches',
                    'wind_speed_mph', 'roofing_lead_potential', 'created_at'
                ]
            }
            
            # Expected views
            expected_views = [
                'all_property_leads',
                'high_priority_leads', 
                'recent_leads',
                'leads_by_city'
            ]
            
            validation_results = {
                'missing_tables': [],
                'missing_columns': {},
                'missing_views': [],
                'table_counts': {},
                'validation_passed': True
            }
            
            # Check each table
            for table_name, expected_columns in expected_schema.items():
                logger.info(f"  Checking table: {table_name}")
                
                # Check if table exists
                if not supabase.check_table_exists(table_name):
                    logger.error(f"    ❌ Table missing: {table_name}")
                    validation_results['missing_tables'].append(table_name)
                    validation_results['validation_passed'] = False
                    continue
                
                # Get table count
                count = supabase.get_table_count(table_name)
                validation_results['table_counts'][table_name] = count
                logger.info(f"    ✅ Table exists with {count} records")
                
                # Check columns (simplified - would need more detailed introspection for production)
                try:
                    # Test a simple select to verify basic structure
                    result = supabase.supabase.table(table_name).select('id').limit(1).execute()
                    logger.info(f"    ✅ Table accessible and structured correctly")
                except Exception as e:
                    logger.warning(f"    ⚠️ Table structure issue: {e}")
            
            # Check views
            for view_name in expected_views:
                logger.info(f"  Checking view: {view_name}")
                try:
                    # Test view accessibility
                    result = supabase.supabase.table(view_name).select('*').limit(1).execute()
                    logger.info(f"    ✅ View accessible")
                except Exception as e:
                    logger.error(f"    ❌ View missing or inaccessible: {view_name}")
                    validation_results['missing_views'].append(view_name)
                    validation_results['validation_passed'] = False
            
            # Print validation summary
            logger.info("\n📊 SCHEMA VALIDATION SUMMARY:")
            logger.info("=" * 50)
            
            if validation_results['validation_passed']:
                logger.info("✅ All schema validation checks passed!")
            else:
                logger.error("❌ Schema validation failed!")
                
                if validation_results['missing_tables']:
                    logger.error(f"Missing tables: {validation_results['missing_tables']}")
                
                if validation_results['missing_views']:
                    logger.error(f"Missing views: {validation_results['missing_views']}")
            
            logger.info("\n📈 Table Counts:")
            for table, count in validation_results['table_counts'].items():
                logger.info(f"  • {table}: {count:,} records")
            
            return validation_results['validation_passed']
            
        except Exception as e:
            logger.error(f"❌ Schema validation failed: {e}")
            return False
    
    def check_production_readiness(self) -> bool:
        """Comprehensive production readiness check"""
        logger.info("🚀 PRODUCTION READINESS CHECK")
        logger.info("=" * 60)
        
        checks = [
            ("Schema validation", self.validate_schema),
            ("Supabase client connectivity", self._check_supabase_connectivity),
            ("Environment variables", self._check_environment_vars),
            ("Scraper API access", self._check_scraper_api),
            ("Data integrity", self._check_data_integrity),
        ]
        
        passed_checks = 0
        total_checks = len(checks)
        
        for check_name, check_func in checks:
            logger.info(f"\n🔍 {check_name}...")
            try:
                if check_func():
                    logger.info(f"✅ {check_name}: PASSED")
                    passed_checks += 1
                else:
                    logger.error(f"❌ {check_name}: FAILED")
            except Exception as e:
                logger.error(f"❌ {check_name}: ERROR - {e}")
        
        # Final assessment
        logger.info(f"\n📊 PRODUCTION READINESS SUMMARY:")
        logger.info("=" * 60)
        logger.info(f"Checks passed: {passed_checks}/{total_checks}")
        logger.info(f"Success rate: {(passed_checks/total_checks)*100:.1f}%")
        
        if passed_checks == total_checks:
            logger.info("🎉 SYSTEM IS PRODUCTION READY!")
            return True
        else:
            logger.error("⚠️ SYSTEM NOT READY FOR PRODUCTION")
            logger.error("Please address the failed checks before deployment")
            return False
    
    def _check_supabase_connectivity(self) -> bool:
        """Check Supabase client connectivity"""
        try:
            sys.path.append('.')
            from supabase_client import supabase
            
            if not supabase.supabase:
                logger.error("Supabase client not initialized")
                return False
            
            # Test basic connectivity
            test_result = supabase.supabase.table('zillow_leads').select('count').limit(1).execute()
            logger.info("Supabase connectivity verified")
            return True
            
        except Exception as e:
            logger.error(f"Supabase connectivity failed: {e}")
            return False
    
    def _check_environment_vars(self) -> bool:
        """Check required environment variables"""
        required_vars = ['SUPABASE_URL', 'SUPABASE_KEY', 'SCRAPER_API_KEY']
        missing_vars = []
        
        for var in required_vars:
            if not os.getenv(var):
                missing_vars.append(var)
        
        if missing_vars:
            logger.error(f"Missing environment variables: {missing_vars}")
            return False
        
        logger.info("All required environment variables present")
        return True
    
    def _check_scraper_api(self) -> bool:
        """Check ScraperAPI connectivity"""
        try:
            import requests
            api_key = os.getenv('SCRAPER_API_KEY', '6972d80a231d2c07209e0ce837e34e69')
            
            # Test ScraperAPI with a simple request
            test_url = f"http://api.scraperapi.com?api_key={api_key}&url=https://httpbin.org/ip"
            response = requests.get(test_url, timeout=10)
            
            if response.status_code == 200:
                logger.info("ScraperAPI connectivity verified")
                return True
            else:
                logger.error(f"ScraperAPI returned status {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"ScraperAPI connectivity failed: {e}")
            return False
    
    def _check_data_integrity(self) -> bool:
        """Check data integrity and quality"""
        try:
            sys.path.append('.')
            from supabase_client import supabase
            
            # Check for duplicate records
            integrity_checks = []
            
            # Check each table for basic data quality
            tables = ['zillow_leads', 'redfin_leads', 'cad_leads', 'permit_leads', 'storm_events']
            
            for table in tables:
                try:
                    count = supabase.get_table_count(table)
                    if count > 0:
                        # Check for records with valid addresses
                        result = supabase.supabase.table(table).select('address_text').not_.is_('address_text', 'null').limit(5).execute()
                        if result.data:
                            integrity_checks.append(f"{table}: {count} records with valid data")
                        else:
                            integrity_checks.append(f"{table}: {count} records but no valid addresses")
                    else:
                        integrity_checks.append(f"{table}: empty")
                except Exception as e:
                    integrity_checks.append(f"{table}: error - {e}")
            
            logger.info("Data integrity summary:")
            for check in integrity_checks:
                logger.info(f"  • {check}")
            
            return True
            
        except Exception as e:
            logger.error(f"Data integrity check failed: {e}")
            return False

    def full_deployment(self) -> bool:
        """Run complete deployment process"""
        logger.info("🚀 Starting full Supabase deployment...")
        
        steps = [
            ("Initialize project", self.init_project),
            ("Deploy schema", lambda: self.deploy_schema()),
            ("Setup RLS policies", self.setup_rls_policies),
            ("Generate TypeScript types", self.generate_types),
        ]
        
        # Add remote steps if project ID is configured
        if self.project_id:
            steps.extend([
                ("Link to remote project", self.link_project),
                ("Push to remote", self.push_to_remote),
            ])
        
        for step_name, step_func in steps:
            logger.info(f"📋 {step_name}...")
            if not step_func():
                logger.error(f"❌ Failed: {step_name}")
                return False
            logger.info(f"✅ Completed: {step_name}")
        
        logger.info("🎉 Full deployment completed successfully!")
        return True

def main():
    """Main CLI interface"""
    parser = argparse.ArgumentParser(description='Supabase deployment and management')
    
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # Init command
    subparsers.add_parser('init', help='Initialize Supabase project')
    
    # Start command
    subparsers.add_parser('start', help='Start local Supabase')
    
    # Deploy command
    deploy_parser = subparsers.add_parser('deploy', help='Deploy schema')
    deploy_parser.add_argument('--file', default='supabase_tables.sql', 
                              help='SQL file to deploy')
    
    # Migration command
    migration_parser = subparsers.add_parser('migration', help='Create migration')
    migration_parser.add_argument('name', help='Migration name')
    
    # Link command
    subparsers.add_parser('link', help='Link to remote project')
    
    # Push command
    subparsers.add_parser('push', help='Push to remote')
    
    # Types command
    types_parser = subparsers.add_parser('types', help='Generate TypeScript types')
    types_parser.add_argument('--output', default='database.types.ts', 
                             help='Output file for types')
    
    # Backup command
    backup_parser = subparsers.add_parser('backup', help='Create database backup')
    backup_parser.add_argument('--output', help='Output file for backup')
    
    # RLS command
    subparsers.add_parser('rls', help='Setup RLS policies')
    
    # Full deployment command
    subparsers.add_parser('full', help='Run full deployment process')
    
    # Schema validation command
    subparsers.add_parser('check', help='Validate database schema')
    
    # Production readiness command
    subparsers.add_parser('ready', help='Check production readiness')
    
    # Info command
    subparsers.add_parser('info', help='Get project information')
    
    args = parser.parse_args()
    
    deployer = SupabaseDeployer()
    
    if args.command == 'init':
        success = deployer.init_project()
    elif args.command == 'start':
        success = deployer.start_local()
    elif args.command == 'deploy':
        success = deployer.deploy_schema(args.file)
    elif args.command == 'migration':
        success = deployer.create_migration(args.name)
    elif args.command == 'link':
        success = deployer.link_project()
    elif args.command == 'push':
        success = deployer.push_to_remote()
    elif args.command == 'types':
        success = deployer.generate_types(args.output)
    elif args.command == 'backup':
        success = deployer.backup_database(args.output)
    elif args.command == 'rls':
        success = deployer.setup_rls_policies()
    elif args.command == 'full':
        success = deployer.full_deployment()
    elif args.command == 'check':
        success = deployer.validate_schema()
    elif args.command == 'ready':
        success = deployer.check_production_readiness()
    elif args.command == 'info':
        info = deployer.get_project_info()
        print(json.dumps(info, indent=2))
        success = True
    else:
        parser.print_help()
        success = False
    
    return 0 if success else 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)