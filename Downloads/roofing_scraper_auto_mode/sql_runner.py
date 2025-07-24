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