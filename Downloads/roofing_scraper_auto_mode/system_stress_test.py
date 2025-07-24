#!/usr/bin/env python3
"""
Roofing Lead Generation System - Comprehensive Stress Test
Validates all components, dependencies, and integrations
"""

import os
import sys
import json
import time
import traceback
from datetime import datetime
from typing import Dict, List, Any
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SystemStressTest:
    def __init__(self):
        self.results = {
            'test_timestamp': datetime.now().isoformat(),
            'environment_check': {},
            'dependency_audit': {},
            'supabase_validation': {},
            'scraper_stress_test': {},
            'lead_routing_check': {},
            'dashboard_reporting': {},
            'webhook_test': {},
            'overall_status': 'UNKNOWN',
            'critical_issues': [],
            'warnings': [],
            'recommendations': []
        }
        
    def log_result(self, category: str, test_name: str, status: str, details: Any = None):
        """Log test result to results dictionary"""
        if category not in self.results:
            self.results[category] = {}
        
        self.results[category][test_name] = {
            'status': status,
            'details': details,
            'timestamp': datetime.now().isoformat()
        }
        
        # Log to console
        status_emoji = "✅" if status == "PASS" else "❌" if status == "FAIL" else "⚠️"
        logger.info(f"{status_emoji} {category} - {test_name}: {status}")
        if details:
            logger.debug(f"   Details: {details}")

    def environment_check(self):
        """Check environment variables and .env file"""
        logger.info("🔍 Starting Environment Check...")
        
        try:
            # Check if .env file exists
            env_file_path = '.env'
            env_exists = os.path.exists(env_file_path)
            self.log_result('environment_check', 'env_file_exists', 
                          'PASS' if env_exists else 'FAIL', 
                          f".env file {'found' if env_exists else 'missing'}")
            
            # Required environment variables
            required_vars = {
                'SUPABASE_URL': 'Supabase database URL',
                'SUPABASE_KEY': 'Supabase service key',
                'SCRAPERAPI_KEY': 'ScraperAPI key for proxy rotation',
                'EMAIL_USER': 'SMTP email username',
                'EMAIL_PASSWORD': 'SMTP email password', 
                'EMAIL_RECIPIENTS': 'Comma-separated email recipients',
                'GHL_WEBHOOK_URL': 'GoHighLevel webhook URL',
                'ZAPIER_WEBHOOK_URL': 'Zapier webhook URL'
            }
            
            missing_vars = []
            placeholder_vars = []
            valid_vars = []
            
            for var, description in required_vars.items():
                value = os.getenv(var)
                if not value:
                    missing_vars.append(f"{var} ({description})")
                elif value in ['your_value_here', 'placeholder', 'change_me', '']:
                    placeholder_vars.append(f"{var} ({description})")
                else:
                    valid_vars.append(var)
            
            # Log results
            self.log_result('environment_check', 'required_variables', 
                          'PASS' if not missing_vars else 'FAIL',
                          {
                              'valid_vars': valid_vars,
                              'missing_vars': missing_vars,
                              'placeholder_vars': placeholder_vars
                          })
            
            if missing_vars:
                self.results['critical_issues'].append(f"Missing environment variables: {', '.join(missing_vars)}")
                
            if placeholder_vars:
                self.results['warnings'].append(f"Placeholder values detected: {', '.join(placeholder_vars)}")
                
        except Exception as e:
            self.log_result('environment_check', 'overall', 'FAIL', str(e))
            self.results['critical_issues'].append(f"Environment check failed: {e}")

    def dependency_audit(self):
        """Check all required Python packages"""
        logger.info("📦 Starting Dependency Audit...")
        
        required_packages = {
            'beautifulsoup4': 'HTML parsing',
            'python-dotenv': 'Environment variable loading',
            'supabase': 'Database connectivity',
            'requests': 'HTTP requests',
            'pandas': 'Data manipulation',
            'streamlit': 'Dashboard framework',
            'plotly': 'Interactive charts',
            'jinja2': 'Email templating',
            'concurrent.futures': 'Threading support'  # Built-in module
        }
        
        missing_packages = []
        installed_packages = []
        version_info = {}
        
        for package, description in required_packages.items():
            try:
                if package == 'concurrent.futures':
                    # Built-in module check
                    import concurrent.futures
                    installed_packages.append(package)
                    version_info[package] = 'built-in'
                else:
                    __import__(package.replace('-', '_'))
                    installed_packages.append(package)
                    
                    # Try to get version
                    try:
                        import pkg_resources
                        version = pkg_resources.get_distribution(package).version
                        version_info[package] = version
                    except:
                        version_info[package] = 'unknown'
                        
            except ImportError:
                missing_packages.append(f"{package} ({description})")
        
        self.log_result('dependency_audit', 'package_check',
                      'PASS' if not missing_packages else 'FAIL',
                      {
                          'installed': installed_packages,
                          'missing': missing_packages,
                          'versions': version_info
                      })
        
        if missing_packages:
            self.results['critical_issues'].append(f"Missing packages: {', '.join(missing_packages)}")

    def supabase_validation(self):
        """Validate Supabase connectivity and table structure"""
        logger.info("🗄️ Starting Supabase Validation...")
        
        try:
            # Import supabase client
            sys.path.append('.')
            from supabase_client import supabase
            
            # Test connection
            try:
                # Try a simple query to test connectivity
                result = supabase.supabase.table('zillow_leads').select('id').limit(1).execute()
                self.log_result('supabase_validation', 'connectivity', 'PASS', 'Connection successful')
            except Exception as e:
                self.log_result('supabase_validation', 'connectivity', 'FAIL', str(e))
                self.results['critical_issues'].append(f"Supabase connection failed: {e}")
                return
            
            # Check required tables
            required_tables = ['zillow_leads', 'redfin_leads', 'cad_leads', 'permit_leads', 'storm_events']
            table_status = {}
            
            for table in required_tables:
                try:
                    result = supabase.supabase.table(table).select('id').limit(1).execute()
                    count_result = supabase.get_table_count(table)
                    table_status[table] = {
                        'exists': True,
                        'accessible': True,
                        'record_count': count_result
                    }
                except Exception as e:
                    table_status[table] = {
                        'exists': False,
                        'accessible': False,
                        'error': str(e)
                    }
            
            all_tables_ok = all(status['exists'] and status['accessible'] for status in table_status.values())
            self.log_result('supabase_validation', 'table_structure', 
                          'PASS' if all_tables_ok else 'FAIL',
                          table_status)
            
            # Test read/write capability
            try:
                test_data = {
                    'address_text': 'TEST ADDRESS - STRESS TEST',
                    'city': 'TEST CITY',
                    'state': 'TX',
                    'zip_code': '99999',
                    'price': 1,
                    'lead_score': 1
                }
                
                # Insert test record
                insert_result = supabase.supabase.table('zillow_leads').insert(test_data).execute()
                
                # Clean up test record
                if insert_result.data:
                    test_id = insert_result.data[0]['id']
                    supabase.supabase.table('zillow_leads').delete().eq('id', test_id).execute()
                
                self.log_result('supabase_validation', 'read_write_test', 'PASS', 'Read/write operations successful')
                
            except Exception as e:
                self.log_result('supabase_validation', 'read_write_test', 'FAIL', str(e))
                self.results['warnings'].append(f"Supabase write test failed: {e}")
                
        except Exception as e:
            self.log_result('supabase_validation', 'overall', 'FAIL', str(e))
            self.results['critical_issues'].append(f"Supabase validation failed: {e}")

    def scraper_stress_test(self):
        """Run all scrapers and measure performance"""
        logger.info("🤖 Starting Scraper Stress Test...")
        
        scrapers = {
            'redfin_scraper': 'redfin_scraper.py',
            'texas_cad_scraper': 'texas_cad_scraper.py', 
            'permit_scraper': 'permit_scraper.py',
            'storm_integration': 'storm_integration.py'
        }
        
        scraper_results = {}
        
        for scraper_name, script_file in scrapers.items():
            try:
                if not os.path.exists(script_file):
                    scraper_results[scraper_name] = {
                        'status': 'FAIL',
                        'error': f'Script file {script_file} not found'
                    }
                    continue
                
                logger.info(f"Testing {scraper_name}...")
                
                # Import and test scraper
                start_time = time.time()
                
                if scraper_name == 'redfin_scraper':
                    from redfin_scraper import DFWRedfinScraper
                    scraper = DFWRedfinScraper(max_workers=3)  # Reduced for stress test
                    results = scraper.scrape_dfw_redfin_properties()
                    
                elif scraper_name == 'texas_cad_scraper':
                    from texas_cad_scraper import TexasCADScraper
                    scraper = TexasCADScraper(max_workers=3)
                    results = scraper.scrape_all_texas_cads()
                    
                elif scraper_name == 'permit_scraper':
                    from permit_scraper import DFWPermitScraper
                    scraper = DFWPermitScraper(max_workers=2)
                    results = scraper.scrape_all_permits()
                    
                elif scraper_name == 'storm_integration':
                    from storm_integration import StormDataIntegrator
                    integrator = StormDataIntegrator(max_workers=2)
                    results = integrator.collect_storm_data_threaded()
                
                end_time = time.time()
                runtime = end_time - start_time
                
                scraper_results[scraper_name] = {
                    'status': 'PASS',
                    'runtime_seconds': round(runtime, 2),
                    'records_processed': len(results) if results else 0,
                    'throughput_per_second': round(len(results) / runtime, 2) if results and runtime > 0 else 0,
                    'database_insertions': getattr(scraper, 'processed_count', 0) if 'scraper' in locals() else getattr(integrator, 'processed_count', 0)
                }
                
            except Exception as e:
                scraper_results[scraper_name] = {
                    'status': 'FAIL',
                    'error': str(e),
                    'traceback': traceback.format_exc()
                }
                self.results['critical_issues'].append(f"{scraper_name} failed: {e}")
        
        self.log_result('scraper_stress_test', 'all_scrapers', 
                      'PASS' if all(r['status'] == 'PASS' for r in scraper_results.values()) else 'FAIL',
                      scraper_results)

    def lead_routing_check(self):
        """Test lead routing functionality"""
        logger.info("🧠 Starting Lead Routing Check...")
        
        try:
            # Check if lead_router.py exists
            if not os.path.exists('lead_router.py'):
                self.log_result('lead_routing_check', 'router_exists', 'FAIL', 'lead_router.py not found')
                return
            
            # Import and test basic routing logic
            # Since we can't run the full router, we'll test the scoring logic
            sys.path.append('.')
            from supabase_client import supabase
            
            # Test lead scoring functionality
            sample_lead = {
                'price': 350000,
                'year_built': 2010,
                'city': 'Dallas',
                'zip_code': '75201'
            }
            
            # Calculate basic lead score
            lead_score = supabase.calculate_lead_score(
                sample_lead.get('price', 0),
                sample_lead.get('year_built', 2020),
                sample_lead.get('city', ''),
                sample_lead.get('zip_code', '')
            )
            
            self.log_result('lead_routing_check', 'scoring_logic', 'PASS', 
                          f'Sample lead scored: {lead_score}/10')
            
        except Exception as e:
            self.log_result('lead_routing_check', 'overall', 'FAIL', str(e))
            self.results['warnings'].append(f"Lead routing check failed: {e}")

    def dashboard_reporting_check(self):
        """Test dashboard and reporting components"""
        logger.info("📊 Starting Dashboard & Reporting Check...")
        
        dashboard_components = {
            'lead_dashboard.py': 'Streamlit dashboard',
            'email_reports.py': 'Email reporting system',
            'webhook_integration.py': 'Webhook system'
        }
        
        component_results = {}
        
        for component, description in dashboard_components.items():
            try:
                if not os.path.exists(component):
                    component_results[component] = {
                        'status': 'FAIL',
                        'error': f'{component} not found'
                    }
                    continue
                
                # Test import capability
                if component == 'lead_dashboard.py':
                    # Test dashboard imports
                    import importlib.util
                    spec = importlib.util.spec_from_file_location("lead_dashboard", component)
                    module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(module)
                    
                elif component == 'email_reports.py':
                    from email_reports import EmailReporter
                    reporter = EmailReporter()
                    # Test data generation
                    data = reporter.get_daily_summary_data()
                    
                elif component == 'webhook_integration.py':
                    from webhook_integration import WebhookManager
                    webhook_manager = WebhookManager()
                    
                component_results[component] = {
                    'status': 'PASS',
                    'description': description
                }
                
            except Exception as e:
                component_results[component] = {
                    'status': 'FAIL',
                    'error': str(e)
                }
        
        self.log_result('dashboard_reporting', 'components', 
                      'PASS' if all(r['status'] == 'PASS' for r in component_results.values()) else 'FAIL',
                      component_results)

    def webhook_test(self):
        """Test webhook integrations"""
        logger.info("🔗 Starting Webhook Test...")
        
        try:
            from webhook_integration import WebhookManager
            webhook_manager = WebhookManager()
            
            # Test webhook configurations
            webhook_count = len(webhook_manager.webhooks)
            
            if webhook_count == 0:
                self.log_result('webhook_test', 'configuration', 'FAIL', 'No webhooks configured')
                self.results['warnings'].append("No webhook URLs configured in environment")
                return
            
            # Test webhook connectivity (without actually sending)
            webhook_results = {}
            for webhook in webhook_manager.webhooks:
                webhook_results[webhook.name] = {
                    'configured': True,
                    'url_present': bool(webhook.url),
                    'enabled': webhook.enabled
                }
            
            self.log_result('webhook_test', 'configuration', 'PASS', 
                          f'{webhook_count} webhooks configured: {webhook_results}')
            
        except Exception as e:
            self.log_result('webhook_test', 'overall', 'FAIL', str(e))
            self.results['warnings'].append(f"Webhook test failed: {e}")

    def generate_summary_report(self):
        """Generate final summary report"""
        logger.info("📝 Generating Summary Report...")
        
        # Count test results
        total_tests = 0
        passed_tests = 0
        failed_tests = 0
        
        for category, tests in self.results.items():
            if isinstance(tests, dict) and 'status' in str(tests):
                for test_name, test_result in tests.items():
                    if isinstance(test_result, dict) and 'status' in test_result:
                        total_tests += 1
                        if test_result['status'] == 'PASS':
                            passed_tests += 1
                        elif test_result['status'] == 'FAIL':
                            failed_tests += 1
        
        # Determine overall system status
        critical_issues = len(self.results['critical_issues'])
        warnings = len(self.results['warnings'])
        
        if critical_issues == 0 and failed_tests == 0:
            self.results['overall_status'] = 'HEALTHY'
        elif critical_issues == 0 and failed_tests <= 2:
            self.results['overall_status'] = 'OPERATIONAL_WITH_WARNINGS'
        else:
            self.results['overall_status'] = 'CRITICAL_ISSUES_DETECTED'
        
        # Add test statistics
        self.results['test_statistics'] = {
            'total_tests': total_tests,
            'passed_tests': passed_tests,
            'failed_tests': failed_tests,
            'success_rate': round((passed_tests / total_tests * 100), 1) if total_tests > 0 else 0
        }
        
        # Generate recommendations
        if self.results['critical_issues']:
            self.results['recommendations'].append("Address critical issues immediately before production deployment")
        
        if self.results['warnings']:
            self.results['recommendations'].append("Review warnings and configure missing optional components")
            
        if self.results['test_statistics']['success_rate'] < 80:
            self.results['recommendations'].append("System requires significant fixes before production use")
        elif self.results['test_statistics']['success_rate'] < 95:
            self.results['recommendations'].append("System is functional but needs minor improvements")
        else:
            self.results['recommendations'].append("System is production-ready with minimal issues")

    def run_full_stress_test(self):
        """Execute complete stress test suite"""
        logger.info("🚀 Starting Full System Stress Test...")
        logger.info("=" * 80)
        
        try:
            # Run all test categories
            self.environment_check()
            self.dependency_audit()
            self.supabase_validation()
            self.scraper_stress_test()
            self.lead_routing_check()
            self.dashboard_reporting_check()
            self.webhook_test()
            
            # Generate final report
            self.generate_summary_report()
            
            # Print summary to console
            self.print_summary_report()
            
            # Write detailed report to file
            self.write_detailed_report()
            
        except Exception as e:
            logger.error(f"Stress test failed: {e}")
            self.results['overall_status'] = 'TEST_EXECUTION_FAILED'
            self.results['critical_issues'].append(f"Test execution failed: {e}")

    def print_summary_report(self):
        """Print summary report to console"""
        print("\n" + "=" * 80)
        print("🏠 ROOFING LEAD GENERATION SYSTEM - STRESS TEST RESULTS")
        print("=" * 80)
        
        # Overall status
        status_emoji = {
            'HEALTHY': '✅',
            'OPERATIONAL_WITH_WARNINGS': '⚠️',
            'CRITICAL_ISSUES_DETECTED': '❌',
            'TEST_EXECUTION_FAILED': '💥'
        }
        
        overall_status = self.results['overall_status']
        print(f"\n📊 OVERALL SYSTEM STATUS: {status_emoji.get(overall_status, '❓')} {overall_status}")
        
        # Test statistics
        stats = self.results.get('test_statistics', {})
        print(f"\n📈 TEST STATISTICS:")
        print(f"   • Total Tests: {stats.get('total_tests', 0)}")
        print(f"   • Passed: {stats.get('passed_tests', 0)}")
        print(f"   • Failed: {stats.get('failed_tests', 0)}")
        print(f"   • Success Rate: {stats.get('success_rate', 0)}%")
        
        # Critical issues
        if self.results['critical_issues']:
            print(f"\n🚨 CRITICAL ISSUES ({len(self.results['critical_issues'])}):")
            for issue in self.results['critical_issues']:
                print(f"   • {issue}")
        
        # Warnings
        if self.results['warnings']:
            print(f"\n⚠️ WARNINGS ({len(self.results['warnings'])}):")
            for warning in self.results['warnings']:
                print(f"   • {warning}")
        
        # Recommendations
        if self.results['recommendations']:
            print(f"\n💡 RECOMMENDATIONS:")
            for rec in self.results['recommendations']:
                print(f"   • {rec}")
        
        print("\n" + "=" * 80)
        print(f"📝 Detailed results written to: ~/Desktop/stress_test_results.txt")
        print("=" * 80)

    def write_detailed_report(self):
        """Write detailed report to file"""
        try:
            desktop_path = os.path.expanduser("~/Desktop/stress_test_results.txt")
            
            with open(desktop_path, 'w') as f:
                f.write("ROOFING LEAD GENERATION SYSTEM - DETAILED STRESS TEST RESULTS\n")
                f.write("=" * 80 + "\n")
                f.write(f"Test Timestamp: {self.results['test_timestamp']}\n")
                f.write(f"Overall Status: {self.results['overall_status']}\n\n")
                
                # Write detailed results for each category
                for category, tests in self.results.items():
                    if category in ['test_timestamp', 'overall_status', 'critical_issues', 'warnings', 'recommendations', 'test_statistics']:
                        continue
                    
                    f.write(f"\n{category.upper().replace('_', ' ')}\n")
                    f.write("-" * 40 + "\n")
                    
                    if isinstance(tests, dict):
                        for test_name, result in tests.items():
                            if isinstance(result, dict) and 'status' in result:
                                f.write(f"{test_name}: {result['status']}\n")
                                if 'details' in result and result['details']:
                                    f.write(f"  Details: {json.dumps(result['details'], indent=2)}\n")
                                f.write(f"  Timestamp: {result['timestamp']}\n\n")
                
                # Write summary sections
                f.write("\nTEST STATISTICS\n")
                f.write("-" * 40 + "\n")
                stats = self.results.get('test_statistics', {})
                for key, value in stats.items():
                    f.write(f"{key}: {value}\n")
                
                f.write("\nCRITICAL ISSUES\n")
                f.write("-" * 40 + "\n")
                for issue in self.results['critical_issues']:
                    f.write(f"• {issue}\n")
                
                f.write("\nWARNINGS\n")
                f.write("-" * 40 + "\n")
                for warning in self.results['warnings']:
                    f.write(f"• {warning}\n")
                
                f.write("\nRECOMMENDATIONS\n")
                f.write("-" * 40 + "\n")
                for rec in self.results['recommendations']:
                    f.write(f"• {rec}\n")
                
                f.write(f"\n\nFull test results JSON:\n")
                f.write(json.dumps(self.results, indent=2))
            
            logger.info(f"✅ Detailed report written to {desktop_path}")
            
        except Exception as e:
            logger.error(f"Failed to write detailed report: {e}")

def main():
    """Main execution function"""
    stress_test = SystemStressTest()
    stress_test.run_full_stress_test()

if __name__ == "__main__":
    main()