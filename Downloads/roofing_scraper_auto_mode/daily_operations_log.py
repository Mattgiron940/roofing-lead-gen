#!/usr/bin/env python3
"""
Daily Operations Logger for Async Roofing Lead Engine
Monitors performance, tracks failures, and manages operational alerts
"""

import json
import logging
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from pathlib import Path

from supabase_client import supabase

# Configure operational logging
log_dir = Path('logs')
log_dir.mkdir(exist_ok=True)

# Create daily log file
today = datetime.now().strftime('%Y%m%d')
log_file = log_dir / f"daily_operations_{today}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger('DailyOperations')

class DailyOperationsManager:
    """Manages daily operations for the Async Roofing Lead Engine"""
    
    def __init__(self):
        self.start_time = datetime.now()
        self.daily_stats = {
            'leads_scraped': 0,
            'leads_saved': 0,
            'requests_made': 0,
            'scrapers_run': [],
            'failures': [],
            'ghl_integrations': 0,
            'zapier_integrations': 0,
            'alerts_triggered': []
        }
        
        # Load configuration
        self.daily_limit = int(os.getenv('DAILY_LEAD_LIMIT', '10000'))
        self.alert_threshold = int(os.getenv('ALERT_THRESHOLD', '3000'))
        self.monthly_target = int(os.getenv('MONTHLY_LEAD_TARGET', '50000'))
        
        logger.info("🚀 Daily Operations Manager initialized")
        logger.info(f"📊 Daily Limit: {self.daily_limit:,} leads")
        logger.info(f"⚠️ Alert Threshold: {self.alert_threshold:,} leads")
        logger.info(f"🎯 Monthly Target: {self.monthly_target:,} leads")
    
    def log_scraper_start(self, scraper_name: str):
        """Log scraper startup"""
        logger.info(f"🔄 Starting {scraper_name} scraper...")
        self.daily_stats['scrapers_run'].append({
            'name': scraper_name,
            'start_time': datetime.now().isoformat(),
            'status': 'running'
        })
    
    def log_scraper_complete(self, scraper_name: str, results: Dict):
        """Log scraper completion with results"""
        
        # Update scraper status
        for scraper in self.daily_stats['scrapers_run']:
            if scraper['name'] == scraper_name:
                scraper['status'] = 'completed'
                scraper['end_time'] = datetime.now().isoformat()
                scraper['results'] = results
                break
        
        # Update daily stats
        self.daily_stats['leads_scraped'] += results.get('leads_extracted', 0)
        self.daily_stats['leads_saved'] += results.get('leads_saved', 0)
        self.daily_stats['requests_made'] += results.get('total_requests', 0)
        
        logger.info(f"✅ {scraper_name} completed:")
        logger.info(f"   • Leads Extracted: {results.get('leads_extracted', 0):,}")
        logger.info(f"   • Leads Saved: {results.get('leads_saved', 0):,}")
        logger.info(f"   • Success Rate: {results.get('success_rate_percent', 0):.1f}%")
        logger.info(f"   • Requests/Hour: {results.get('requests_per_hour', 0):,.0f}")
    
    def log_scraper_failure(self, scraper_name: str, error: str):
        """Log scraper failure"""
        
        failure_record = {
            'scraper': scraper_name,
            'error': error,
            'timestamp': datetime.now().isoformat()
        }
        
        self.daily_stats['failures'].append(failure_record)
        
        logger.error(f"❌ {scraper_name} FAILED: {error}")
        
        # Update scraper status
        for scraper in self.daily_stats['scrapers_run']:
            if scraper['name'] == scraper_name:
                scraper['status'] = 'failed'
                scraper['error'] = error
                break
    
    def check_daily_targets(self):
        """Check daily targets and trigger alerts if needed"""
        
        current_leads = self.daily_stats['leads_saved']
        
        # Check if below alert threshold
        if current_leads < self.alert_threshold:
            alert = {
                'type': 'LOW_LEADS',
                'message': f"Daily leads ({current_leads:,}) below threshold ({self.alert_threshold:,})",
                'timestamp': datetime.now().isoformat(),
                'severity': 'HIGH'
            }
            self.daily_stats['alerts_triggered'].append(alert)
            logger.warning(f"⚠️ ALERT: {alert['message']}")
        
        # Check monthly pace
        today = datetime.now()
        days_in_month = today.replace(day=28).day  # Approximate
        daily_target_for_monthly = self.monthly_target / days_in_month
        
        if current_leads < daily_target_for_monthly * 0.8:  # 80% of target
            alert = {
                'type': 'MONTHLY_PACE_LOW',
                'message': f"Behind monthly pace: {current_leads:,} vs target {daily_target_for_monthly:.0f}",
                'timestamp': datetime.now().isoformat(),
                'severity': 'MEDIUM'
            }
            self.daily_stats['alerts_triggered'].append(alert)
            logger.warning(f"⚠️ MONTHLY PACE ALERT: {alert['message']}")
    
    def log_integration_status(self):
        """Log integration status for GHL and Zapier"""
        
        ghl_key = os.getenv('GHL_API_KEY', '')
        zapier_webhook = os.getenv('ZAPIER_WEBHOOK_URL', '')
        
        if 'pending' in ghl_key.lower():
            logger.warning("🟡 GHL Integration: PENDING - Awaiting API keys")
        else:
            logger.info("✅ GHL Integration: CONFIGURED")
        
        if 'pending' in zapier_webhook.lower():
            logger.warning("🟡 Zapier Integration: PENDING - Awaiting webhook URLs")
        else:
            logger.info("✅ Zapier Integration: CONFIGURED")
    
    def check_scraper_concurrency(self):
        """Check if scrapers are maintaining expected concurrency"""
        
        expected_concurrent = int(os.getenv('MAX_CONCURRENT_REQUESTS', '390'))
        
        # This would be implemented with actual monitoring of scraper performance
        # For now, log the expected concurrency
        logger.info(f"🔧 Expected Total Concurrency: {expected_concurrent} requests")
        
        # Alert if any scraper reports significantly lower performance
        for scraper in self.daily_stats['scrapers_run']:
            if scraper.get('status') == 'completed' and 'results' in scraper:
                req_per_hour = scraper['results'].get('requests_per_hour', 0)
                scraper_name = scraper['name']
                
                # Define minimum expected performance per scraper
                min_performance = {
                    'zillow': 1200,
                    'redfin': 1100,
                    'cad': 900,
                    'permit': 1000
                }
                
                min_expected = min_performance.get(scraper_name.replace('async_', '').replace('_scraper', ''), 800)
                
                if req_per_hour < min_expected:
                    alert = {
                        'type': 'LOW_CONCURRENCY',
                        'message': f"{scraper_name} below expected performance: {req_per_hour:.0f} vs {min_expected}",
                        'timestamp': datetime.now().isoformat(),
                        'severity': 'MEDIUM'
                    }
                    self.daily_stats['alerts_triggered'].append(alert)
                    logger.warning(f"⚠️ CONCURRENCY ALERT: {alert['message']}")
    
    def save_daily_report(self):
        """Save daily operations report to Supabase and local file"""
        
        # Calculate runtime
        runtime = (datetime.now() - self.start_time).total_seconds()
        
        # Create comprehensive daily report
        daily_report = {
            'date': datetime.now().strftime('%Y-%m-%d'),
            'runtime_seconds': runtime,
            'daily_stats': self.daily_stats,
            'configuration': {
                'daily_limit': self.daily_limit,
                'alert_threshold': self.alert_threshold,
                'monthly_target': self.monthly_target
            },
            'performance_summary': {
                'total_leads_scraped': self.daily_stats['leads_scraped'],
                'total_leads_saved': self.daily_stats['leads_saved'],
                'total_requests': self.daily_stats['requests_made'],
                'success_rate': (self.daily_stats['leads_saved'] / max(self.daily_stats['leads_scraped'], 1)) * 100,
                'scrapers_successful': len([s for s in self.daily_stats['scrapers_run'] if s.get('status') == 'completed']),
                'scrapers_failed': len([s for s in self.daily_stats['scrapers_run'] if s.get('status') == 'failed']),
                'alerts_triggered': len(self.daily_stats['alerts_triggered'])
            }
        }
        
        # Save to local file
        report_file = log_dir / f"daily_report_{datetime.now().strftime('%Y%m%d')}.json"
        try:
            with open(report_file, 'w') as f:
                json.dump(daily_report, f, indent=2, default=str)
            logger.info(f"📊 Daily report saved to {report_file}")
        except Exception as e:
            logger.error(f"❌ Failed to save daily report: {e}")
        
        # Try to save to Supabase report_logs table
        try:
            # This assumes a report_logs table exists in Supabase
            supabase.table('report_logs').insert({
                'report_date': datetime.now().strftime('%Y-%m-%d'),
                'report_type': 'daily_operations',
                'report_data': daily_report,
                'created_at': datetime.now().isoformat()
            }).execute()
            logger.info("📈 Daily report uploaded to Supabase report_logs")
        except Exception as e:
            logger.warning(f"⚠️ Could not save to Supabase report_logs: {e}")
        
        return daily_report
    
    def print_daily_summary(self):
        """Print formatted daily summary"""
        
        print("\n" + "="*70)
        print("📊 DAILY OPERATIONS SUMMARY")
        print("="*70)
        
        # Performance metrics
        print(f"📅 Date: {datetime.now().strftime('%Y-%m-%d')}")
        print(f"⏱️ Runtime: {(datetime.now() - self.start_time).total_seconds()/3600:.1f} hours")
        print(f"🎯 Leads Scraped: {self.daily_stats['leads_scraped']:,}")
        print(f"💾 Leads Saved: {self.daily_stats['leads_saved']:,}")
        print(f"🔄 Total Requests: {self.daily_stats['requests_made']:,}")
        
        # Target achievement
        target_percentage = (self.daily_stats['leads_saved'] / self.daily_limit) * 100
        print(f"📈 Daily Target: {target_percentage:.1f}% ({self.daily_stats['leads_saved']:,}/{self.daily_limit:,})")
        
        # Scraper breakdown
        print(f"\n🔍 SCRAPER PERFORMANCE:")
        for scraper in self.daily_stats['scrapers_run']:
            name = scraper['name']
            status = scraper.get('status', 'unknown')
            
            if status == 'completed' and 'results' in scraper:
                results = scraper['results']
                leads = results.get('leads_saved', 0)
                req_hour = results.get('requests_per_hour', 0)
                print(f"   • {name}: ✅ {leads:,} leads, {req_hour:,.0f} req/hr")
            elif status == 'failed':
                error = scraper.get('error', 'Unknown error')
                print(f"   • {name}: ❌ FAILED - {error}")
            else:
                print(f"   • {name}: 🔄 {status}")
        
        # Integration status
        print(f"\n🔗 INTEGRATIONS:")
        ghl_status = "🟡 PENDING" if 'pending' in os.getenv('GHL_API_KEY', '').lower() else "✅ ACTIVE"
        zapier_status = "🟡 PENDING" if 'pending' in os.getenv('ZAPIER_WEBHOOK_URL', '').lower() else "✅ ACTIVE"
        print(f"   • GHL: {ghl_status}")
        print(f"   • Zapier: {zapier_status}")
        
        # Alerts
        if self.daily_stats['alerts_triggered']:
            print(f"\n⚠️ ALERTS ({len(self.daily_stats['alerts_triggered'])}):")
            for alert in self.daily_stats['alerts_triggered']:
                severity_icon = "🔴" if alert['severity'] == 'HIGH' else "🟡"
                print(f"   • {severity_icon} {alert['type']}: {alert['message']}")
        else:
            print(f"\n✅ NO ALERTS - All systems operating normally")
        
        print("="*70)

# Daily operations manager instance
daily_ops = DailyOperationsManager()

if __name__ == "__main__":
    # Test the daily operations manager
    daily_ops.log_integration_status()
    daily_ops.check_daily_targets()
    daily_ops.print_daily_summary()
    daily_ops.save_daily_report()