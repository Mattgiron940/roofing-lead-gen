#!/usr/bin/env python3
"""
DFW Lead Limit Controller - Daily Lead Management System
Manages daily lead processing limits across all scrapers to ensure 3,000 leads/day cap
Provides thread-safe tracking, persistence, and reset functionality
"""

import os
import json
import threading
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple
import logging

logger = logging.getLogger(__name__)

class LeadLimitController:
    """Thread-safe daily lead limit controller for all scrapers"""
    
    def __init__(self, daily_limit: int = 3000):
        self.daily_limit = int(os.getenv('DAILY_LEAD_LIMIT', daily_limit))
        self.lock = threading.Lock()
        self.data_file = 'lead_limits.json'
        
        # Initialize counters
        self.today = datetime.now().strftime('%Y-%m-%d')
        self.total_processed_today = 0
        self.scraper_counts = {
            'redfin_scraper': 0,
            'texas_cad_scraper': 0,
            'permit_scraper': 0,
            'storm_integration': 0
        }
        
        # Load existing data if available
        self.load_daily_data()
        
        logger.info(f"🎯 Lead Limit Controller initialized: {self.total_processed_today}/{self.daily_limit} leads processed today")
    
    def load_daily_data(self):
        """Load existing daily lead data from file"""
        try:
            if os.path.exists(self.data_file):
                with open(self.data_file, 'r') as f:
                    data = json.load(f)
                    
                # Check if data is from today
                if data.get('date') == self.today:
                    self.total_processed_today = data.get('total_processed', 0)
                    self.scraper_counts = data.get('scraper_counts', self.scraper_counts)
                    logger.info(f"📊 Loaded existing daily data: {self.total_processed_today} leads processed")
                else:
                    logger.info(f"🔄 New day detected, resetting counters (previous date: {data.get('date')})")
                    self.reset_daily_counters()
            else:
                logger.info("📂 No existing lead data file found, starting fresh")
                self.save_daily_data()
                
        except Exception as e:
            logger.error(f"Error loading daily data: {e}")
            self.reset_daily_counters()
    
    def save_daily_data(self):
        """Save current daily lead data to file"""
        try:
            data = {
                'date': self.today,
                'total_processed': self.total_processed_today,
                'scraper_counts': self.scraper_counts,
                'daily_limit': self.daily_limit,
                'last_updated': datetime.now().isoformat()
            }
            
            with open(self.data_file, 'w') as f:
                json.dump(data, f, indent=2)
                
        except Exception as e:
            logger.error(f"Error saving daily data: {e}")
    
    def reset_daily_counters(self):
        """Reset all counters for a new day"""
        with self.lock:
            self.total_processed_today = 0
            self.scraper_counts = {scraper: 0 for scraper in self.scraper_counts}
            self.save_daily_data()
            logger.info("🔄 Daily lead counters reset")
    
    def check_daily_reset(self):
        """Check if we need to reset counters for a new day"""
        current_date = datetime.now().strftime('%Y-%m-%d')
        if current_date != self.today:
            logger.info(f"📅 Date changed from {self.today} to {current_date}, resetting counters")
            self.today = current_date
            self.reset_daily_counters()
    
    def can_process_lead(self, scraper_name: str) -> bool:
        """Check if a scraper can process another lead without exceeding daily limit"""
        with self.lock:
            self.check_daily_reset()
            return self.total_processed_today < self.daily_limit
    
    def increment_lead_count(self, scraper_name: str, is_dfw: bool = True) -> bool:
        """
        Increment lead count for a scraper if within daily limit
        Returns True if lead was counted, False if limit exceeded
        """
        with self.lock:
            self.check_daily_reset()
            
            # Only count DFW leads towards the limit
            if not is_dfw:
                return True
            
            if self.total_processed_today >= self.daily_limit:
                logger.warning(f"⚠️ Daily limit of {self.daily_limit} leads reached, cannot process more")
                return False
            
            # Increment counters
            self.total_processed_today += 1
            if scraper_name in self.scraper_counts:
                self.scraper_counts[scraper_name] += 1
            else:
                self.scraper_counts[scraper_name] = 1
            
            # Save updated data
            self.save_daily_data()
            
            logger.debug(f"📈 {scraper_name}: {self.scraper_counts.get(scraper_name, 0)} leads | Total: {self.total_processed_today}/{self.daily_limit}")
            
            return True
    
    def get_current_stats(self) -> Dict:
        """Get current lead processing statistics"""
        with self.lock:
            self.check_daily_reset()
            
            return {
                'date': self.today,
                'total_processed_today': self.total_processed_today,
                'daily_limit': self.daily_limit,
                'remaining_capacity': self.daily_limit - self.total_processed_today,
                'utilization_percentage': round((self.total_processed_today / self.daily_limit) * 100, 2),
                'scraper_counts': self.scraper_counts.copy(),
                'limit_reached': self.total_processed_today >= self.daily_limit
            }
    
    def get_scraper_allocation(self) -> Dict[str, int]:
        """Get recommended lead allocation per scraper"""
        remaining = self.daily_limit - self.total_processed_today
        scrapers = list(self.scraper_counts.keys())
        
        if remaining <= 0:
            return {scraper: 0 for scraper in scrapers}
        
        # Allocate remaining leads proportionally
        allocation = {}
        per_scraper = remaining // len(scrapers)
        extra = remaining % len(scrapers)
        
        for i, scraper in enumerate(scrapers):
            allocation[scraper] = per_scraper + (1 if i < extra else 0)
        
        return allocation
    
    def log_daily_summary(self):
        """Log comprehensive daily summary"""
        stats = self.get_current_stats()
        
        logger.info("📊 DAILY LEAD PROCESSING SUMMARY")
        logger.info("=" * 50)
        logger.info(f"📅 Date: {stats['date']}")
        logger.info(f"🎯 Total Processed: {stats['total_processed_today']}/{stats['daily_limit']} ({stats['utilization_percentage']}%)")
        logger.info(f"📈 Remaining Capacity: {stats['remaining_capacity']} leads")
        
        if stats['scraper_counts']:
            logger.info("🤖 Scraper Breakdown:")
            for scraper, count in stats['scraper_counts'].items():
                percentage = round((count / stats['total_processed_today']) * 100, 1) if stats['total_processed_today'] > 0 else 0
                logger.info(f"   • {scraper}: {count} leads ({percentage}%)")
        
        if stats['limit_reached']:
            logger.warning("⚠️ DAILY LIMIT REACHED - No more leads can be processed today")
        else:
            logger.info(f"✅ System operational - {stats['remaining_capacity']} leads remaining")
    
    def export_daily_report(self, filename: Optional[str] = None) -> str:
        """Export daily report to JSON file"""
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'daily_lead_report_{timestamp}.json'
        
        stats = self.get_current_stats()
        
        # Enhanced report with additional metrics
        report = {
            **stats,
            'generated_at': datetime.now().isoformat(),
            'scraper_allocation': self.get_scraper_allocation(),
            'performance_metrics': {
                'efficiency_score': min(stats['utilization_percentage'] / 80 * 100, 100),  # Target 80% utilization
                'capacity_remaining_hours': 24 - datetime.now().hour if stats['remaining_capacity'] > 0 else 0,
                'average_leads_per_hour': round(stats['total_processed_today'] / max(datetime.now().hour, 1), 2)
            }
        }
        
        try:
            with open(filename, 'w') as f:
                json.dump(report, f, indent=2)
            
            logger.info(f"📋 Daily report exported to: {filename}")
            return filename
            
        except Exception as e:
            logger.error(f"Error exporting daily report: {e}")
            return ""

# Global instance
lead_controller = LeadLimitController()

def can_process_lead(scraper_name: str) -> bool:
    """Convenience function to check if scraper can process another lead"""
    return lead_controller.can_process_lead(scraper_name)

def increment_lead_count(scraper_name: str, is_dfw: bool = True) -> bool:
    """Convenience function to increment lead count"""
    return lead_controller.increment_lead_count(scraper_name, is_dfw)

def get_daily_stats() -> Dict:
    """Convenience function to get current daily statistics"""
    return lead_controller.get_current_stats()

def export_daily_report() -> str:
    """Convenience function to export daily report"""
    return lead_controller.export_daily_report()

def log_daily_summary():
    """Convenience function to log daily summary"""
    lead_controller.log_daily_summary()

if __name__ == "__main__":
    # Test the lead limit controller
    controller = LeadLimitController(daily_limit=100)  # Small limit for testing
    
    # Simulate lead processing
    scrapers = ['redfin_scraper', 'texas_cad_scraper', 'permit_scraper', 'storm_integration']
    
    for i in range(25):
        scraper = scrapers[i % len(scrapers)]
        is_dfw = i % 3 == 0  # Every 3rd lead is DFW
        
        success = controller.increment_lead_count(scraper, is_dfw)
        if not success:
            print(f"Lead {i+1} rejected - daily limit reached")
            break
        else:
            print(f"Lead {i+1} processed by {scraper} (DFW: {is_dfw})")
    
    # Show final stats
    controller.log_daily_summary()
    report_file = controller.export_daily_report()
    print(f"Report exported to: {report_file}")