#!/usr/bin/env python3
"""
DFW Zapier Workflow Manager - Enhanced Integration
Manages Zapier webhook automation for DFW roofing leads with enhanced error handling,
DFW-specific filtering, and comprehensive logging integration
"""

import os
import json
import time
import requests
import logging
from datetime import datetime
from typing import Dict, List, Optional, Union
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DFWZapierWorkflowManager:
    """Enhanced Zapier workflow manager for DFW roofing lead automation"""
    
    def __init__(self):
        self.zapier_webhook_url = os.getenv("ZAPIER_WEBHOOK_URL", "")
        self.backup_webhook_url = os.getenv("ZAPIER_BACKUP_WEBHOOK_URL", "")
        self.api_timeout = int(os.getenv("ZAPIER_TIMEOUT", "15"))
        self.max_retries = int(os.getenv("ZAPIER_MAX_RETRIES", "3"))
        self.retry_delay = int(os.getenv("ZAPIER_RETRY_DELAY", "2"))
        
        # DFW-specific configuration
        self.dfw_filter_enabled = os.getenv("ZAPIER_DFW_ONLY", "true").lower() == "true"
        self.lead_score_threshold = int(os.getenv("ZAPIER_MIN_LEAD_SCORE", "6"))
        
        # Statistics tracking
        self.stats = {
            'total_attempts': 0,
            'successful_pushes': 0,
            'failed_pushes': 0,
            'dfw_leads_processed': 0,
            'filtered_out': 0,
            'last_push_timestamp': None
        }
        
        logger.info(f"🔧 DFW Zapier Workflow Manager initialized")
        logger.info(f"📊 DFW Filter: {'Enabled' if self.dfw_filter_enabled else 'Disabled'}")
        logger.info(f"🎯 Min Lead Score: {self.lead_score_threshold}")
        
    def validate_configuration(self) -> bool:
        """Validate Zapier configuration and connectivity"""
        if not self.zapier_webhook_url:
            logger.error("❌ Missing ZAPIER_WEBHOOK_URL in environment variables")
            return False
            
        try:
            # Test webhook connectivity with a ping
            test_payload = {"test": True, "timestamp": datetime.now().isoformat()}
            response = requests.post(
                self.zapier_webhook_url, 
                json=test_payload, 
                timeout=self.api_timeout,
                headers={"Content-Type": "application/json"}
            )
            
            if response.status_code == 200:
                logger.info("✅ Zapier webhook connectivity verified")
                return True
            else:
                logger.warning(f"⚠️ Zapier webhook test returned status {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Zapier webhook validation failed: {e}")
            return False
    
    def should_process_lead(self, lead_data: Dict) -> tuple[bool, str]:
        """
        Determine if lead should be sent to Zapier based on DFW filtering and quality
        Returns: (should_process, reason)
        """
        # Check if DFW filtering is enabled
        if self.dfw_filter_enabled:
            is_dfw = lead_data.get('dfw', False)
            if not is_dfw:
                return False, "Non-DFW lead filtered out"
        
        # Check lead score threshold
        lead_score = lead_data.get('lead_score', 0)
        if lead_score < self.lead_score_threshold:
            return False, f"Lead score {lead_score} below threshold {self.lead_score_threshold}"
        
        # Check required fields
        required_fields = ['address', 'city', 'state']
        missing_fields = [field for field in required_fields if not lead_data.get(field)]
        if missing_fields:
            return False, f"Missing required fields: {', '.join(missing_fields)}"
        
        return True, "Lead meets all criteria"
    
    def format_lead_for_zapier(self, lead_data: Dict) -> Dict:
        """Format lead data for Zapier webhook with DFW-specific enhancements"""
        
        formatted_lead = {
            # Basic lead information
            'lead_id': lead_data.get('id', f"lead_{int(time.time())}"),
            'source': lead_data.get('source', 'DFW_Scraper'),
            'timestamp': datetime.now().isoformat(),
            
            # Property details
            'address': lead_data.get('address', lead_data.get('address_text', '')),
            'city': lead_data.get('city', ''),
            'state': lead_data.get('state', 'TX'),
            'zip_code': lead_data.get('zip_code', lead_data.get('zipcode', '')),
            'county': lead_data.get('county', ''),
            
            # Property value and scoring
            'property_value': lead_data.get('price', lead_data.get('market_value', 0)),
            'lead_score': lead_data.get('lead_score', 5),
            'year_built': lead_data.get('year_built', ''),
            'square_feet': lead_data.get('square_feet', ''),
            
            # DFW-specific flags
            'is_dfw_lead': lead_data.get('dfw', False),
            'storm_affected': lead_data.get('storm_affected', False),
            'storm_priority': lead_data.get('storm_priority', 0),
            
            # Contact enhancement (if available)
            'property_owner': lead_data.get('property_owner', lead_data.get('owner_name', '')),
            'estimated_value': lead_data.get('price', 0),
            
            # Metadata
            'scraped_at': lead_data.get('scraped_at', datetime.now().isoformat()),
            'processing_batch': lead_data.get('batch_id', ''),
            
            # Zapier workflow routing
            'workflow_type': 'dfw_roofing_lead',
            'priority': 'high' if lead_data.get('lead_score', 0) >= 8 else 'normal',
            'follow_up_days': 1 if lead_data.get('storm_affected', False) else 3
        }
        
        return formatted_lead
    
    def push_lead_to_zapier(self, lead_data: Dict) -> bool:
        """
        Enhanced lead pushing with DFW filtering, retry logic, and comprehensive logging
        """
        self.stats['total_attempts'] += 1
        
        # Validate lead should be processed
        should_process, reason = self.should_process_lead(lead_data)
        if not should_process:
            logger.debug(f"🚫 Lead filtered out: {reason}")
            self.stats['filtered_out'] += 1
            return False
        
        # Format lead for Zapier
        formatted_lead = self.format_lead_for_zapier(lead_data)
        
        # Track DFW leads
        if formatted_lead.get('is_dfw_lead', False):
            self.stats['dfw_leads_processed'] += 1
        
        # Attempt to push with retry logic
        webhook_urls = [self.zapier_webhook_url]
        if self.backup_webhook_url:
            webhook_urls.append(self.backup_webhook_url)
        
        for webhook_url in webhook_urls:
            if self._attempt_webhook_push(webhook_url, formatted_lead):
                self.stats['successful_pushes'] += 1
                self.stats['last_push_timestamp'] = datetime.now().isoformat()
                logger.info(f"✅ Lead pushed to Zapier: {formatted_lead['lead_id']} - {formatted_lead['address']}")
                return True
        
        # All attempts failed
        self.stats['failed_pushes'] += 1
        logger.error(f"❌ Failed to push lead to Zapier after all attempts: {formatted_lead['lead_id']}")
        return False
    
    def _attempt_webhook_push(self, webhook_url: str, lead_data: Dict) -> bool:
        """Attempt to push to a specific webhook URL with retries"""
        
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "DFW-Roofing-Lead-System/1.0"
        }
        
        for attempt in range(self.max_retries):
            try:
                response = requests.post(
                    webhook_url,
                    headers=headers,
                    json=lead_data,
                    timeout=self.api_timeout
                )
                
                if response.status_code == 200:
                    logger.debug(f"✅ Webhook success on attempt {attempt + 1}")
                    return True
                else:
                    logger.warning(f"⚠️ Attempt {attempt + 1} failed with status {response.status_code}: {response.text[:200]}")
                    
            except requests.exceptions.Timeout:
                logger.warning(f"⚠️ Attempt {attempt + 1} timed out after {self.api_timeout}s")
            except requests.exceptions.ConnectionError:
                logger.warning(f"⚠️ Attempt {attempt + 1} connection error")
            except Exception as e:
                logger.warning(f"⚠️ Attempt {attempt + 1} error: {str(e)}")
            
            # Wait before retry (except on last attempt)
            if attempt < self.max_retries - 1:
                time.sleep(self.retry_delay)
        
        return False
    
    def push_leads_batch(self, leads: List[Dict]) -> Dict:
        """Push multiple leads to Zapier with batch processing"""
        
        batch_stats = {
            'total_leads': len(leads),
            'processed': 0,
            'successful': 0,
            'failed': 0,
            'filtered': 0,
            'start_time': datetime.now(),
            'end_time': None
        }
        
        logger.info(f"🚀 Starting Zapier batch processing: {len(leads)} leads")
        
        for i, lead in enumerate(leads):
            batch_stats['processed'] += 1
            
            if self.push_lead_to_zapier(lead):
                batch_stats['successful'] += 1
            else:
                # Check if it was filtered vs failed
                should_process, _ = self.should_process_lead(lead)
                if should_process:
                    batch_stats['failed'] += 1
                else:
                    batch_stats['filtered'] += 1
            
            # Log progress every 10 leads
            if (i + 1) % 10 == 0:
                logger.info(f"📊 Batch progress: {i + 1}/{len(leads)} leads processed")
        
        batch_stats['end_time'] = datetime.now()
        runtime = batch_stats['end_time'] - batch_stats['start_time']
        
        logger.info(f"✅ Zapier batch completed in {runtime.total_seconds():.2f}s")
        logger.info(f"📊 Results: {batch_stats['successful']} successful, {batch_stats['failed']} failed, {batch_stats['filtered']} filtered")
        
        return batch_stats
    
    def get_statistics(self) -> Dict:
        """Get comprehensive Zapier workflow statistics"""
        
        success_rate = 0
        if self.stats['total_attempts'] > 0:
            success_rate = (self.stats['successful_pushes'] / self.stats['total_attempts']) * 100
        
        return {
            **self.stats,
            'success_rate_percent': round(success_rate, 2),
            'configuration': {
                'webhook_configured': bool(self.zapier_webhook_url),
                'backup_webhook_configured': bool(self.backup_webhook_url),
                'dfw_filter_enabled': self.dfw_filter_enabled,
                'min_lead_score': self.lead_score_threshold,
                'max_retries': self.max_retries,
                'timeout_seconds': self.api_timeout
            }
        }
    
    def export_statistics_report(self) -> str:
        """Export Zapier statistics to JSON file"""
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'zapier_stats_{timestamp}.json'
        
        stats = self.get_statistics()
        stats['report_generated'] = datetime.now().isoformat()
        
        try:
            with open(filename, 'w') as f:
                json.dump(stats, f, indent=2)
            
            logger.info(f"📋 Zapier statistics exported: {filename}")
            return filename
            
        except Exception as e:
            logger.error(f"❌ Failed to export Zapier statistics: {e}")
            return ""

# Global instance
zapier_manager = DFWZapierWorkflowManager()

# Convenience functions for backward compatibility
def push_lead_to_zapier(lead_data: dict) -> bool:
    """Legacy function - now routes to enhanced manager"""
    return zapier_manager.push_lead_to_zapier(lead_data)

def log_message(message: str):
    """Legacy function - now uses proper logging"""
    logger.info(message)

# Enhanced convenience functions
def push_leads_batch(leads: List[Dict]) -> Dict:
    """Convenience function to push batch of leads to Zapier"""
    return zapier_manager.push_leads_batch(leads)

def get_zapier_statistics() -> Dict:
    """Convenience function to get Zapier statistics"""
    return zapier_manager.get_statistics()

def validate_zapier_config() -> bool:
    """Convenience function to validate Zapier configuration"""
    return zapier_manager.validate_configuration()

if __name__ == "__main__":
    # Test the Zapier workflow manager
    manager = DFWZapierWorkflowManager()
    
    # Validate configuration
    if manager.validate_configuration():
        print("✅ Zapier configuration validated")
        
        # Test lead data
        test_lead = {
            'id': 'test_001',
            'address': '123 Test St, Dallas, TX 75201',
            'city': 'Dallas',
            'state': 'TX',
            'zip_code': '75201',
            'county': 'Dallas County',
            'price': 350000,
            'lead_score': 8,
            'dfw': True,
            'storm_affected': True,
            'source': 'test_scraper'
        }
        
        # Test single lead push  
        success = manager.push_lead_to_zapier(test_lead)
        print(f"Test lead push: {'✅ Success' if success else '❌ Failed'}")
        
        # Show statistics
        stats = manager.get_statistics()
        print(f"📊 Statistics: {stats}")
        
    else:
        print("❌ Zapier configuration validation failed")