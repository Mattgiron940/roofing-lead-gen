#!/usr/bin/env python3
"""
Webhook Integration for Zapier/HighLevel/Make.com
Automated lead distribution with duplicate prevention
"""

import os
import json
import time
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import logging
from dataclasses import dataclass
from supabase_config import SupabaseConnection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class WebhookConfig:
    name: str
    url: str
    enabled: bool = True
    headers: Dict[str, str] = None
    retry_attempts: int = 3
    timeout: int = 30

class WebhookManager:
    def __init__(self):
        self.supabase_conn = SupabaseConnection()
        self.webhooks = self.load_webhook_configs()
        self.processed_leads = set()  # Track processed leads to avoid duplicates
        
    def load_webhook_configs(self) -> List[WebhookConfig]:
        """Load webhook configurations from environment variables"""
        webhooks = []
        
        # GoHighLevel webhook
        ghl_webhook = os.getenv('GHL_WEBHOOK_URL')
        if ghl_webhook:
            webhooks.append(WebhookConfig(
                name="GoHighLevel",
                url=ghl_webhook,
                headers={
                    'Content-Type': 'application/json',
                    'Authorization': f"Bearer {os.getenv('GHL_API_KEY', '')}"
                }
            ))
        
        # Zapier webhook
        zapier_webhook = os.getenv('ZAPIER_WEBHOOK_URL')
        if zapier_webhook:
            webhooks.append(WebhookConfig(
                name="Zapier",
                url=zapier_webhook,
                headers={'Content-Type': 'application/json'}
            ))
        
        # Make.com webhook
        make_webhook = os.getenv('MAKE_WEBHOOK_URL')
        if make_webhook:
            webhooks.append(WebhookConfig(
                name="Make.com",
                url=make_webhook,
                headers={'Content-Type': 'application/json'}
            ))
        
        # Custom webhook
        custom_webhook = os.getenv('CUSTOM_WEBHOOK_URL')
        if custom_webhook:
            webhooks.append(WebhookConfig(
                name="Custom",
                url=custom_webhook,
                headers={
                    'Content-Type': 'application/json',
                    'X-API-Key': os.getenv('CUSTOM_API_KEY', '')
                }
            ))
        
        logger.info(f"Loaded {len(webhooks)} webhook configurations")
        return webhooks
    
    def get_new_leads(self, minutes_back: int = 5) -> List[Dict[str, Any]]:
        """Get leads created in the last N minutes"""
        if not self.supabase_conn.supabase:
            logger.error("Supabase not available")
            return []
        
        cutoff_time = (datetime.now() - timedelta(minutes=minutes_back)).isoformat()
        
        # Tables to monitor
        tables = ['zillow_leads', 'redfin_leads', 'cad_leads', 'permit_leads']
        
        all_new_leads = []
        
        for table in tables:
            try:
                result = self.supabase_conn.supabase.table(table)\
                    .select("*")\
                    .gte('created_at', cutoff_time)\
                    .order('created_at', desc=True)\
                    .execute()
                
                if result.data:
                    for lead in result.data:
                        # Add source information
                        lead['source_table'] = table
                        lead['source_type'] = table.replace('_leads', '')
                        
                        # Create unique identifier to prevent duplicates
                        lead_id = f"{table}_{lead['id']}"
                        if lead_id not in self.processed_leads:
                            all_new_leads.append(lead)
                            self.processed_leads.add(lead_id)
                
                logger.debug(f"Found {len(result.data) if result.data else 0} new leads in {table}")
                
            except Exception as e:
                logger.error(f"Error fetching leads from {table}: {e}")
        
        logger.info(f"📊 Found {len(all_new_leads)} new leads to process")
        return all_new_leads
    
    def format_lead_for_webhook(self, lead: Dict[str, Any]) -> Dict[str, Any]:
        """Format lead data for webhook consumption"""
        # Standardize field names across different lead types
        formatted_lead = {
            'id': lead.get('id'),
            'source': lead.get('source_type', 'unknown'),
            'created_at': lead.get('created_at'),
            'updated_at': lead.get('updated_at'),
            
            # Contact information
            'address': lead.get('address_text', ''),
            'city': lead.get('city', ''),
            'state': lead.get('state', 'TX'),
            'zip_code': lead.get('zip_code', ''),
            'county': lead.get('county', ''),
            
            # Property details
            'price': lead.get('price', 0) or lead.get('appraised_value', 0),
            'property_type': lead.get('property_type', ''),
            'year_built': lead.get('year_built'),
            'square_feet': lead.get('square_feet'),
            
            # Lead scoring
            'lead_score': lead.get('lead_score', 0) or lead.get('lead_priority', 0),
            'priority': self.get_priority_label(lead.get('lead_score', 0) or lead.get('lead_priority', 0)),
            
            # Source-specific data
            'source_url': lead.get('zillow_url') or lead.get('redfin_url') or lead.get('cad_url', ''),
            'mls_number': lead.get('mls_number', ''),
            'account_number': lead.get('account_number', ''),
            'permit_id': lead.get('permit_id', ''),
            'owner_name': lead.get('owner_name', ''),
            
            # Additional context
            'storm_affected': lead.get('storm_affected', False),
            'homestead_exemption': lead.get('homestead_exemption', False),
            
            # Metadata for tracking
            'webhook_timestamp': datetime.now().isoformat(),
            'lead_unique_id': f"{lead.get('source_type', 'unknown')}_{lead.get('id')}"
        }
        
        # Add permit-specific fields
        if lead.get('source_type') == 'permit':
            formatted_lead.update({
                'permit_type': lead.get('permit_type', ''),
                'work_description': lead.get('work_description', ''),
                'permit_value': lead.get('permit_value', ''),
                'contractor_name': lead.get('contractor_name', ''),
                'date_filed': lead.get('date_filed', ''),
                'status': lead.get('status', '')
            })
        
        return formatted_lead
    
    def get_priority_label(self, score: int) -> str:
        """Convert numeric lead score to priority label"""
        if score >= 8:
            return "HIGH"
        elif score >= 6:
            return "MEDIUM"
        else:
            return "LOW"
    
    def send_webhook(self, webhook: WebhookConfig, lead_data: Dict[str, Any]) -> bool:
        """Send lead data to webhook endpoint"""
        if not webhook.enabled:
            logger.debug(f"Webhook {webhook.name} is disabled")
            return False
        
        for attempt in range(webhook.retry_attempts):
            try:
                headers = webhook.headers or {}
                
                response = requests.post(
                    webhook.url,
                    json=lead_data,
                    headers=headers,
                    timeout=webhook.timeout
                )
                
                if response.status_code in [200, 201, 202]:
                    logger.info(f"✅ Successfully sent lead {lead_data['lead_unique_id']} to {webhook.name}")
                    return True
                else:
                    logger.warning(f"⚠️ Webhook {webhook.name} returned status {response.status_code}: {response.text}")
                    
            except requests.exceptions.Timeout:
                logger.warning(f"⏰ Webhook {webhook.name} timed out (attempt {attempt + 1}/{webhook.retry_attempts})")
            except requests.exceptions.ConnectionError:
                logger.warning(f"🔌 Connection error to {webhook.name} (attempt {attempt + 1}/{webhook.retry_attempts})")
            except Exception as e:
                logger.error(f"❌ Error sending to {webhook.name}: {e}")
            
            if attempt < webhook.retry_attempts - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
        
        logger.error(f"❌ Failed to send lead to {webhook.name} after {webhook.retry_attempts} attempts")
        return False
    
    def process_leads(self, leads: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Process and send leads to all configured webhooks"""
        if not leads:
            return {'processed': 0, 'successful': 0, 'failed': 0}
        
        stats = {
            'processed': len(leads),
            'successful': 0,
            'failed': 0,
            'webhook_results': {}
        }
        
        for webhook in self.webhooks:
            stats['webhook_results'][webhook.name] = {'sent': 0, 'failed': 0}
        
        for lead in leads:
            formatted_lead = self.format_lead_for_webhook(lead)
            lead_sent_successfully = False
            
            for webhook in self.webhooks:
                success = self.send_webhook(webhook, formatted_lead)
                
                if success:
                    stats['webhook_results'][webhook.name]['sent'] += 1
                    lead_sent_successfully = True
                else:
                    stats['webhook_results'][webhook.name]['failed'] += 1
            
            if lead_sent_successfully:
                stats['successful'] += 1
            else:
                stats['failed'] += 1
        
        return stats
    
    def run_continuous_monitoring(self, check_interval: int = 60):
        """Run continuous monitoring for new leads"""
        logger.info(f"🔄 Starting continuous lead monitoring (checking every {check_interval}s)")
        
        while True:
            try:
                # Get new leads
                new_leads = self.get_new_leads(minutes_back=check_interval//60 + 1)
                
                if new_leads:
                    logger.info(f"📨 Processing {len(new_leads)} new leads")
                    stats = self.process_leads(new_leads)
                    
                    logger.info(f"✅ Processed: {stats['processed']}, Successful: {stats['successful']}, Failed: {stats['failed']}")
                    
                    # Log webhook-specific results
                    for webhook_name, results in stats['webhook_results'].items():
                        logger.info(f"   {webhook_name}: {results['sent']} sent, {results['failed']} failed")
                
                # Wait for next check
                time.sleep(check_interval)
                
            except KeyboardInterrupt:
                logger.info("🛑 Monitoring stopped by user")
                break
            except Exception as e:
                logger.error(f"❌ Error in monitoring loop: {e}")
                time.sleep(check_interval)
    
    def test_webhooks(self) -> Dict[str, bool]:
        """Test all configured webhooks with sample data"""
        logger.info("🧪 Testing webhook configurations")
        
        sample_lead = {
            'id': 'test_123',
            'source': 'test',
            'created_at': datetime.now().isoformat(),
            'address': '123 Test Street, Dallas, TX 75201',
            'city': 'Dallas',
            'state': 'TX',
            'zip_code': '75201',
            'price': 350000,
            'lead_score': 8,
            'priority': 'HIGH',
            'property_type': 'Single Family Residence',
            'lead_unique_id': 'test_123',
            'webhook_timestamp': datetime.now().isoformat(),
            'test_mode': True
        }
        
        results = {}
        
        for webhook in self.webhooks:
            logger.info(f"Testing {webhook.name}...")
            success = self.send_webhook(webhook, sample_lead)
            results[webhook.name] = success
        
        return results

class SupabaseWebhookTrigger:
    """Create Supabase database triggers for real-time webhook notifications"""
    
    def __init__(self):
        self.supabase_conn = SupabaseConnection()
    
    def create_webhook_trigger_function(self) -> str:
        """SQL function for webhook triggers"""
        return """
        CREATE OR REPLACE FUNCTION notify_webhook_on_insert()
        RETURNS TRIGGER AS $$
        BEGIN
            -- Notify webhook service about new lead
            PERFORM pg_notify('new_lead', json_build_object(
                'table', TG_TABLE_NAME,
                'id', NEW.id,
                'created_at', NEW.created_at
            )::text);
            
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    
    def create_triggers_sql(self) -> str:
        """SQL to create triggers on all lead tables"""
        tables = ['zillow_leads', 'redfin_leads', 'cad_leads', 'permit_leads']
        
        sql_statements = [self.create_webhook_trigger_function()]
        
        for table in tables:
            trigger_sql = f"""
            DROP TRIGGER IF EXISTS webhook_trigger_{table} ON {table};
            CREATE TRIGGER webhook_trigger_{table}
                AFTER INSERT ON {table}
                FOR EACH ROW
                EXECUTE FUNCTION notify_webhook_on_insert();
            """
            sql_statements.append(trigger_sql)
        
        return '\n'.join(sql_statements)
    
    def deploy_triggers(self) -> bool:
        """Deploy webhook triggers to Supabase"""
        if not self.supabase_conn.supabase:
            logger.error("Supabase not available")
            return False
        
        try:
            sql = self.create_triggers_sql()
            # Note: This would require elevated permissions in Supabase
            logger.info("📋 Webhook trigger SQL generated")
            logger.info("To deploy, run this SQL in your Supabase SQL editor:")
            logger.info("=" * 50)
            logger.info(sql)
            logger.info("=" * 50)
            return True
            
        except Exception as e:
            logger.error(f"Error creating webhook triggers: {e}")
            return False

def main():
    """Main CLI interface"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Webhook integration for lead distribution')
    parser.add_argument('--test', action='store_true', help='Test webhook configurations')
    parser.add_argument('--monitor', action='store_true', help='Start continuous monitoring')
    parser.add_argument('--interval', type=int, default=60, help='Check interval in seconds (default: 60)')
    parser.add_argument('--generate-triggers', action='store_true', help='Generate Supabase trigger SQL')
    
    args = parser.parse_args()
    
    webhook_manager = WebhookManager()
    
    if args.test:
        logger.info("🧪 Testing webhook configurations...")
        results = webhook_manager.test_webhooks()
        
        print("\n🔗 Webhook Test Results:")
        for webhook_name, success in results.items():
            status = "✅ PASS" if success else "❌ FAIL"
            print(f"  {webhook_name}: {status}")
        
        return 0 if all(results.values()) else 1
    
    elif args.generate_triggers:
        trigger_manager = SupabaseWebhookTrigger()
        trigger_manager.deploy_triggers()
        return 0
    
    elif args.monitor:
        webhook_manager.run_continuous_monitoring(args.interval)
        return 0
    
    else:
        # One-time lead processing
        leads = webhook_manager.get_new_leads()
        if leads:
            stats = webhook_manager.process_leads(leads)
            print(f"✅ Processed {stats['processed']} leads")
            print(f"Successful: {stats['successful']}, Failed: {stats['failed']}")
        else:
            print("ℹ️ No new leads found")
        
        return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)