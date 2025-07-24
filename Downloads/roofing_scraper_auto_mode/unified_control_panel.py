#!/usr/bin/env python3
"""
Unified Multi-Source Roofing Lead Control Panel
Orchestrates Zillow, Redfin, Permits, CAD, and Storm data scraping
Unified CSV output + Google Sheets integration + GitHub Actions ready
"""

import asyncio
import csv
import json
import os
import time
from datetime import datetime
from typing import List, Dict, Any
import logging

# Import all our scrapers
from scrape import DFWZillowScraper
from redfin_scraper import DFWRedfinScraper  
from permit_scraper import DFWPermitScraper
from texas_cad_scraper import TexasCADScraper
from storm_integration import integrate_storm_data_with_properties
from save_to_csv import save_results_to_csv
from supabase_config import SupabaseConnection

# Google Sheets API (basic setup)
try:
    import gspread
    from google.oauth2.service_account import Credentials
    SHEETS_AVAILABLE = True
except ImportError:
    SHEETS_AVAILABLE = False
    logging.warning("Google Sheets integration not available - install gspread and google-auth")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class UnifiedLeadControlPanel:
    def __init__(self):
        self.all_leads = []
        self.source_stats = {}
        self.unified_stats = {}
        
        # Initialize Supabase connection
        self.supabase_conn = SupabaseConnection()
        
        # Configuration
        self.config = {
            'enable_zillow': True,
            'enable_redfin': True, 
            'enable_permits': True,
            'enable_cad': True,
            'enable_storm_integration': True,
            'enable_sheets_push': SHEETS_AVAILABLE,
            'storm_lookback_days': 90,
            'max_leads_per_source': 50  # Limit for GitHub Actions
        }
        
        # Google Sheets setup
        self.sheets_client = None
        self.setup_sheets_client()

    def setup_sheets_client(self):
        """Setup Google Sheets client"""
        if not SHEETS_AVAILABLE:
            return
        
        try:
            # Look for service account credentials
            creds_file = os.environ.get('GOOGLE_SHEETS_CREDENTIALS', 'service_account.json')
            if os.path.exists(creds_file):
                scopes = [
                    'https://www.googleapis.com/auth/spreadsheets',
                    'https://www.googleapis.com/auth/drive'
                ]
                creds = Credentials.from_service_account_file(creds_file, scopes=scopes)
                self.sheets_client = gspread.authorize(creds)
                logger.info("✅ Google Sheets client initialized")
            else:
                logger.warning(f"Google Sheets credentials not found at {creds_file}")
        except Exception as e:
            logger.error(f"Failed to setup Google Sheets: {e}")

    def run_zillow_scraper(self) -> List[Dict]:
        """Run Zillow scraper"""
        if not self.config['enable_zillow']:
            return []
        
        logger.info("🏠 Running Zillow Scraper...")
        try:
            zillow_scraper = DFWZillowScraper()
            properties = zillow_scraper.scrape_dfw_properties()
            
            # Add source identifier
            for prop in properties:
                prop['data_source'] = 'Zillow'
                prop['source_priority'] = 1  # High priority
            
            self.source_stats['zillow'] = {
                'count': len(properties),
                'avg_price': sum(p.get('price', 0) for p in properties) / len(properties) if properties else 0,
                'status': 'success'
            }
            
            logger.info(f"✅ Zillow: {len(properties)} properties")
            return properties[:self.config['max_leads_per_source']]
            
        except Exception as e:
            logger.error(f"❌ Zillow scraper failed: {e}")
            self.source_stats['zillow'] = {'count': 0, 'status': 'failed', 'error': str(e)}
            return []

    def run_redfin_scraper(self) -> List[Dict]:
        """Run Redfin scraper"""
        if not self.config['enable_redfin']:
            return []
        
        logger.info("🏘️ Running Redfin Scraper...")
        try:
            redfin_scraper = DFWRedfinScraper()
            properties = redfin_scraper.scrape_dfw_redfin_properties()
            
            # Add source identifier
            for prop in properties:
                prop['data_source'] = 'Redfin'
                prop['source_priority'] = 1
            
            self.source_stats['redfin'] = {
                'count': len(properties),
                'avg_price': sum(p.get('price', 0) for p in properties) / len(properties) if properties else 0,
                'status': 'success'
            }
            
            logger.info(f"✅ Redfin: {len(properties)} properties")
            return properties[:self.config['max_leads_per_source']]
            
        except Exception as e:
            logger.error(f"❌ Redfin scraper failed: {e}")
            self.source_stats['redfin'] = {'count': 0, 'status': 'failed', 'error': str(e)}
            return []

    def run_permit_scraper(self) -> List[Dict]:
        """Run permit scraper"""
        if not self.config['enable_permits']:
            return []
        
        logger.info("🏗️ Running Permit Scraper...")
        try:
            permit_scraper = DFWPermitScraper()
            permits = permit_scraper.scrape_all_permits()
            
            # Convert permits to unified format
            unified_permits = []
            for permit in permits:
                unified_permit = {
                    'address': permit.get('address', ''),
                    'city': permit.get('city', ''),
                    'zipcode': permit.get('zipcode', ''),
                    'county': f"{permit.get('city', '')} County",
                    'price': permit.get('permit_value', 0),
                    'permit_type': permit.get('permit_type', ''),
                    'permit_id': permit.get('permit_id', ''),
                    'date_filed': permit.get('date_filed', ''),
                    'contractor': permit.get('contractor', ''),
                    'lead_score': permit.get('lead_priority', 5),
                    'data_source': 'Permits',
                    'source_priority': 3,  # Very high priority - active roofing work
                    'scraped_at': permit.get('scraped_at', '')
                }
                unified_permits.append(unified_permit)
            
            self.source_stats['permits'] = {
                'count': len(permits),
                'avg_value': sum(p.get('permit_value', 0) for p in permits) / len(permits) if permits else 0,
                'status': 'success'
            }
            
            logger.info(f"✅ Permits: {len(permits)} permits")
            return unified_permits[:self.config['max_leads_per_source']]
            
        except Exception as e:
            logger.error(f"❌ Permit scraper failed: {e}")
            self.source_stats['permits'] = {'count': 0, 'status': 'failed', 'error': str(e)}
            return []

    def run_cad_scraper(self) -> List[Dict]:
        """Run CAD scraper"""
        if not self.config['enable_cad']:
            return []
        
        logger.info("🏛️ Running CAD Scraper...")
        try:
            cad_scraper = TexasCADScraper()
            properties = cad_scraper.scrape_all_texas_cads()
            
            # Convert CAD data to unified format
            unified_cad = []
            for prop in properties:
                unified_prop = {
                    'address': prop.get('property_address', ''),
                    'city': prop.get('city', ''),
                    'zipcode': prop.get('zipcode', ''),
                    'county': prop.get('county', ''),
                    'price': prop.get('appraised_value', 0),
                    'owner_name': prop.get('owner_name', ''),
                    'year_built': prop.get('year_built', ''),
                    'square_feet': prop.get('square_feet', ''),
                    'property_type': prop.get('property_type', ''),
                    'last_sale_price': prop.get('last_sale_price', 0),
                    'homestead_exemption': prop.get('homestead_exemption', False),
                    'lead_score': prop.get('lead_score', 5),
                    'data_source': 'CAD',
                    'source_priority': 2,  # High priority - direct owner info
                    'scraped_at': prop.get('scraped_at', '')
                }
                unified_cad.append(unified_prop)
            
            self.source_stats['cad'] = {
                'count': len(properties),
                'avg_value': sum(p.get('appraised_value', 0) for p in properties) / len(properties) if properties else 0,
                'status': 'success'
            }
            
            logger.info(f"✅ CAD: {len(properties)} properties")
            return unified_cad[:self.config['max_leads_per_source']]
            
        except Exception as e:
            logger.error(f"❌ CAD scraper failed: {e}")
            self.source_stats['cad'] = {'count': 0, 'status': 'failed', 'error': str(e)}
            return []

    def integrate_storm_data(self, all_leads: List[Dict]) -> List[Dict]:
        """Integrate storm data with all leads"""
        if not self.config['enable_storm_integration']:
            return all_leads
        
        logger.info("⛈️ Integrating Storm Data...")
        try:
            enhanced_leads, storm_report = integrate_storm_data_with_properties(
                all_leads, 
                self.config['storm_lookback_days']
            )
            
            self.source_stats['storm_integration'] = {
                'total_leads': len(enhanced_leads),
                'storm_affected': storm_report.get('storm_affected_properties', 0),
                'high_priority': storm_report.get('high_priority_storm_leads', 0),
                'status': 'success'
            }
            
            logger.info(f"✅ Storm Integration: {storm_report.get('storm_affected_properties', 0)} affected properties")
            return enhanced_leads
            
        except Exception as e:
            logger.error(f"❌ Storm integration failed: {e}")
            self.source_stats['storm_integration'] = {'status': 'failed', 'error': str(e)}
            return all_leads

    def deduplicate_leads(self, all_leads: List[Dict]) -> List[Dict]:
        """Remove duplicate leads based on address"""
        logger.info("🔍 Deduplicating leads...")
        
        seen_addresses = set()
        unique_leads = []
        duplicates_removed = 0
        
        # Sort by source priority first (permits highest, then CAD, then real estate)
        sorted_leads = sorted(all_leads, key=lambda x: x.get('source_priority', 5), reverse=True)
        
        for lead in sorted_leads:
            address_key = lead.get('address', '').lower().strip()
            if address_key and address_key not in seen_addresses:
                seen_addresses.add(address_key)
                unique_leads.append(lead)
            else:
                duplicates_removed += 1
        
        logger.info(f"✅ Removed {duplicates_removed} duplicates, {len(unique_leads)} unique leads remain")
        return unique_leads

    def calculate_unified_stats(self, all_leads: List[Dict]):
        """Calculate comprehensive statistics across all sources"""
        if not all_leads:
            self.unified_stats = {}
            return
        
        # Source distribution
        source_counts = {}
        lead_scores = {'high': 0, 'medium': 0, 'low': 0}
        storm_affected = 0
        total_value = 0
        
        for lead in all_leads:
            source = lead.get('data_source', 'Unknown')
            source_counts[source] = source_counts.get(source, 0) + 1
            
            # Lead scoring
            score = lead.get('lead_score', 5)
            if score >= 8:
                lead_scores['high'] += 1
            elif score >= 6:
                lead_scores['medium'] += 1
            else:
                lead_scores['low'] += 1
            
            # Storm affected
            if lead.get('storm_affected', False):
                storm_affected += 1
            
            # Value
            price = lead.get('price', 0)
            if isinstance(price, (int, float)) and price > 0:
                total_value += price
        
        # County distribution
        county_counts = {}
        for lead in all_leads:
            county = lead.get('county', 'Unknown')
            county_counts[county] = county_counts.get(county, 0) + 1
        
        self.unified_stats = {
            'total_leads': len(all_leads),
            'source_distribution': source_counts,
            'lead_quality': lead_scores,
            'storm_affected_count': storm_affected,
            'storm_affected_percentage': round(storm_affected / len(all_leads) * 100, 1),
            'total_market_value': total_value,
            'average_value': int(total_value / len(all_leads)) if all_leads else 0,
            'county_distribution': dict(sorted(county_counts.items(), key=lambda x: x[1], reverse=True)[:10]),
            'generated_at': datetime.now().isoformat()
        }

    def push_to_google_sheets(self, leads: List[Dict]):
        """Push leads to Google Sheets"""
        if not self.config['enable_sheets_push'] or not self.sheets_client:
            logger.info("📊 Google Sheets push disabled or not configured")
            return
        
        try:
            logger.info("📊 Pushing leads to Google Sheets...")
            
            # Get or create spreadsheet
            sheet_name = f"DFW Roofing Leads - {datetime.now().strftime('%Y-%m-%d')}"
            
            try:
                spreadsheet = self.sheets_client.open(sheet_name)
            except gspread.SpreadsheetNotFound:
                spreadsheet = self.sheets_client.create(sheet_name)
                logger.info(f"Created new spreadsheet: {sheet_name}")
            
            # Clear and update main worksheet
            worksheet = spreadsheet.sheet1
            worksheet.clear()
            
            if leads:
                # Prepare data
                headers = list(leads[0].keys())
                rows = [headers]
                
                for lead in leads:
                    row = [str(lead.get(header, '')) for header in headers]
                    rows.append(row)
                
                # Update sheet
                worksheet.update(rows)
                
                # Add summary sheet
                try:
                    summary_sheet = spreadsheet.worksheet('Summary')
                except gspread.WorksheetNotFound:
                    summary_sheet = spreadsheet.add_worksheet('Summary', 20, 10)
                
                summary_data = [
                    ['Metric', 'Value'],
                    ['Total Leads', self.unified_stats.get('total_leads', 0)],
                    ['High Priority Leads', self.unified_stats.get('lead_quality', {}).get('high', 0)],
                    ['Storm Affected', self.unified_stats.get('storm_affected_count', 0)],
                    ['Total Market Value', f"${self.unified_stats.get('total_market_value', 0):,}"],
                    ['Average Value', f"${self.unified_stats.get('average_value', 0):,}"],
                    ['Last Updated', datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
                ]
                
                summary_sheet.clear()
                summary_sheet.update(summary_data)
                
                logger.info(f"✅ Pushed {len(leads)} leads to Google Sheets")
                logger.info(f"📊 Spreadsheet URL: {spreadsheet.url}")
            
        except Exception as e:
            logger.error(f"❌ Google Sheets push failed: {e}")

    def run_unified_scraping(self):
        """Main unified scraping orchestrator"""
        start_time = datetime.now()
        
        logger.info("🚀 STARTING UNIFIED MULTI-SOURCE ROOFING LEAD SCRAPER")
        logger.info("=" * 80)
        logger.info(f"Configuration: {json.dumps(self.config, indent=2)}")
        logger.info("=" * 80)
        
        all_leads = []
        
        try:
            # Run all enabled scrapers
            zillow_leads = self.run_zillow_scraper()
            all_leads.extend(zillow_leads)
            
            redfin_leads = self.run_redfin_scraper()
            all_leads.extend(redfin_leads)
            
            permit_leads = self.run_permit_scraper()
            all_leads.extend(permit_leads)
            
            cad_leads = self.run_cad_scraper()
            all_leads.extend(cad_leads)
            
            logger.info(f"📊 Raw leads collected: {len(all_leads)}")
            
            # Deduplicate
            unique_leads = self.deduplicate_leads(all_leads)
            
            # Integrate storm data
            enhanced_leads = self.integrate_storm_data(unique_leads)
            
            # Calculate final statistics
            self.calculate_unified_stats(enhanced_leads)
            
            # Save unified CSV
            if enhanced_leads:
                save_results_to_csv(enhanced_leads, 'leads.csv')
                
                # Also save timestamped version
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                save_results_to_csv(enhanced_leads, f'unified_leads_{timestamp}.csv')
                
                # Push to Google Sheets
                self.push_to_google_sheets(enhanced_leads)
            
            # Final reporting
            self.print_final_report(start_time)
            
            # Log Supabase integration status
            if self.supabase_conn.supabase:
                logger.info("📊 All leads have been automatically inserted into Supabase tables")
            else:
                logger.warning("⚠️ Supabase not configured - leads only saved to CSV")
            
            self.all_leads = enhanced_leads
            return enhanced_leads
            
        except Exception as e:
            logger.error(f"❌ Unified scraping failed: {e}")
            return []

    def print_final_report(self, start_time: datetime):
        """Print comprehensive final report"""
        end_time = datetime.now()
        runtime = end_time - start_time
        
        logger.info("")
        logger.info("🎯 UNIFIED SCRAPING FINAL REPORT")
        logger.info("=" * 80)
        
        # Overall stats
        logger.info(f"⏱️  Total Runtime: {runtime}")
        logger.info(f"📊 Total Unified Leads: {self.unified_stats.get('total_leads', 0)}")
        logger.info(f"💰 Total Market Value: ${self.unified_stats.get('total_market_value', 0):,}")
        logger.info(f"📈 Average Lead Value: ${self.unified_stats.get('average_value', 0):,}")
        
        # Source breakdown
        logger.info("\n📋 Source Breakdown:")
        source_dist = self.unified_stats.get('source_distribution', {})
        for source, count in source_dist.items():
            status = self.source_stats.get(source.lower(), {}).get('status', 'unknown')
            logger.info(f"   • {source}: {count} leads ({status})")
        
        # Lead quality
        logger.info("\n🎯 Lead Quality Distribution:")
        quality = self.unified_stats.get('lead_quality', {})
        logger.info(f"   • High Priority (8-10): {quality.get('high', 0)} leads")
        logger.info(f"   • Medium Priority (6-7): {quality.get('medium', 0)} leads")
        logger.info(f"   • Low Priority (1-5): {quality.get('low', 0)} leads")
        
        # Storm impact
        storm_count = self.unified_stats.get('storm_affected_count', 0)
        storm_pct = self.unified_stats.get('storm_affected_percentage', 0)
        logger.info(f"\n⛈️  Storm-Affected Properties: {storm_count} ({storm_pct}%)")
        
        # Top counties
        logger.info("\n🏛️  Top Counties:")
        counties = self.unified_stats.get('county_distribution', {})
        for county, count in list(counties.items())[:5]:
            logger.info(f"   • {county}: {count} leads")
        
        logger.info("\n✅ UNIFIED SCRAPING COMPLETED SUCCESSFULLY!")
        logger.info("=" * 80)


def main():
    """Main execution function for GitHub Actions"""
    control_panel = UnifiedLeadControlPanel()
    
    # Run unified scraping
    leads = control_panel.run_unified_scraping()
    
    # Return count for GitHub Actions
    return len(leads) if leads else 0


if __name__ == "__main__":
    main()