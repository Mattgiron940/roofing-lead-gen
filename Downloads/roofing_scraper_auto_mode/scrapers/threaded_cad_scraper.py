#!/usr/bin/env python3
"""
Threaded Texas CAD Scraper using ScraperAPI
High-performance parallel scraping with Supabase integration and real-time lead scoring
"""

import sys
import os
sys.path.append('..')

import re
import json
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import time
import logging
from typing import List, Dict, Any, Optional

# Import unified Supabase client
from supabase_client import supabase

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ThreadedCADScraper:
    """High-performance CAD scraper with multi-threading and real-time insertion"""
    
    def __init__(self):
        self.scraper_api_key = self.get_scraper_api_key()
        self.session = requests.Session()
        self.processed_urls = set()
        
        # CAD search URLs for major Texas counties
        self.target_urls = self.generate_cad_urls()
    
    def get_scraper_api_key(self) -> str:
        """Get ScraperAPI key from environment"""
        # Try environment variable first
        api_key = os.getenv('SCRAPER_API_KEY')
        
        if not api_key:
            # Try Desktop .env file
            desktop_env = os.path.expanduser("~/Desktop/.env")
            if os.path.exists(desktop_env):
                with open(desktop_env, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if line.startswith('SCRAPER_API_KEY='):
                            api_key = line.split('=', 1)[1]
                            break
        
        if not api_key:
            # Fallback to known working key
            api_key = "6972d80a231d2c07209e0ce837e34e69"
        
        return api_key
    
    def generate_cad_urls(self) -> List[str]:
        """Generate CAD search URLs for different Texas counties"""
        # These are example URLs - in reality, these would need to be actual CAD search URLs
        # Each county has different CAD search systems
        base_urls = [
            # Dallas County CAD - property search with high-value filters
            "https://www.dallascad.org/PropertySearch/Search?searchType=advanced&minValue=300000&maxValue=800000",
            "https://www.dallascad.org/PropertySearch/Search?searchType=street&street=Preston&city=Dallas",
            
            # Tarrant County CAD - property search
            "https://www.tad.org/property-search?city=Fort+Worth&minValue=250000&maxValue=700000",
            "https://www.tad.org/property-search?city=Arlington&minValue=200000&maxValue=600000",
            
            # Collin County CAD - high-value areas like Plano, Frisco
            "https://www.collincad.org/search?city=Plano&minValue=400000&maxValue=1000000",
            "https://www.collincad.org/search?city=Frisco&minValue=450000&maxValue=1200000",
            
            # Denton County CAD
            "https://www.dentoncad.com/property-search?city=Lewisville&minValue=300000",
            "https://www.dentoncad.com/property-search?city=Flower+Mound&minValue=350000",
            
            # Harris County CAD (Houston area - for expansion)
            "https://hcad.org/property-search?city=Houston&minValue=300000&maxValue=800000",
            
            # Add more real URLs here based on actual CAD search patterns
        ]
        
        return base_urls
    
    def get_scraperapi_url(self, target_url: str) -> str:
        """Generate ScraperAPI URL with proper parameters"""
        return f"http://api.scraperapi.com?api_key={self.scraper_api_key}&url={target_url}&render=true"

    def parse_cad_listing(self, html: str, source_url: str) -> List[Dict[str, Any]]:
        """Parse CAD search results page and extract individual properties"""
        properties = []
        
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # Look for JSON data first (many CAD portals use JavaScript)
            script_tags = soup.find_all('script')
            
            for script in script_tags:
                if script.string and ('properties' in script.string.lower() or 'accounts' in script.string.lower()):
                    try:
                        # Try to extract JSON property data
                        json_match = re.search(r'(?:properties|accounts)\s*[:=]\s*(\[.+?\])', script.string)
                        if json_match:
                            data = json.loads(json_match.group(1))
                            properties.extend(self.extract_properties_from_json(data, source_url))
                            break
                    except (json.JSONDecodeError, Exception) as e:
                        logger.debug(f"Error parsing JSON from script: {e}")
                        continue
            
            # Fallback to HTML parsing if JSON not found
            if not properties:
                properties = self.parse_cad_html_listings(soup, source_url)
            
            logger.info(f"Found {len(properties)} properties from {source_url}")
            
        except Exception as e:
            logger.error(f"❌ Error parsing CAD listings from {source_url}: {e}")
        
        return properties
    
    def extract_properties_from_json(self, data: list, source_url: str) -> List[Dict[str, Any]]:
        """Extract properties from JSON data structure"""
        properties = []
        
        try:
            for prop_obj in data[:15]:  # Limit to first 15 per page
                prop_data = self.extract_property_from_json_obj(prop_obj, source_url)
                if prop_data:
                    properties.append(prop_data)
                    
        except Exception as e:
            logger.debug(f"Error extracting from JSON: {e}")
        
        return properties
    
    def extract_property_from_json_obj(self, prop_obj: dict, source_url: str) -> Optional[Dict[str, Any]]:
        """Extract property data from a single JSON property object"""
        try:
            # Extract account number
            account_number = prop_obj.get('account', '') or prop_obj.get('account_number', '') or prop_obj.get('id', '')
            
            # Extract owner information
            owner_name = prop_obj.get('owner', '') or prop_obj.get('owner_name', '') or prop_obj.get('taxpayer', '')
            
            # Extract address
            address_text = prop_obj.get('address', '') or prop_obj.get('property_address', '') or prop_obj.get('site_address', '')
            
            # Extract property values
            appraised_value = prop_obj.get('appraised_value', 0) or prop_obj.get('total_value', 0) or prop_obj.get('assessed_value', 0)
            market_value = prop_obj.get('market_value', 0) or appraised_value
            
            # Extract property details
            year_built = prop_obj.get('year_built', None) or prop_obj.get('year_constructed', None)
            property_type = prop_obj.get('property_type', '') or prop_obj.get('improvement_type', '')
            square_feet = prop_obj.get('square_feet', None) or prop_obj.get('living_area', None)
            lot_size_acres = prop_obj.get('lot_acres', None) or prop_obj.get('land_acres', None)
            
            # Extract exemptions
            homestead_exemption = prop_obj.get('homestead', False) or prop_obj.get('homestead_exemption', False)
            
            # Extract sale information
            last_sale_date = prop_obj.get('last_sale_date', None) or prop_obj.get('deed_date', None)
            last_sale_price = prop_obj.get('last_sale_price', None) or prop_obj.get('deed_amount', None)
            
            # Parse city from address or URL
            city, state, zip_code = self.parse_address_components(address_text)
            if not city:
                city = self.extract_city_from_url(source_url)
            
            # Only create property if we have essential data
            if account_number and address_text and appraised_value > 0:
                return {
                    "account_number": account_number,
                    "owner_name": owner_name,
                    "address_text": address_text,
                    "city": city,
                    "county": self.get_county_from_city(city),
                    "zip_code": zip_code,
                    "property_type": property_type or "Residential",
                    "year_built": year_built,
                    "square_feet": square_feet,
                    "lot_size_acres": lot_size_acres,
                    "appraised_value": appraised_value,
                    "market_value": market_value,
                    "homestead_exemption": homestead_exemption,
                    "last_sale_date": self.parse_date(last_sale_date) if last_sale_date else None,
                    "last_sale_price": last_sale_price,
                    "cad_url": source_url,
                    "lead_status": "new",
                    "priority": self.calculate_priority(appraised_value, year_built),
                    "routing_tags": f"cad,{city.lower()},{state.lower() if state else 'tx'}",
                    "notes": f"Scraped from CAD on {datetime.now().strftime('%Y-%m-%d')}"
                }
            
            return None
            
        except Exception as e:
            logger.debug(f"Error extracting property from JSON object: {e}")
            return None
    
    def parse_cad_html_listings(self, soup, source_url: str) -> List[Dict[str, Any]]:
        """Fallback HTML parsing if JSON data not available"""
        properties = []
        
        try:
            # Look for property tables or cards in HTML
            property_selectors = [
                'table.search-results tbody tr',
                '.property-row',
                '.property-card',
                '.search-result-row',
                '[data-account]',
                'tr[class*="property"]'
            ]
            
            property_rows = []
            for selector in property_selectors:
                rows = soup.select(selector)
                if rows:
                    property_rows = rows
                    break
            
            for row in property_rows[:12]:  # Limit to first 12
                prop_data = self.extract_property_from_html_row(row, source_url)
                if prop_data:
                    properties.append(prop_data)
                    
        except Exception as e:
            logger.debug(f"Error in HTML fallback parsing: {e}")
        
        return properties
    
    def extract_property_from_html_row(self, row, source_url: str) -> Optional[Dict[str, Any]]:
        """Extract property data from HTML table row or card"""
        try:
            # Extract text from all cells/elements
            cells = row.find_all(['td', 'div', 'span'])
            
            # Extract account number
            account_number = ""
            for cell in cells:
                cell_text = cell.get_text(strip=True)
                # Look for account number patterns
                account_match = re.search(r'(?:account|acct)[#\s]*([A-Z0-9\-]+)', cell_text, re.I)
                if account_match:
                    account_number = account_match.group(1)
                    break
            
            # Extract address
            address_text = ""
            for cell in cells:
                cell_text = cell.get_text(strip=True)
                # Look for address patterns
                if re.search(r'\d+\s+\w+.*(?:st|street|ave|avenue|rd|road|dr|drive|ln|lane|blvd|boulevard)', cell_text, re.I):
                    address_text = cell_text
                    break
            
            # Extract owner name
            owner_name = ""
            for cell in cells:
                cell_text = cell.get_text(strip=True)
                # Look for owner name (usually appears near beginning, contains proper names)
                if len(cell_text) > 5 and len(cell_text) < 50 and re.search(r'[A-Z][a-z]+\s+[A-Z][a-z]+', cell_text):
                    owner_name = cell_text
                    break
            
            # Extract appraised value
            appraised_value = 0
            for cell in cells:
                cell_text = cell.get_text(strip=True)
                # Look for dollar amounts
                value_match = re.search(r'\$([0-9,]+)', cell_text.replace(',', ''))
                if value_match:
                    try:
                        appraised_value = int(value_match.group(1).replace(',', ''))
                        if appraised_value > 50000:  # Filter out small values that might be fees
                            break
                    except:
                        continue
            
            # Parse city from address or URL
            city, state, zip_code = self.parse_address_components(address_text)
            if not city:
                city = self.extract_city_from_url(source_url)
            
            if account_number and address_text and appraised_value > 0:
                return {
                    "account_number": account_number,
                    "owner_name": owner_name,
                    "address_text": address_text,
                    "city": city,
                    "county": self.get_county_from_city(city),
                    "zip_code": zip_code,
                    "property_type": "Residential",
                    "year_built": None,
                    "square_feet": None,
                    "lot_size_acres": None,
                    "appraised_value": appraised_value,
                    "market_value": appraised_value,
                    "homestead_exemption": False,
                    "last_sale_date": None,
                    "last_sale_price": None,
                    "cad_url": source_url,
                    "lead_status": "new",
                    "priority": self.calculate_priority(appraised_value, None),
                    "routing_tags": f"cad,{city.lower()},tx",
                    "notes": f"Scraped from CAD HTML on {datetime.now().strftime('%Y-%m-%d')}"
                }
            
            return None
            
        except Exception as e:
            logger.debug(f"Error extracting from HTML row: {e}")
            return None
    
    def parse_address_components(self, address: str) -> tuple:
        """Parse city, state, zip from address string"""
        city = ""
        state = "TX"
        zip_code = ""
        
        try:
            if not address:
                return city, state, zip_code
            
            # Split by comma
            parts = [part.strip() for part in address.split(',')]
            
            if len(parts) >= 2:
                # Last part usually contains state and zip
                last_part = parts[-1]
                
                # Extract zip code
                zip_match = re.search(r'(\d{5})', last_part)
                if zip_match:
                    zip_code = zip_match.group(1)
                
                # Extract state
                state_match = re.search(r'\b([A-Z]{2})\b', last_part)
                if state_match:
                    state = state_match.group(1)
                
                # City is usually the second-to-last part
                if len(parts) >= 2:
                    city = parts[-2].strip()
                    
        except Exception as e:
            logger.debug(f"Error parsing address components: {e}")
        
        return city, state, zip_code
    
    def extract_city_from_url(self, url: str) -> str:
        """Extract city name from CAD URL"""
        try:
            # Look for city names in URL
            city_patterns = [
                r'dallas',
                r'fort[\+\s]worth',
                r'plano',
                r'frisco',
                r'arlington',
                r'irving',
                r'garland',
                r'lewisville',
                r'flower[\+\s]mound',
                r'houston'
            ]
            
            for pattern in city_patterns:
                match = re.search(pattern, url, re.I)
                if match:
                    return match.group().replace('+', ' ').replace('%20', ' ').title()
        except:
            pass
        return ""

    def get_county_from_city(self, city: str) -> str:
        """Map city names to counties"""
        city_lower = city.lower()
        
        county_map = {
            'dallas': 'Dallas County',
            'fort worth': 'Tarrant County',
            'arlington': 'Tarrant County',
            'plano': 'Collin County',
            'frisco': 'Collin County',
            'mckinney': 'Collin County',
            'allen': 'Collin County',
            'irving': 'Dallas County',
            'garland': 'Dallas County',
            'mesquite': 'Dallas County',
            'carrollton': 'Dallas County',
            'richardson': 'Dallas County',
            'lewisville': 'Denton County',
            'flower mound': 'Denton County',
            'southlake': 'Tarrant County',
            'grapevine': 'Tarrant County',
            'houston': 'Harris County'
        }
        
        return county_map.get(city_lower, 'Dallas County')
    
    def extract_county_from_url(self, url: str) -> str:
        """Extract county name from CAD URL"""
        if 'dallascad' in url:
            return "Dallas County"
        elif 'tad.org' in url:
            return "Tarrant County"
        elif 'hcad.org' in url:
            return "Harris County"
        elif 'bcad.org' in url:
            return "Bexar County"
        elif 'tcad.org' in url:
            return "Travis County"
        elif 'collincad' in url:
            return "Collin County"
        elif 'dentoncad' in url:
            return "Denton County"
        else:
            return "Unknown County"

    def calculate_priority(self, appraised_value: int, year_built: Optional[int]) -> str:
        """Calculate priority based on property characteristics"""
        # High priority for high-value properties or older homes
        if appraised_value > 500000:
            return "high"
        elif year_built and (datetime.now().year - year_built) > 15:
            return "high"
        elif appraised_value > 300000:
            return "medium"
        else:
            return "low"
    
    def parse_date(self, date_str: str) -> Optional[str]:
        """Parse date string into ISO format"""
        if not date_str:
            return None
        
        try:
            # Try different date formats
            date_formats = [
                '%Y-%m-%d',
                '%m/%d/%Y',
                '%m-%d-%Y',
                '%Y/%m/%d',
                '%m/%d/%y',
                '%d/%m/%Y'
            ]
            
            for fmt in date_formats:
                try:
                    parsed_date = datetime.strptime(date_str.strip(), fmt)
                    return parsed_date.strftime('%Y-%m-%d')
                except ValueError:
                    continue
            
            return None
        except:
            return None
    
    def scrape_single_url(self, url: str) -> List[Dict[str, Any]]:
        """Scrape a single URL and return extracted properties"""
        if url in self.processed_urls:
            return []
        
        try:
            logger.info(f"🔍 Scraping {url}")
            
            # Get page with ScraperAPI
            scraper_url = self.get_scraperapi_url(url)
            response = self.session.get(scraper_url, timeout=60)
            response.raise_for_status()
            
            # Parse properties from page
            properties = self.parse_cad_listing(response.text, url)
            
            # Insert each property into Supabase
            successful_inserts = 0
            for prop in properties:
                if supabase.safe_insert("cad_leads", prop):
                    successful_inserts += 1
            
            self.processed_urls.add(url)
            logger.info(f"✅ {url}: {successful_inserts}/{len(properties)} properties inserted")
            
            return properties
            
        except requests.exceptions.Timeout:
            logger.error(f"⏰ Timeout scraping {url}")
            return []
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Request error for {url}: {e}")
            return []
        except Exception as e:
            logger.error(f"❌ Error scraping {url}: {e}")
            return []
    
    def run_threaded_scraping(self, max_workers: int = 3) -> Dict[str, Any]:
        """Run multi-threaded scraping of all target URLs"""
        logger.info(f"🚀 Starting threaded CAD scraping with {max_workers} workers")
        logger.info(f"📊 Targeting {len(self.target_urls)} URLs")
        
        start_time = time.time()
        all_properties = []
        successful_urls = 0
        failed_urls = 0
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all URLs for processing
            future_to_url = {
                executor.submit(self.scrape_single_url, url): url 
                for url in self.target_urls
            }
            
            # Process completed futures
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    properties = future.result()
                    if properties:
                        all_properties.extend(properties)
                        successful_urls += 1
                    else:
                        failed_urls += 1
                except Exception as e:
                    logger.error(f"❌ Exception processing {url}: {e}")
                    failed_urls += 1
        
        end_time = time.time()
        runtime = end_time - start_time
        
        # Compile results
        results = {
            'total_urls': len(self.target_urls),
            'successful_urls': successful_urls,
            'failed_urls': failed_urls,
            'total_properties': len(all_properties),
            'runtime_seconds': runtime,
            'average_per_url': runtime / len(self.target_urls) if self.target_urls else 0,
            'properties_per_minute': (len(all_properties) / runtime) * 60 if runtime > 0 else 0
        }
        
        logger.info(f"📊 Scraping completed: {results['total_properties']} properties in {runtime:.2f}s")
        return results

def main():
    """Main execution function"""
    print("🏛️ THREADED CAD SCRAPER STARTING")
    print("=" * 50)
    
    # Check if cad_leads table exists
    if not supabase.check_table_exists("cad_leads"):
        print("❌ cad_leads table does not exist!")
        print("💡 Run the Supabase schema deployment first")
        return 1
    
    # Initialize scraper
    scraper = ThreadedCADScraper()
    
    # Run scraping
    results = scraper.run_threaded_scraping(max_workers=3)
    
    # Print final report
    print("\n" + "=" * 50)
    print("🎯 CAD SCRAPING FINAL REPORT")
    print("=" * 50)
    print(f"📊 URLs Processed: {results['successful_urls']}/{results['total_urls']}")
    print(f"✅ Total Properties Found: {results['total_properties']}")
    print(f"⏱️ Runtime: {results['runtime_seconds']:.2f} seconds")
    print(f"🚀 Speed: {results['properties_per_minute']:.1f} properties/minute")
    print(f"📈 Success Rate: {(results['successful_urls']/results['total_urls']*100):.1f}%")
    
    # Check final database count
    final_count = supabase.get_table_count("cad_leads")
    print(f"🗄️ Total CAD Leads in Database: {final_count}")
    
    print("✅ CAD SCRAPER COMPLETED!")
    return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)