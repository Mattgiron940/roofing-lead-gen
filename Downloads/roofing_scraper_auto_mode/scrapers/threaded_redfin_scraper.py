#!/usr/bin/env python3
"""
Threaded Redfin Scraper using ScraperAPI
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

class ThreadedRedfinScraper:
    """High-performance Redfin scraper with multi-threading and real-time insertion"""
    
    def __init__(self):
        self.scraper_api_key = self.get_scraper_api_key()
        self.session = requests.Session()
        self.processed_urls = set()
        
        # Redfin search URLs for Dallas/Fort Worth area
        self.target_urls = self.generate_redfin_urls()
    
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
    
    def generate_redfin_urls(self) -> List[str]:
        """Generate Redfin search URLs for different areas and price ranges"""
        base_urls = [
            # Dallas area searches - various price ranges
            "https://www.redfin.com/city/30794/TX/Dallas?min_price=200000&max_price=500000",
            "https://www.redfin.com/city/30794/TX/Dallas?min_price=500000&max_price=800000",
            
            # Fort Worth area
            "https://www.redfin.com/city/30805/TX/Fort-Worth?min_price=200000&max_price=500000",
            
            # Plano (high-value area)
            "https://www.redfin.com/city/30827/TX/Plano?min_price=300000&max_price=700000",
            
            # Frisco (high-value area)
            "https://www.redfin.com/city/30806/TX/Frisco?min_price=400000&max_price=800000",
            
            # Arlington
            "https://www.redfin.com/city/30776/TX/Arlington?min_price=200000&max_price=500000",
            
            # Irving  
            "https://www.redfin.com/city/30817/TX/Irving?min_price=250000&max_price=600000",
            
            # Garland
            "https://www.redfin.com/city/30808/TX/Garland?min_price=200000&max_price=450000"
        ]
        
        return base_urls
    
    def get_scraperapi_url(self, target_url: str) -> str:
        """Generate ScraperAPI URL with proper parameters"""
        return f"http://api.scraperapi.com?api_key={self.scraper_api_key}&url={target_url}&render=true"
    
    def parse_redfin_listing(self, html: str, source_url: str) -> List[Dict[str, Any]]:
        """Parse Redfin search results page and extract individual listings"""
        listings = []
        
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # Look for JSON data first (Redfin loads data via JavaScript)
            script_tags = soup.find_all('script')
            
            for script in script_tags:
                if script.string and ('window.reactAppData' in script.string or 'window.__INITIAL_STATE__' in script.string):
                    try:
                        # Try to extract JSON data
                        json_match = re.search(r'window\.(?:reactAppData|__INITIAL_STATE__)\s*=\s*({.+?});', script.string)
                        if json_match:
                            data = json.loads(json_match.group(1))
                            listings.extend(self.extract_listings_from_json(data, source_url))
                            break
                    except (json.JSONDecodeError, Exception) as e:
                        logger.debug(f"Error parsing JSON from script: {e}")
                        continue
            
            # Fallback to HTML parsing if JSON not found
            if not listings:
                listings = self.parse_redfin_html_listings(soup, source_url)
            
            logger.info(f"Found {len(listings)} listings from {source_url}")
            
        except Exception as e:
            logger.error(f"❌ Error parsing Redfin listings from {source_url}: {e}")
        
        return listings
    
    def extract_listings_from_json(self, data: dict, source_url: str) -> List[Dict[str, Any]]:
        """Extract listings from JSON data structure"""
        listings = []
        
        try:
            # Navigate JSON structure to find property listings
            properties = []
            
            # Try different JSON paths
            if 'searchResults' in data:
                properties = data['searchResults'].get('properties', [])
            elif 'homes' in data:
                properties = data['homes']
            elif 'listings' in data:
                properties = data['listings']
            
            for prop in properties[:10]:  # Limit to first 10 per page
                listing_data = self.extract_listing_from_json_prop(prop, source_url)
                if listing_data:
                    listings.append(listing_data)
                    
        except Exception as e:
            logger.debug(f"Error extracting from JSON: {e}")
        
        return listings
    
    def extract_listing_from_json_prop(self, prop: dict, source_url: str) -> Optional[Dict[str, Any]]:
        """Extract listing data from a single JSON property object"""
        try:
            # Extract address
            address_text = ""
            city = ""
            state = "TX"
            zip_code = ""
            
            if 'address' in prop:
                addr = prop['address']
                street = addr.get('streetAddress', '')
                city = addr.get('city', '')
                state = addr.get('state', 'TX')
                zip_code = addr.get('zip', '')
                address_text = f"{street}, {city}, {state} {zip_code}".strip()
            
            # Extract basic property data
            price = prop.get('price', 0) or 0
            bedrooms = prop.get('beds', 0) or 0
            bathrooms = prop.get('baths', 0) or 0
            square_feet = prop.get('sqFt', 0) or 0
            year_built = prop.get('yearBuilt')
            lot_size = prop.get('lotSize', 0) or 0
            
            # Property details
            property_type = prop.get('propertyType', 'Single Family Residence')
            days_on_redfin = prop.get('dom', 0) or 0
            mls_number = prop.get('mlsId', '') or ''
            
            # Build property URL
            redfin_url = source_url
            if 'url' in prop and prop['url']:
                redfin_url = f"https://www.redfin.com{prop['url']}"
            
            # Only create listing if we have essential data
            if address_text and price > 0:
                return {
                    "address_text": address_text,
                    "city": city,
                    "state": state,
                    "zip_code": zip_code,
                    "county": self.get_county_from_city(city),
                    "price": price,
                    "num_bedrooms": bedrooms,
                    "num_bathrooms": bathrooms,
                    "square_feet": square_feet,
                    "year_built": year_built,
                    "property_type": property_type,
                    "lot_size_sqft": lot_size,
                    "sold_date": None,
                    "days_on_redfin": days_on_redfin,
                    "mls_number": mls_number,
                    "price_per_sqft": f"${price/square_feet:.0f}" if square_feet > 0 else None,
                    "redfin_url": redfin_url,
                    "hoa_fee": None,
                    "parking_spaces": None,
                    "lead_status": "new",
                    "priority": "medium",
                    "routing_tags": f"redfin,{city.lower()},{state.lower()}",
                    "notes": f"Scraped from Redfin on {datetime.now().strftime('%Y-%m-%d')}"
                }
            
            return None
            
        except Exception as e:
            logger.debug(f"Error extracting listing from JSON property: {e}")
            return None
    
    def parse_redfin_html_listings(self, soup, source_url: str) -> List[Dict[str, Any]]:
        """Fallback HTML parsing if JSON data not available"""
        listings = []
        
        try:
            # Look for property cards in HTML
            property_selectors = [
                '.HomeCard',
                '.SearchResultProperty',
                '[data-rf-test-id="mapHomeCard"]',
                '.home-card',
                '.listing-card'
            ]
            
            property_cards = []
            for selector in property_selectors:
                cards = soup.select(selector)
                if cards:
                    property_cards = cards
                    break
            
            for card in property_cards[:8]:  # Limit to first 8
                listing_data = self.extract_listing_from_html_card(card, source_url)
                if listing_data:
                    listings.append(listing_data)
                    
        except Exception as e:
            logger.debug(f"Error in HTML fallback parsing: {e}")
        
        return listings
    
    def extract_listing_from_html_card(self, card, source_url: str) -> Optional[Dict[str, Any]]:
        """Extract listing data from HTML property card"""
        try:
            # Extract address
            address_selectors = ['.address', '.home-address', '[data-rf-test-id="property-address"]']
            address_text = ""
            
            for selector in address_selectors:
                elem = card.select_one(selector)
                if elem:
                    address_text = elem.get_text(strip=True)
                    break
            
            # Extract price
            price_selectors = ['.price', '.home-price', '[data-rf-test-id="property-price"]']
            price = 0
            
            for selector in price_selectors:
                elem = card.select_one(selector)
                if elem:
                    price_text = elem.get_text(strip=True)
                    price_match = re.search(r'[\$]?([\d,]+)', price_text.replace(',', ''))
                    if price_match:
                        price = int(price_match.group(1).replace(',', ''))
                    break
            
            # Extract beds/baths
            stats_text = card.get_text()
            beds = None
            baths = None
            
            bed_match = re.search(r'(\d+)\s*(?:bed|bd)', stats_text, re.I)
            if bed_match:
                beds = int(bed_match.group(1))
            
            bath_match = re.search(r'(\d+(?:\.\d+)?)\s*(?:bath|ba)', stats_text, re.I)
            if bath_match:
                baths = float(bath_match.group(1))
            
            # Parse city from address or URL
            city, state, zip_code = self.parse_address_components(address_text)
            if not city:
                city = self.extract_city_from_url(source_url)
            
            if address_text and price > 0:
                return {
                    "address_text": address_text,
                    "city": city,
                    "state": state or "TX",
                    "zip_code": zip_code,
                    "county": self.get_county_from_city(city),
                    "price": price,
                    "num_bedrooms": beds,
                    "num_bathrooms": baths,
                    "square_feet": None,
                    "year_built": None,
                    "property_type": "Single Family Residence",
                    "lot_size_sqft": None,
                    "sold_date": None,
                    "days_on_redfin": None,
                    "mls_number": "",
                    "price_per_sqft": None,
                    "redfin_url": source_url,
                    "hoa_fee": None,
                    "parking_spaces": None,
                    "lead_status": "new",
                    "priority": "medium",
                    "routing_tags": f"redfin,{city.lower()},tx",
                    "notes": f"Scraped from Redfin HTML on {datetime.now().strftime('%Y-%m-%d')}"
                }
            
            return None
            
        except Exception as e:
            logger.debug(f"Error extracting from HTML card: {e}")
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
        """Extract city name from Redfin URL"""
        try:
            city_match = re.search(r'/TX/([^/?]+)', url)
            if city_match:
                return city_match.group(1).replace('-', ' ')
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
            'grapevine': 'Tarrant County'
        }
        
        return county_map.get(city_lower, 'Dallas County')
    
    def scrape_single_url(self, url: str) -> List[Dict[str, Any]]:
        """Scrape a single URL and return extracted listings"""
        if url in self.processed_urls:
            return []
        
        try:
            logger.info(f"🔍 Scraping {url}")
            
            # Get page with ScraperAPI
            scraper_url = self.get_scraperapi_url(url)
            response = self.session.get(scraper_url, timeout=60)
            response.raise_for_status()
            
            # Parse listings from page
            listings = self.parse_redfin_listing(response.text, url)
            
            # Insert each listing into Supabase
            successful_inserts = 0
            for listing in listings:
                if supabase.safe_insert("redfin_leads", listing):
                    successful_inserts += 1
            
            self.processed_urls.add(url)
            logger.info(f"✅ {url}: {successful_inserts}/{len(listings)} listings inserted")
            
            return listings
            
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
        logger.info(f"🚀 Starting threaded Redfin scraping with {max_workers} workers")
        logger.info(f"📊 Targeting {len(self.target_urls)} URLs")
        
        start_time = time.time()
        all_listings = []
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
                    listings = future.result()
                    if listings:
                        all_listings.extend(listings)
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
            'total_listings': len(all_listings),
            'runtime_seconds': runtime,
            'average_per_url': runtime / len(self.target_urls) if self.target_urls else 0,
            'listings_per_minute': (len(all_listings) / runtime) * 60 if runtime > 0 else 0
        }
        
        logger.info(f"📊 Scraping completed: {results['total_listings']} listings in {runtime:.2f}s")
        return results

def main():
    """Main execution function"""
    print("🏘️ THREADED REDFIN SCRAPER STARTING")
    print("=" * 50)
    
    # Check if redfin_leads table exists
    if not supabase.check_table_exists("redfin_leads"):
        print("❌ redfin_leads table does not exist!")
        print("💡 Run the Supabase schema deployment first")
        return 1
    
    # Initialize scraper
    scraper = ThreadedRedfinScraper()
    
    # Run scraping
    results = scraper.run_threaded_scraping(max_workers=3)
    
    # Print final report
    print("\n" + "=" * 50)
    print("🎯 REDFIN SCRAPING FINAL REPORT")
    print("=" * 50)
    print(f"📊 URLs Processed: {results['successful_urls']}/{results['total_urls']}")
    print(f"✅ Total Listings Found: {results['total_listings']}")
    print(f"⏱️ Runtime: {results['runtime_seconds']:.2f} seconds")
    print(f"🚀 Speed: {results['listings_per_minute']:.1f} listings/minute")
    print(f"📈 Success Rate: {(results['successful_urls']/results['total_urls']*100):.1f}%")
    
    # Check final database count
    final_count = supabase.get_table_count("redfin_leads")
    print(f"🗄️ Total Redfin Leads in Database: {final_count}")
    
    print("✅ REDFIN SCRAPER COMPLETED!")
    return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)