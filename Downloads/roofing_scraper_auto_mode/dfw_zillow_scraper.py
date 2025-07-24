#!/usr/bin/env python3
"""
DFW Multi-County Zillow Scraper
Scrapes recently sold properties from all major DFW counties for roofing leads
"""

import requests
import time
import json
import csv
import os
from datetime import datetime, timedelta
from typing import List, Dict, Any
import logging
from urllib.parse import urlencode, quote_plus
import random

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DFWZillowScraper:
    def __init__(self):
        self.base_url = "https://www.zillow.com"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
        # DFW Counties and major cities
        self.dfw_locations = {
            'Dallas County': [
                'Dallas, TX', 'Plano, TX', 'Irving, TX', 'Garland, TX', 'Mesquite, TX',
                'Richardson, TX', 'Carrollton, TX', 'Farmers Branch, TX', 'University Park, TX',
                'Highland Park, TX', 'DeSoto, TX', 'Duncanville, TX', 'Lancaster, TX'
            ],
            'Tarrant County': [
                'Fort Worth, TX', 'Arlington, TX', 'Grand Prairie, TX', 'Mansfield, TX',
                'Euless, TX', 'Bedford, TX', 'Hurst, TX', 'Keller, TX', 'Southlake, TX',
                'Colleyville, TX', 'Grapevine, TX', 'North Richland Hills, TX'
            ],
            'Collin County': [
                'McKinney, TX', 'Frisco, TX', 'Allen, TX', 'Wylie, TX', 'Murphy, TX',
                'Prosper, TX', 'Celina, TX', 'Little Elm, TX', 'The Colony, TX'
            ],
            'Denton County': [
                'Denton, TX', 'Lewisville, TX', 'Flower Mound, TX', 'Coppell, TX',
                'Highland Village, TX', 'Lake Dallas, TX', 'Corinth, TX'
            ],
            'Rockwall County': [
                'Rockwall, TX', 'Rowlett, TX', 'Royse City, TX', 'Heath, TX'
            ],
            'Ellis County': [
                'Waxahachie, TX', 'Ennis, TX', 'Midlothian, TX', 'Cedar Hill, TX'
            ]
        }
        
        self.all_properties = []

    def build_search_url(self, location: str, page: int = 1) -> str:
        """Build Zillow search URL for recently sold properties"""
        # Parameters for recently sold homes (last 6 months)
        params = {
            'searchQueryState': json.dumps({
                "pagination": {"currentPage": page},
                "usersSearchTerm": location,
                "mapBounds": {},
                "regionSelection": [{"regionId": None, "regionType": 6}],
                "isMapVisible": True,
                "filterState": {
                    "sortSelection": {"value": "days"},
                    "isRecentlySold": {"value": True},
                    "isForSaleByAgent": {"value": False},
                    "isForSaleByOwner": {"value": False},
                    "isNewConstruction": {"value": False},
                    "isComingSoon": {"value": False},
                    "isAuction": {"value": False},
                    "isForSaleForeclosure": {"value": False}
                },
                "isListVisible": True
            })
        }
        
        return f"{self.base_url}/homes/recently_sold/{quote_plus(location)}/?" + urlencode(params)

    def extract_property_data(self, property_json: Dict) -> Dict[str, Any]:
        """Extract relevant property data from Zillow JSON"""
        try:
            address_parts = property_json.get('address', {})
            price_info = property_json.get('price', {})
            
            return {
                'address': f"{address_parts.get('streetAddress', '')} {address_parts.get('city', '')} {address_parts.get('state', '')} {address_parts.get('zipcode', '')}".strip(),
                'city': address_parts.get('city', ''),
                'state': address_parts.get('state', ''),
                'zipcode': address_parts.get('zipcode', ''),
                'price': price_info.get('value', ''),
                'bedrooms': property_json.get('bedrooms'),
                'bathrooms': property_json.get('bathrooms'),
                'square_feet': property_json.get('livingArea'),
                'lot_size': property_json.get('lotAreaValue'),
                'year_built': property_json.get('yearBuilt'),
                'property_type': property_json.get('homeType', ''),
                'sold_date': property_json.get('dateSold', ''),
                'days_on_market': property_json.get('daysOnZillow'),
                'zillow_url': property_json.get('detailUrl', ''),
                'county': self.get_county_for_city(address_parts.get('city', '')),
                'scraped_at': datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"Error extracting property data: {e}")
            return {}

    def get_county_for_city(self, city: str) -> str:
        """Determine county based on city name"""
        for county, cities in self.dfw_locations.items():
            for city_entry in cities:
                if city.lower() in city_entry.lower():
                    return county
        return 'Unknown County'

    def scrape_location(self, location: str, max_pages: int = 5) -> List[Dict]:
        """Scrape properties for a specific location"""
        properties = []
        logger.info(f"Scraping {location}...")
        
        for page in range(1, max_pages + 1):
            try:
                url = self.build_search_url(location, page)
                logger.info(f"Scraping page {page} of {location}")
                
                response = self.session.get(url, timeout=30)
                
                if response.status_code != 200:
                    logger.warning(f"HTTP {response.status_code} for {location} page {page}")
                    continue
                
                # Look for JSON data in the HTML response
                html = response.text
                
                # Find the JSON data that contains property listings
                start_marker = '"cat1":{"searchResults":{"listResults":'
                if start_marker not in html:
                    logger.warning(f"No property data found for {location} page {page}")
                    break
                
                start_idx = html.find(start_marker) + len('"cat1":{"searchResults":{"listResults":')
                end_idx = html.find(',"mapResults":', start_idx)
                
                if end_idx == -1:
                    logger.warning(f"Malformed JSON data for {location} page {page}")
                    continue
                
                json_str = html[start_idx:end_idx]
                property_data = json.loads(json_str)
                
                if not property_data:
                    logger.info(f"No more properties found for {location} at page {page}")
                    break
                
                # Extract property information
                page_properties = []
                for prop in property_data:
                    if isinstance(prop, dict) and 'address' in prop:
                        extracted_data = self.extract_property_data(prop)
                        if extracted_data:
                            page_properties.append(extracted_data)
                
                properties.extend(page_properties)
                logger.info(f"Found {len(page_properties)} properties on page {page} for {location}")
                
                # Rate limiting
                time.sleep(random.uniform(2, 5))
                
            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error for {location} page {page}: {e}")
                continue
            except Exception as e:
                logger.error(f"Error scraping {location} page {page}: {e}")
                continue
        
        logger.info(f"Total properties found for {location}: {len(properties)}")
        return properties

    def scrape_all_dfw(self, max_pages_per_location: int = 3) -> None:
        """Scrape all DFW counties and cities"""
        total_start_time = time.time()
        
        for county, cities in self.dfw_locations.items():
            logger.info(f"Starting {county}...")
            county_start_time = time.time()
            
            for city in cities:
                try:
                    city_properties = self.scrape_location(city, max_pages_per_location)
                    self.all_properties.extend(city_properties)
                    
                    # Longer delay between cities to avoid rate limiting
                    time.sleep(random.uniform(5, 10))
                    
                except Exception as e:
                    logger.error(f"Error scraping {city}: {e}")
                    continue
            
            county_duration = time.time() - county_start_time
            logger.info(f"Completed {county} in {county_duration:.2f} seconds")
            
            # Save progress after each county
            self.save_progress()
        
        total_duration = time.time() - total_start_time
        logger.info(f"Scraping completed in {total_duration:.2f} seconds")
        logger.info(f"Total properties found: {len(self.all_properties)}")

    def save_progress(self) -> None:
        """Save current progress to CSV"""
        if not self.all_properties:
            return
        
        filename = f"dfw_zillow_properties_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        # Get fieldnames from the first property
        fieldnames = list(self.all_properties[0].keys())
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(self.all_properties)
        
        logger.info(f"Progress saved to {filename} ({len(self.all_properties)} properties)")

    def get_summary_stats(self) -> Dict[str, Any]:
        """Get summary statistics of scraped data"""
        if not self.all_properties:
            return {}
        
        counties = {}
        cities = {}
        price_ranges = {'under_200k': 0, '200k_400k': 0, '400k_600k': 0, 'over_600k': 0}
        
        for prop in self.all_properties:
            # County stats
            county = prop.get('county', 'Unknown')
            counties[county] = counties.get(county, 0) + 1
            
            # City stats
            city = prop.get('city', 'Unknown')
            cities[city] = cities.get(city, 0) + 1
            
            # Price range stats
            price = prop.get('price', 0)
            if isinstance(price, (int, float)):
                if price < 200000:
                    price_ranges['under_200k'] += 1
                elif price < 400000:
                    price_ranges['200k_400k'] += 1
                elif price < 600000:
                    price_ranges['400k_600k'] += 1
                else:
                    price_ranges['over_600k'] += 1
        
        return {
            'total_properties': len(self.all_properties),
            'counties': counties,
            'top_cities': dict(sorted(cities.items(), key=lambda x: x[1], reverse=True)[:10]),
            'price_ranges': price_ranges,
            'scraped_at': datetime.now().isoformat()
        }


def main():
    """Main execution function"""
    logger.info("Starting DFW Zillow Scraper...")
    
    scraper = DFWZillowScraper()
    
    try:
        # Scrape all DFW locations (3 pages per city to avoid rate limiting)
        scraper.scrape_all_dfw(max_pages_per_location=3)
        
        # Final save
        scraper.save_progress()
        
        # Print summary
        stats = scraper.get_summary_stats()
        logger.info("Scraping Summary:")
        logger.info(f"Total Properties: {stats.get('total_properties', 0)}")
        logger.info(f"Counties: {stats.get('counties', {})}")
        logger.info(f"Top Cities: {stats.get('top_cities', {})}")
        logger.info(f"Price Ranges: {stats.get('price_ranges', {})}")
        
        return scraper.all_properties
        
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
        scraper.save_progress()
    except Exception as e:
        logger.error(f"Scraping failed: {e}")
        scraper.save_progress()


if __name__ == "__main__":
    main()