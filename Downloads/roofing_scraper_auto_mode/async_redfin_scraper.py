#!/usr/bin/env python3
"""
High-Performance Async Redfin Scraper
Supports 5,000+ requests/hour with asyncio, ScraperAPI integration,
intelligent retries, incremental fetching, and direct Supabase integration
"""

import asyncio
import json
import re
from datetime import datetime
from typing import List, Dict, Optional
from bs4 import BeautifulSoup
import logging

from async_scraper_framework import BaseAsyncScraper, ScraperConfig
from dfw_geo_filter import get_dfw_zip_codes, get_dfw_counties

logger = logging.getLogger(__name__)

class AsyncRedfinScraper(BaseAsyncScraper):
    """High-performance async Redfin scraper for DFW properties"""
    
    def __init__(self, max_concurrent: int = 100, requests_per_hour: int = 5000):
        config = ScraperConfig(
            name="async_redfin_scraper",
            max_concurrent=max_concurrent,
            requests_per_hour=requests_per_hour,
            retry_attempts=3,
            retry_backoff=1.5,
            timeout=30.0,
            incremental_enabled=True,
            cache_duration_hours=12  # Refresh Redfin data every 12 hours
        )
        super().__init__(config)
        
        # DFW-specific data
        self.dfw_zip_codes = list(get_dfw_zip_codes())[:50]  # Limit for demo
        self.major_cities = [
            'Dallas', 'Fort-Worth', 'Arlington', 'Plano', 
            'Irving', 'Garland', 'McKinney', 'Frisco'
        ]
        
        logger.info(f"🏠 Async Redfin scraper initialized for {len(self.dfw_zip_codes)} ZIP codes")
    
    async def generate_urls(self) -> List[str]:
        """Generate Redfin search URLs for DFW area"""
        
        urls = []
        
        # City-based searches
        for city in self.major_cities:
            city_slug = city.lower().replace(' ', '-')
            
            # Recently sold properties
            url = f"https://www.redfin.com/city/{city_slug}/filter/sold-7da"
            urls.append(url)
            
            # Active listings
            url = f"https://www.redfin.com/city/{city_slug}/filter/property-type=house"
            urls.append(url)
            
            # Price range filters for better targeting
            for price_min, price_max in [(250000, 500000), (500000, 1000000)]:
                url = f"https://www.redfin.com/city/{city_slug}/filter/min-price={price_min},max-price={price_max}"
                urls.append(url)
        
        # ZIP code searches for precise targeting
        for zip_code in self.dfw_zip_codes[:25]:  # Limit to 25 ZIP codes
            url = f"https://www.redfin.com/zipcode/{zip_code}/filter/property-type=house"
            urls.append(url)
        
        # County-level searches
        dfw_counties_clean = ['Dallas', 'Tarrant', 'Collin', 'Denton']
        for county in dfw_counties_clean:
            url = f"https://www.redfin.com/county/{county}-County-TX/filter/property-type=house"
            urls.append(url)
        
        logger.info(f"📍 Generated {len(urls)} Redfin URLs")
        return urls
    
    async def extract_data(self, content: str, url: str) -> List[Dict]:
        """Extract property data from Redfin page content"""
        
        try:
            properties = []
            
            # Parse HTML content
            soup = BeautifulSoup(content, 'html.parser')
            
            # Method 1: Extract from Redfin's React component data
            await self.extract_from_react_data(soup, properties, url)
            
            # Method 2: Extract from API endpoints embedded in page
            await self.extract_from_api_data(soup, properties, url)
            
            # Method 3: Fallback to HTML parsing
            if not properties:
                properties = await self.parse_html_listings(soup, url)
            
            # Filter and enhance properties
            enhanced_properties = []
            for prop in properties:
                enhanced_prop = await self.enhance_property_data(prop, url)
                if enhanced_prop:
                    enhanced_properties.append(enhanced_prop)
            
            logger.debug(f"✅ Extracted {len(enhanced_properties)} properties from {url}")
            return enhanced_properties
            
        except Exception as e:
            logger.error(f"❌ Error extracting data from {url}: {e}")
            return []
    
    async def extract_from_react_data(self, soup: BeautifulSoup, properties: List[Dict], url: str):
        """Extract data from Redfin's React component data"""
        
        try:
            # Look for Redfin's initial page data
            scripts = soup.find_all('script')
            
            for script in scripts:
                if script.string and 'window.__INITIAL_STATE__' in script.string:
                    # Extract React initial state
                    script_content = script.string
                    json_match = re.search(r'window\.__INITIAL_STATE__\s*=\s*({.*?});', script_content, re.DOTALL)
                    
                    if json_match:
                        try:
                            initial_state = json.loads(json_match.group(1))
                            await self.parse_redfin_state(initial_state, properties, url)
                        except json.JSONDecodeError:
                            continue
                
                elif script.string and 'window.reactBootstrap' in script.string:
                    # Alternative React bootstrap data
                    script_content = script.string
                    json_match = re.search(r'window\.reactBootstrap\s*=\s*({.*?});', script_content, re.DOTALL)
                    
                    if json_match:
                        try:
                            bootstrap_data = json.loads(json_match.group(1))
                            await self.parse_bootstrap_data(bootstrap_data, properties, url)
                        except json.JSONDecodeError:
                            continue
                            
        except Exception as e:
            logger.debug(f"Error extracting React data: {e}")
    
    async def extract_from_api_data(self, soup: BeautifulSoup, properties: List[Dict], url: str):
        """Extract data from embedded API responses"""
        
        try:
            # Look for embedded JSON data
            json_scripts = soup.find_all('script', type='application/json')
            
            for script in json_scripts:
                try:
                    json_data = json.loads(script.string)
                    
                    # Look for property listings in various data structures  
                    if 'homes' in json_data:
                        for home in json_data['homes']:
                            property_data = await self.parse_redfin_home_data(home, url)
                            if property_data:
                                properties.append(property_data)
                    
                    elif 'props' in json_data and 'pageProps' in json_data['props']:
                        page_props = json_data['props']['pageProps']
                        if 'searchResults' in page_props:
                            for result in page_props['searchResults'].get('homes', []):
                                property_data = await self.parse_redfin_home_data(result, url)
                                if property_data:
                                    properties.append(property_data)
                                    
                except (json.JSONDecodeError, KeyError):
                    continue
                    
        except Exception as e:
            logger.debug(f"Error extracting API data: {e}")
    
    async def parse_redfin_state(self, state_data: Dict, properties: List[Dict], url: str):
        """Parse Redfin initial state data"""
        
        try:
            # Navigate through Redfin's state structure
            if 'searchPage' in state_data:
                search_page = state_data['searchPage']
                
                if 'searchResults' in search_page:
                    search_results = search_page['searchResults']
                    
                    for result in search_results.get('listings', []):
                        property_data = await self.parse_redfin_listing(result, url)
                        if property_data:
                            properties.append(property_data)
            
            # Alternative structure
            if 'page' in state_data and 'cluster' in state_data['page']:
                cluster_data = state_data['page']['cluster']
                
                for cluster in cluster_data.get('listings', []):
                    property_data = await self.parse_redfin_listing(cluster, url)
                    if property_data:
                        properties.append(property_data)
                        
        except Exception as e:
            logger.debug(f"Error parsing Redfin state: {e}")
    
    async def parse_bootstrap_data(self, bootstrap_data: Dict, properties: List[Dict], url: str):
        """Parse Redfin bootstrap data"""
        
        try:
            if 'SearchPage' in bootstrap_data:
                search_page = bootstrap_data['SearchPage']
                
                if 'searchResults' in search_page:
                    for result in search_page['searchResults'].get('homes', []):
                        property_data = await self.parse_redfin_home_data(result, url)
                        if property_data:
                            properties.append(property_data)
                            
        except Exception as e:
            logger.debug(f"Error parsing bootstrap data: {e}")
    
    async def parse_redfin_listing(self, listing: Dict, url: str) -> Optional[Dict]:
        """Parse individual Redfin listing from API data"""
        
        try:
            property_data = {
                'source': 'redfin',
                'source_url': url,
                'scraped_at': datetime.now().isoformat(),
                
                # Property identification
                'redfin_property_id': listing.get('propertyId'),
                'mls_number': listing.get('mlsId'),
                'listing_id': listing.get('listingId'),
                
                # Address information
                'address': listing.get('streetLine', {}).get('value', ''),
                'city': listing.get('city'),
                'state': listing.get('state'),
                'zip_code': listing.get('zip'),
                'county': listing.get('county'),
                
                # Property details
                'price': self.extract_numeric_value(listing.get('price')),
                'bedrooms': listing.get('beds'),
                'bathrooms': listing.get('baths'),
                'square_feet': self.extract_numeric_value(listing.get('sqFt')),
                'lot_size': self.extract_numeric_value(listing.get('lotSize')),
                'year_built': listing.get('yearBuilt'),
                'property_type': listing.get('propertyType', ''),
                
                # Pricing information
                'price_per_sqft': self.extract_numeric_value(listing.get('pricePerSqFt')),
                'hoa_fee': self.extract_numeric_value(listing.get('hoa')),
                
                # Listing status
                'status': listing.get('homeStatus', ''),
                'days_on_redfin': listing.get('dom'),  # Days on market
                'listing_date': listing.get('listingRemarks', {}).get('datePosted'),
                'sold_date': listing.get('soldDate'),
                
                # Location coordinates
                'latitude': listing.get('latLong', {}).get('latitude'),
                'longitude': listing.get('latLong', {}).get('longitude'),
                
                # Additional features
                'has_photos': bool(listing.get('hasPhotos', False)),
                'photo_count': len(listing.get('photos', [])),
                'has_virtual_tour': bool(listing.get('hasVirtualTour', False)),
                'has_3d_walkthrough': bool(listing.get('has3DWalkthrough', False)),
                
                # Market data
                'price_change': self.extract_numeric_value(listing.get('priceChange')),
                'original_list_price': self.extract_numeric_value(listing.get('originalListPrice')),
                
                # Property features
                'parking_spaces': listing.get('parkingSpaces'),
                'garage_spaces': listing.get('garageSpaces'),
                'stories': listing.get('stories'),
                
                # URL for property details
                'property_url': listing.get('url'),
            }
            
            # Calculate lead score
            property_data['lead_score'] = self.calculate_lead_score(property_data)
            
            return property_data
            
        except Exception as e:
            logger.debug(f"Error parsing Redfin listing: {e}")
            return None
    
    async def parse_redfin_home_data(self, home_data: Dict, url: str) -> Optional[Dict]:
        """Parse home data from Redfin API response"""
        
        try:
            property_data = {
                'source': 'redfin',
                'source_url': url,
                'scraped_at': datetime.now().isoformat(),
                
                # Basic info
                'redfin_property_id': home_data.get('homeId'),
                'mls_number': home_data.get('mlsNumber'),
                
                # Address
                'address': home_data.get('address'),
                'city': home_data.get('city'),
                'state': home_data.get('state'), 
                'zip_code': home_data.get('postalCode'),
                
                # Property details
                'price': self.extract_numeric_value(home_data.get('price')),
                'bedrooms': home_data.get('beds'),
                'bathrooms': home_data.get('baths'),
                'square_feet': self.extract_numeric_value(home_data.get('sqft')),
                'lot_size': self.extract_numeric_value(home_data.get('lotSqft')),
                'year_built': home_data.get('yearBuilt'),
                'property_type': home_data.get('homeType'),
                
                # Market info
                'status': home_data.get('listingStatus'),
                'days_on_market': home_data.get('daysOnMarket'),
                'price_per_sqft': self.extract_numeric_value(home_data.get('pricePerSqFt')),
                
                # Location
                'latitude': home_data.get('latitude'),
                'longitude': home_data.get('longitude'),
            }
            
            # Calculate lead score
            property_data['lead_score'] = self.calculate_lead_score(property_data)
            
            return property_data
            
        except Exception as e:
            logger.debug(f"Error parsing home data: {e}")
            return None
    
    async def parse_html_listings(self, soup: BeautifulSoup, url: str) -> List[Dict]:
        """Fallback HTML parsing for Redfin property listings"""
        
        properties = []
        
        try:
            # Redfin-specific selectors
            listing_selectors = [
                '.HomeCard',
                '.home-card',
                '[data-rf-test-id="home-card"]',
                '.HomeCardContainer'
            ]
            
            for selector in listing_selectors:
                listings = soup.select(selector)
                if listings:
                    for listing in listings:
                        property_data = await self.parse_html_listing_card(listing, url)
                        if property_data:
                            properties.append(property_data)
                    break
                    
        except Exception as e:
            logger.debug(f"Error parsing HTML listings: {e}")
        
        return properties
    
    async def parse_html_listing_card(self, listing_element, url: str) -> Optional[Dict]:
        """Parse individual Redfin property card from HTML"""
        
        try:
            property_data = {
                'source': 'redfin',
                'source_url': url,
                'scraped_at': datetime.now().isoformat(),
            }
            
            # Extract price
            price_elem = listing_element.select_one('.HomeStatsV2 .price, .home-card-price')
            if price_elem:
                property_data['price'] = self.extract_numeric_value(price_elem.get_text())
            
            # Extract address
            address_elem = listing_element.select_one('.homeAddress, .home-card-address')
            if address_elem:
                property_data['address'] = address_elem.get_text().strip()
            
            # Extract property details
            stats_elem = listing_element.select_one('.HomeStats, .home-card-stats')
            if stats_elem:
                stats_text = stats_elem.get_text()
                
                # Parse beds
                bed_match = re.search(r'(\d+)\s*bed', stats_text, re.I)
                if bed_match:
                    property_data['bedrooms'] = int(bed_match.group(1))
                
                # Parse baths
                bath_match = re.search(r'(\d+(?:\.\d+)?)\s*bath', stats_text, re.I)
                if bath_match:
                    property_data['bathrooms'] = float(bath_match.group(1))
                
                # Parse square feet
                sqft_match = re.search(r'([\d,]+)\s*sq\.?\s*ft', stats_text, re.I)
                if sqft_match:
                    property_data['square_feet'] = self.extract_numeric_value(sqft_match.group(1))
            
            # Extract property URL
            link_elem = listing_element.select_one('a[href*="/home/"]')
            if link_elem:
                property_data['property_url'] = link_elem.get('href')
            
            # Calculate lead score
            property_data['lead_score'] = self.calculate_lead_score(property_data)
            
            return property_data if property_data.get('price') else None
            
        except Exception as e:
            logger.debug(f"Error parsing HTML listing card: {e}")
            return None
    
    async def enhance_property_data(self, property_data: Dict, source_url: str) -> Optional[Dict]:
        """Enhance property data with additional processing"""
        
        try:
            # Skip if no price information
            if not property_data.get('price'):
                return None
            
            # Standardize address format
            if property_data.get('address'):
                property_data['address_normalized'] = self.normalize_address(property_data['address'])
            
            # Parse address components if missing
            if property_data.get('address') and not property_data.get('city'):
                address_parts = self.parse_address_components(property_data['address'])
                property_data.update(address_parts)
            
            # Ensure numeric fields are properly typed
            numeric_fields = ['price', 'bedrooms', 'bathrooms', 'square_feet', 'lot_size', 'year_built']
            for field in numeric_fields:
                if field in property_data:
                    property_data[field] = self.extract_numeric_value(property_data[field])
            
            # Calculate derived fields
            property_data['price_per_sqft_calculated'] = self.calculate_price_per_sqft(
                property_data.get('price'), 
                property_data.get('square_feet')
            )
            
            # Add property age
            if property_data.get('year_built'):
                property_data['property_age'] = datetime.now().year - int(property_data['year_built'])
            
            # Ensure all required fields for Supabase
            property_data['address_text'] = property_data.get('address', '')
            property_data['zip_code'] = property_data.get('zip_code', '')
            
            return property_data
            
        except Exception as e:
            logger.debug(f"Error enhancing property data: {e}")
            return property_data
    
    def extract_numeric_value(self, value) -> Optional[float]:
        """Extract numeric value from string or return None"""
        
        if value is None:
            return None
        
        if isinstance(value, (int, float)):
            return float(value)
        
        if isinstance(value, str):
            # Remove common symbols and formatting
            cleaned = re.sub(r'[^\d.]', '', value.replace(',', ''))
            try:
                return float(cleaned) if cleaned else None
            except ValueError:
                return None
        
        return None
    
    def normalize_address(self, address: str) -> str:
        """Normalize address format"""
        
        normalized = address.strip()
        normalized = re.sub(r'\s+', ' ', normalized)
        normalized = re.sub(r',\s*,', ',', normalized)
        
        return normalized
    
    def parse_address_components(self, address: str) -> Dict[str, str]:
        """Parse city, state, zip from address string"""
        
        components = {}
        
        # Format: "123 Main St, Dallas, TX 75201"
        match = re.search(r',\s*([^,]+),\s*([A-Z]{2})\s*(\d{5})', address)
        if match:
            components['city'] = match.group(1).strip()
            components['state'] = match.group(2).strip()
            components['zip_code'] = match.group(3).strip()
        
        return components
    
    def calculate_price_per_sqft(self, price: Optional[float], sqft: Optional[float]) -> Optional[float]:
        """Calculate price per square foot"""
        
        if price and sqft and sqft > 0:
            return round(price / sqft, 2)
        
        return None
    
    def calculate_lead_score(self, property_data: Dict) -> int:
        """Calculate lead quality score (1-10)"""
        
        score = 5  # Base score
        
        try:
            price = property_data.get('price', 0)
            year_built = property_data.get('year_built')
            
            # Price-based scoring
            if price:
                if price > 500000:
                    score += 3
                elif price > 350000:
                    score += 2
                elif price > 250000:
                    score += 1
            
            # Age-based scoring
            if year_built:
                age = datetime.now().year - int(year_built)
                if age > 15:
                    score += 3
                elif age > 10:
                    score += 2
                elif age > 5:
                    score += 1
            
            # Property type scoring
            prop_type = property_data.get('property_type', '').lower()
            if 'single' in prop_type or 'house' in prop_type:
                score += 1
            
            return min(max(score, 1), 10)
            
        except Exception:
            return 5
    
    def get_supabase_table(self) -> str:
        """Get Supabase table name for Redfin data"""
        return 'redfin_leads'

async def main():
    """Main function to run the async Redfin scraper"""
    
    logger.info("🚀 Starting High-Performance Async Redfin Scraper")
    
    try:
        # Initialize scraper
        scraper = AsyncRedfinScraper(
            max_concurrent=100,
            requests_per_hour=5000
        )
        
        # Run the scraper
        results = await scraper.run_scraper()
        
        # Display results
        logger.info("📊 REDFIN SCRAPING RESULTS:")
        logger.info(f"   • Total Runtime: {results['total_runtime']:.1f} seconds")
        logger.info(f"   • Requests Made: {results['total_requests']}")
        logger.info(f"   • Success Rate: {results['success_rate_percent']:.1f}%")
        logger.info(f"   • Requests/Second: {results['requests_per_second']:.2f}")
        logger.info(f"   • Requests/Hour: {results['requests_per_hour']:.0f}")
        logger.info(f"   • Leads Extracted: {results['leads_extracted']}")
        logger.info(f"   • Leads Saved: {results['leads_saved']}")
        logger.info(f"   • Cache Hits: {results['cache_hits']}")
        
        logger.info("✅ Redfin scraping completed successfully!")
        
        return results
        
    except Exception as e:
        logger.error(f"❌ Redfin scraping failed: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())