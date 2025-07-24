#!/usr/bin/env python3
"""
High-Performance Async Zillow Scraper
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

class AsyncZillowScraper(BaseAsyncScraper):
    """High-performance async Zillow scraper for DFW properties"""
    
    def __init__(self, max_concurrent: int = 100, requests_per_hour: int = 5000):
        config = ScraperConfig(
            name="async_zillow_scraper",
            max_concurrent=max_concurrent,
            requests_per_hour=requests_per_hour,
            retry_attempts=3,
            retry_backoff=1.5,
            timeout=30.0,
            incremental_enabled=True,
            cache_duration_hours=12  # Refresh Zillow data every 12 hours
        )
        super().__init__(config)
        
        # DFW-specific data
        self.dfw_zip_codes = list(get_dfw_zip_codes())[:50]  # Limit for demo
        self.dfw_counties = list(get_dfw_counties())
        
        logger.info(f"🏠 Async Zillow scraper initialized for {len(self.dfw_zip_codes)} ZIP codes")
    
    async def generate_urls(self) -> List[str]:
        """Generate Zillow search URLs for DFW area"""
        
        urls = []
        
        # Recent sales URLs by ZIP code
        for zip_code in self.dfw_zip_codes:
            # Sold in last 90 days
            url = f"https://www.zillow.com/homes/{zip_code}_rb/sold_type/0-_price/globalrelevanceex_sort/1_p/"
            urls.append(url)
            
            # For sale URLs
            url = f"https://www.zillow.com/homes/{zip_code}_rb/for_sale_type/0-_price/globalrelevanceex_sort/1_p/"
            urls.append(url)
        
        # City-based searches for major DFW cities
        major_cities = [
            'Dallas-TX', 'Fort-Worth-TX', 'Arlington-TX', 'Plano-TX', 
            'Irving-TX', 'Garland-TX', 'McKinney-TX', 'Frisco-TX'
        ]
        
        for city in major_cities:
            # Recently sold
            url = f"https://www.zillow.com/{city}/sold/"
            urls.append(url)
            
            # For sale
            url = f"https://www.zillow.com/{city}/"
            urls.append(url)
        
        logger.info(f"📍 Generated {len(urls)} Zillow URLs")
        return urls
    
    async def extract_data(self, content: str, url: str) -> List[Dict]:
        """Extract property data from Zillow page content"""
        
        try:
            properties = []
            
            # Parse HTML content
            soup = BeautifulSoup(content, 'html.parser')
            
            # Method 1: Extract from JSON-LD structured data
            json_scripts = soup.find_all('script', type='application/ld+json')
            for script in json_scripts:
                try:
                    json_data = json.loads(script.string)
                    if isinstance(json_data, list):
                        for item in json_data:
                            if item.get('@type') == 'Product':
                                property_data = await self.parse_structured_data(item, url)
                                if property_data:
                                    properties.append(property_data)
                except (json.JSONDecodeError, KeyError):
                    continue
            
            # Method 2: Extract from React component data
            react_scripts = soup.find_all('script', string=lambda text: text and 'window.__SERVER_DATA__' in text)
            for script in react_scripts:
                try:
                    # Extract JSON from JavaScript
                    script_content = script.string
                    json_match = re.search(r'window\.__SERVER_DATA__\s*=\s*({.*?});', script_content, re.DOTALL)
                    if json_match:
                        server_data = json.loads(json_match.group(1))
                        await self.parse_server_data(server_data, properties, url)
                except (json.JSONDecodeError, AttributeError):
                    continue
            
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
    
    async def parse_structured_data(self, json_data: Dict, url: str) -> Optional[Dict]:
        """Parse structured JSON-LD data"""
        
        try:
            offers = json_data.get('offers', {})
            if not offers:
                return None
            
            # Extract basic property information
            property_data = {
                'source': 'zillow',
                'source_url': url,
                'scraped_at': datetime.now().isoformat(),
                
                # Property details
                'name': json_data.get('name', ''),
                'description': json_data.get('description', ''),
                'url': json_data.get('url', ''),
                
                # Pricing
                'price': self.extract_numeric_value(offers.get('price', 0)),
                'price_currency': offers.get('priceCurrency', 'USD'),
                
                # Additional structured data
                'availability': offers.get('availability', ''),
                'valid_through': offers.get('validThrough', ''),
            }
            
            return property_data
            
        except Exception as e:
            logger.debug(f"Error parsing structured data: {e}")
            return None
    
    async def parse_server_data(self, server_data: Dict, properties: List[Dict], url: str):
        """Parse Zillow server-side data"""
        
        try:
            # Navigate through the nested structure to find property listings
            if 'searchResults' in server_data:
                search_results = server_data['searchResults']
                if 'listResults' in search_results:
                    for listing in search_results['listResults']:
                        property_data = await self.parse_listing_data(listing, url)
                        if property_data:
                            properties.append(property_data)
            
            # Alternative structure
            if 'props' in server_data:
                props = server_data['props']
                if 'pageProps' in props and 'searchPageState' in props['pageProps']:
                    search_state = props['pageProps']['searchPageState']
                    if 'cat1' in search_state and 'searchResults' in search_state['cat1']:
                        for listing in search_state['cat1']['searchResults'].get('listResults', []):
                            property_data = await self.parse_listing_data(listing, url)
                            if property_data:
                                properties.append(property_data)
                                
        except Exception as e:
            logger.debug(f"Error parsing server data: {e}")
    
    async def parse_listing_data(self, listing: Dict, url: str) -> Optional[Dict]:
        """Parse individual listing data from Zillow API response"""
        
        try:
            property_data = {
                'source': 'zillow',
                'source_url': url,
                'scraped_at': datetime.now().isoformat(),
                
                # Property identification
                'zpid': listing.get('zpid'),
                'mls_id': listing.get('mlsid'),
                'listing_id': listing.get('listingId'),
                
                # Address information
                'address': listing.get('address', ''),
                'street_address': listing.get('streetAddress', ''),
                'city': listing.get('city', ''),
                'state': listing.get('state', ''),
                'zip_code': listing.get('zipcode'),
                'county': listing.get('county', ''),
                
                # Property details
                'price': self.extract_numeric_value(listing.get('price')),
                'bedrooms': listing.get('bedrooms'),
                'bathrooms': listing.get('bathrooms'),
                'square_feet': self.extract_numeric_value(listing.get('livingArea')),
                'lot_size': self.extract_numeric_value(listing.get('lotAreaValue')),
                'year_built': listing.get('yearBuilt'),
                'property_type': listing.get('homeType', ''),
                
                # Pricing and market data
                'price_per_sqft': listing.get('pricePerSquareFoot'),
                'zestimate': self.extract_numeric_value(listing.get('zestimate')),
                'rent_zestimate': self.extract_numeric_value(listing.get('rentZestimate')),
                
                # Listing information
                'listing_status': listing.get('homeStatus', ''),
                'days_on_zillow': listing.get('daysOnZillow'),
                'date_sold': listing.get('dateSold'),
                'listing_date': listing.get('datePosted'),
                
                # Location data
                'latitude': listing.get('latitude'),
                'longitude': listing.get('longitude'),
                
                # Additional features
                'has_garage': listing.get('hasGarage', False),
                'parking_spaces': listing.get('parkingSpaces'),
                'hoa_fee': self.extract_numeric_value(listing.get('monthlyHoaFee')),
                
                # Photos and media
                'photo_count': len(listing.get('photos', [])),
                'has_virtual_tour': bool(listing.get('hasVirtualTour', False)),
                
                # Market insights
                'price_change': self.extract_numeric_value(listing.get('priceChange')),
                'price_change_date': listing.get('priceChangeDate'),
            }
            
            # Calculate lead score
            property_data['lead_score'] = self.calculate_lead_score(property_data)
            
            return property_data
            
        except Exception as e:
            logger.debug(f"Error parsing listing data: {e}")
            return None
    
    async def parse_html_listings(self, soup: BeautifulSoup, url: str) -> List[Dict]:
        """Fallback HTML parsing for property listings"""
        
        properties = []
        
        try:
            # Look for property cards/listings in HTML
            listing_selectors = [
                'article[data-test="property-card"]',
                '.list-card',
                '.property-card',
                '[data-test="list-card"]'
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
        """Parse individual property card from HTML"""
        
        try:
            property_data = {
                'source': 'zillow',
                'source_url': url,
                'scraped_at': datetime.now().isoformat(),
            }
            
            # Extract price
            price_elem = listing_element.select_one('[data-test="property-card-price"], .list-card-price')
            if price_elem:
                property_data['price'] = self.extract_numeric_value(price_elem.get_text())
            
            # Extract address
            address_elem = listing_element.select_one('[data-test="property-card-addr"], .list-card-addr')
            if address_elem:
                property_data['address'] = address_elem.get_text().strip()
            
            # Extract property details (beds, baths, sqft)
            details_elem = listing_element.select_one('[data-test="property-card-details"], .list-card-details')
            if details_elem:
                details_text = details_elem.get_text()
                
                # Parse beds
                bed_match = re.search(r'(\d+)\s*bds?', details_text, re.I)
                if bed_match:
                    property_data['bedrooms'] = int(bed_match.group(1))
                
                # Parse baths  
                bath_match = re.search(r'(\d+(?:\.\d+)?)\s*ba', details_text, re.I)
                if bath_match:
                    property_data['bathrooms'] = float(bath_match.group(1))
                
                # Parse square feet
                sqft_match = re.search(r'([\d,]+)\s*sqft', details_text, re.I)
                if sqft_match:
                    property_data['square_feet'] = self.extract_numeric_value(sqft_match.group(1))
            
            # Extract property link
            link_elem = listing_element.select_one('a[href*="/homedetails/"]')
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
            # Ensure required fields
            if not property_data.get('price') and not property_data.get('zestimate'):
                return None
            
            # Standardize address format
            if property_data.get('address'):
                property_data['address_normalized'] = self.normalize_address(property_data['address'])
            
            # Extract city, state, zip from address if missing
            if property_data.get('address') and not property_data.get('city'):
                address_parts = self.parse_address_components(property_data['address'])
                property_data.update(address_parts)
            
            # Ensure numeric fields are properly typed
            numeric_fields = ['price', 'bedrooms', 'bathrooms', 'square_feet', 'lot_size', 'year_built']
            for field in numeric_fields:
                if field in property_data:
                    property_data[field] = self.extract_numeric_value(property_data[field])
            
            # Add derived fields
            property_data['price_per_sqft_calculated'] = self.calculate_price_per_sqft(
                property_data.get('price'), 
                property_data.get('square_feet')
            )
            
            # Add property age
            if property_data.get('year_built'):
                property_data['property_age'] = datetime.now().year - int(property_data['year_built'])
            
            # Ensure lead score is calculated
            if 'lead_score' not in property_data:
                property_data['lead_score'] = self.calculate_lead_score(property_data)
            
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
            # Remove common currency symbols and formatting
            cleaned = re.sub(r'[^\d.]', '', value.replace(',', ''))
            try:
                return float(cleaned) if cleaned else None
            except ValueError:
                return None
        
        return None
    
    def normalize_address(self, address: str) -> str:
        """Normalize address format"""
        
        # Basic address normalization
        normalized = address.strip()
        normalized = re.sub(r'\s+', ' ', normalized)  # Multiple spaces to single
        normalized = re.sub(r',\s*,', ',', normalized)  # Double commas
        
        return normalized
    
    def parse_address_components(self, address: str) -> Dict[str, str]:
        """Parse city, state, zip from address string"""
        
        components = {}
        
        # Basic regex patterns for address parsing
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
            price = property_data.get('price') or property_data.get('zestimate', 0)
            year_built = property_data.get('year_built')
            
            # Price-based scoring
            if price:
                if price > 500000:
                    score += 3
                elif price > 350000:
                    score += 2
                elif price > 250000:
                    score += 1
            
            # Age-based scoring (older homes more likely to need roofing)
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
            
            # Size scoring
            sqft = property_data.get('square_feet')
            if sqft and sqft > 2500:
                score += 1
            
            return min(max(score, 1), 10)  # Ensure 1-10 range
            
        except Exception:
            return 5
    
    def get_supabase_table(self) -> str:
        """Get Supabase table name for Zillow data"""
        return 'zillow_leads'

async def main():
    """Main function to run the async Zillow scraper"""
    
    logger.info("🚀 Starting High-Performance Async Zillow Scraper")
    
    try:
        # Initialize scraper with high-performance settings
        scraper = AsyncZillowScraper(
            max_concurrent=100,  # 100 concurrent requests
            requests_per_hour=5000  # Target 5K requests per hour
        )
        
        # Run the scraper
        results = await scraper.run_scraper()
        
        # Display results
        logger.info("📊 ZILLOW SCRAPING RESULTS:")
        logger.info(f"   • Total Runtime: {results['total_runtime']:.1f} seconds")
        logger.info(f"   • Requests Made: {results['total_requests']}")
        logger.info(f"   • Success Rate: {results['success_rate_percent']:.1f}%")
        logger.info(f"   • Requests/Second: {results['requests_per_second']:.2f}")
        logger.info(f"   • Requests/Hour: {results['requests_per_hour']:.0f}")
        logger.info(f"   • Leads Extracted: {results['leads_extracted']}")
        logger.info(f"   • Leads Saved: {results['leads_saved']}")
        logger.info(f"   • Cache Hits: {results['cache_hits']}")
        
        # ScraperAPI usage stats
        logger.info("🔑 ScraperAPI Usage:")
        for key, usage in results['key_usage_distribution'].items():
            key_masked = key[:8] + "..." if len(key) > 8 else key
            logger.info(f"   • {key_masked}: {usage} requests")
        
        logger.info("✅ Zillow scraping completed successfully!")
        
        return results
        
    except Exception as e:
        logger.error(f"❌ Zillow scraping failed: {e}")
        raise

if __name__ == "__main__":
    # Run the async scraper
    asyncio.run(main())