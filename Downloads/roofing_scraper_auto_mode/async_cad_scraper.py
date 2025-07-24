#!/usr/bin/env python3
"""
High-Performance Async CAD (County/City) Scraper
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

class AsyncCADScraper(BaseAsyncScraper):
    """High-performance async CAD scraper for DFW property records"""
    
    def __init__(self, max_concurrent: int = 100, requests_per_hour: int = 5000):
        config = ScraperConfig(
            name="async_cad_scraper",
            max_concurrent=max_concurrent,
            requests_per_hour=requests_per_hour,
            retry_attempts=4,  # CAD sites can be flaky
            retry_backoff=2.0,
            timeout=45.0,  # CAD sites can be slow
            incremental_enabled=True,
            cache_duration_hours=24  # CAD data changes less frequently
        )
        super().__init__(config)
        
        # DFW CAD office data
        self.dfw_cad_offices = {
            'Dallas': {
                'base_url': 'https://www.dallascad.org',
                'search_url': 'https://www.dallascad.org/PropertySearch/search.aspx',
                'counties': ['Dallas']
            },
            'Tarrant': {
                'base_url': 'https://www.tad.org',
                'search_url': 'https://www.tad.org/PropSearch/search.aspx',
                'counties': ['Tarrant']
            },
            'Collin': {
                'base_url': 'https://www.collincad.org',
                'search_url': 'https://www.collincad.org/Property-Search',
                'counties': ['Collin']
            },
            'Denton': {
                'base_url': 'https://www.dentoncad.com',
                'search_url': 'https://www.dentoncad.com/property-search',
                'counties': ['Denton']
            }
        }
        
        self.dfw_zip_codes = list(get_dfw_zip_codes())[:100]  # Limit for CAD searches
        
        logger.info(f"🏛️ Async CAD scraper initialized for {len(self.dfw_cad_offices)} counties")
    
    async def generate_urls(self) -> List[str]:
        """Generate CAD search URLs for DFW area"""
        
        urls = []
        
        # Property search URLs by county and ZIP code
        for cad_name, cad_info in self.dfw_cad_offices.items():
            base_url = cad_info['base_url']
            
            # Recent property transfers and sales
            for zip_code in self.dfw_zip_codes[:25]:  # Limit ZIP codes per county
                # Different CAD systems have different URL patterns
                if 'dallas' in cad_name.lower():
                    url = f"{base_url}/PropertySearch/Results?zipcode={zip_code}&saleDate=recent"
                    urls.append(url)
                    
                    # High-value properties
                    url = f"{base_url}/PropertySearch/Results?zipcode={zip_code}&minValue=300000"
                    urls.append(url)
                    
                elif 'tarrant' in cad_name.lower():
                    url = f"{base_url}/PropSearch/Results?postal={zip_code}&recentSale=90"
                    urls.append(url)
                    
                    # Property type filters
                    url = f"{base_url}/PropSearch/Results?postal={zip_code}&propType=residential"
                    urls.append(url)
                    
                elif 'collin' in cad_name.lower():
                    url = f"{base_url}/Property-Search/Results?zip={zip_code}&soldRecent=true"
                    urls.append(url)
                    
                    # Value range targeting
                    url = f"{base_url}/Property-Search/Results?zip={zip_code}&minValue=250000"
                    urls.append(url)
                    
                elif 'denton' in cad_name.lower():
                    url = f"{base_url}/property-search/results?zipCode={zip_code}&recentActivity=true"
                    urls.append(url)
            
            # County-wide searches for recent activity
            url = f"{base_url}/search/recent-transfers"
            urls.append(url)
            
            # Property improvement/permit correlations
            url = f"{base_url}/search/building-permits"
            urls.append(url)
        
        logger.info(f"🏛️ Generated {len(urls)} CAD URLs")
        return urls
    
    async def extract_data(self, content: str, url: str) -> List[Dict]:
        """Extract property data from CAD page content"""
        
        try:
            properties = []
            
            # Parse HTML content
            soup = BeautifulSoup(content, 'html.parser')
            
            # Determine CAD system type from URL
            cad_type = self.identify_cad_system(url)
            
            # Extract data based on CAD system
            if cad_type == 'dallas':
                properties = await self.extract_dallas_cad_data(soup, url)
            elif cad_type == 'tarrant':
                properties = await self.extract_tarrant_cad_data(soup, url)
            elif cad_type == 'collin':
                properties = await self.extract_collin_cad_data(soup, url)
            elif cad_type == 'denton':
                properties = await self.extract_denton_cad_data(soup, url)
            else:
                # Generic CAD parsing fallback
                properties = await self.extract_generic_cad_data(soup, url)
            
            # Enhance and filter properties
            enhanced_properties = []
            for prop in properties:
                enhanced_prop = await self.enhance_cad_property_data(prop, url)
                if enhanced_prop:
                    enhanced_properties.append(enhanced_prop)
            
            logger.debug(f"✅ Extracted {len(enhanced_properties)} CAD properties from {url}")
            return enhanced_properties
            
        except Exception as e:
            logger.error(f"❌ Error extracting CAD data from {url}: {e}")
            return []
    
    def identify_cad_system(self, url: str) -> str:
        """Identify which CAD system based on URL"""
        
        url_lower = url.lower()
        
        if 'dallas' in url_lower:
            return 'dallas'
        elif 'tarrant' in url_lower or 'tad.org' in url_lower:
            return 'tarrant'
        elif 'collin' in url_lower:
            return 'collin'
        elif 'denton' in url_lower:
            return 'denton'
        else:
            return 'generic'
    
    async def extract_dallas_cad_data(self, soup: BeautifulSoup, url: str) -> List[Dict]:
        """Extract data from Dallas CAD system"""
        
        properties = []
        
        try:
            # Dallas CAD specific selectors
            property_rows = soup.select('tr.PropertyRow, .property-record, .search-result')
            
            for row in property_rows:
                property_data = {
                    'source': 'dallas_cad',
                    'source_url': url,
                    'scraped_at': datetime.now().isoformat(),
                    'county': 'Dallas'
                }
                
                # Extract property ID
                id_elem = row.select_one('.PropertyID, [data-property-id]')
                if id_elem:
                    property_data['cad_property_id'] = id_elem.get_text().strip()
                
                # Extract address
                addr_elem = row.select_one('.PropertyAddress, .address')
                if addr_elem:
                    property_data['address'] = addr_elem.get_text().strip()
                
                # Extract owner information
                owner_elem = row.select_one('.OwnerName, .owner')
                if owner_elem:
                    property_data['owner_name'] = owner_elem.get_text().strip()
                
                # Extract assessed values
                value_elem = row.select_one('.AssessedValue, .market-value')
                if value_elem:
                    property_data['assessed_value'] = self.extract_numeric_value(value_elem.get_text())
                
                # Extract property details
                details_elem = row.select_one('.PropertyDetails, .details')
                if details_elem:
                    details_text = details_elem.get_text()
                    property_data.update(self.parse_property_details(details_text))
                
                if property_data.get('address'):
                    properties.append(property_data)
                    
        except Exception as e:
            logger.debug(f"Error parsing Dallas CAD data: {e}")
        
        return properties
    
    async def extract_tarrant_cad_data(self, soup: BeautifulSoup, url: str) -> List[Dict]:
        """Extract data from Tarrant CAD system"""
        
        properties = []
        
        try:
            # Tarrant CAD specific selectors
            property_rows = soup.select('tr[class*="prop"], .property-item, .search-row')
            
            for row in property_rows:
                property_data = {
                    'source': 'tarrant_cad',
                    'source_url': url,
                    'scraped_at': datetime.now().isoformat(),
                    'county': 'Tarrant'
                }
                
                # Extract account number
                account_elem = row.select_one('.AccountNumber, .account')
                if account_elem:
                    property_data['cad_account_number'] = account_elem.get_text().strip()
                
                # Extract property address
                addr_elem = row.select_one('.SitusAddress, .situs, .address')
                if addr_elem:
                    property_data['address'] = addr_elem.get_text().strip()
                
                # Extract owner
                owner_elem = row.select_one('.OwnerName, .owner-name')
                if owner_elem:
                    property_data['owner_name'] = owner_elem.get_text().strip()
                
                # Extract market value
                market_elem = row.select_one('.MarketValue, .market-value')
                if market_elem:
                    property_data['market_value'] = self.extract_numeric_value(market_elem.get_text())
                
                # Extract land and improvement values
                land_elem = row.select_one('.LandValue, .land-value')
                if land_elem:
                    property_data['land_value'] = self.extract_numeric_value(land_elem.get_text())
                
                improvement_elem = row.select_one('.ImprovementValue, .improvement-value')
                if improvement_elem:
                    property_data['improvement_value'] = self.extract_numeric_value(improvement_elem.get_text())
                
                if property_data.get('address'):
                    properties.append(property_data)
                    
        except Exception as e:
            logger.debug(f"Error parsing Tarrant CAD data: {e}")
        
        return properties
    
    async def extract_collin_cad_data(self, soup: BeautifulSoup, url: str) -> List[Dict]:
        """Extract data from Collin CAD system"""
        
        properties = []
        
        try:
            # Collin CAD specific selectors
            property_cards = soup.select('.property-card, .result-item, .prop-result')
            
            for card in property_cards:
                property_data = {
                    'source': 'collin_cad',
                    'source_url': url,
                    'scraped_at': datetime.now().isoformat(),
                    'county': 'Collin'
                }
                
                # Extract property details from card
                prop_id_elem = card.select_one('.property-id, [data-prop-id]')
                if prop_id_elem:
                    property_data['cad_property_id'] = prop_id_elem.get_text().strip()
                
                # Address extraction
                addr_elem = card.select_one('.property-address, .address')
                if addr_elem:
                    property_data['address'] = addr_elem.get_text().strip()
                
                # Owner information
                owner_elem = card.select_one('.owner-info, .owner')
                if owner_elem:
                    property_data['owner_name'] = owner_elem.get_text().strip()
                
                # Valuation data
                appraised_elem = card.select_one('.appraised-value, .total-value')
                if appraised_elem:
                    property_data['appraised_value'] = self.extract_numeric_value(appraised_elem.get_text())
                
                # Property characteristics
                char_elem = card.select_one('.property-characteristics, .characteristics')
                if char_elem:
                    char_text = char_elem.get_text()
                    property_data.update(self.parse_property_characteristics(char_text))
                
                if property_data.get('address'):
                    properties.append(property_data)
                    
        except Exception as e:
            logger.debug(f"Error parsing Collin CAD data: {e}")
        
        return properties
    
    async def extract_denton_cad_data(self, soup: BeautifulSoup, url: str) -> List[Dict]:
        """Extract data from Denton CAD system"""
        
        properties = []
        
        try:
            # Denton CAD specific selectors
            property_listings = soup.select('.property-listing, .search-result, .prop-record')
            
            for listing in property_listings:
                property_data = {
                    'source': 'denton_cad',
                    'source_url': url,
                    'scraped_at': datetime.now().isoformat(),
                    'county': 'Denton'
                }
                
                # Extract basic property information
                account_elem = listing.select_one('.account-number, .account')
                if account_elem:
                    property_data['cad_account_number'] = account_elem.get_text().strip()
                
                # Property address
                addr_elem = listing.select_one('.property-address, .location')
                if addr_elem:
                    property_data['address'] = addr_elem.get_text().strip()
                
                # Owner details
                owner_elem = listing.select_one('.owner-name, .owner')
                if owner_elem:
                    property_data['owner_name'] = owner_elem.get_text().strip()
                
                # Value information
                total_value_elem = listing.select_one('.total-appraised-value, .total-value')
                if total_value_elem:
                    property_data['total_appraised_value'] = self.extract_numeric_value(total_value_elem.get_text())
                
                # Property details
                details_elem = listing.select_one('.property-details, .details')
                if details_elem:
                    details_text = details_elem.get_text()
                    property_data.update(self.parse_property_details(details_text))
                
                if property_data.get('address'):
                    properties.append(property_data)
                    
        except Exception as e:
            logger.debug(f"Error parsing Denton CAD data: {e}")
        
        return properties
    
    async def extract_generic_cad_data(self, soup: BeautifulSoup, url: str) -> List[Dict]:
        """Generic CAD data extraction for unknown systems"""
        
        properties = []
        
        try:
            # Generic selectors that work across different CAD systems
            selectors = [
                'tr[class*="row"]',
                '.property-record',
                '.search-result',
                '.result-row',
                'tr:has(td)'
            ]
            
            for selector in selectors:
                rows = soup.select(selector)
                if rows:
                    for row in rows:
                        property_data = {
                            'source': 'generic_cad',
                            'source_url': url,
                            'scraped_at': datetime.now().isoformat()
                        }
                        
                        # Extract all text content and try to parse
                        row_text = row.get_text()
                        
                        # Look for address patterns
                        address_match = re.search(r'\d+\s+[A-Za-z\s]+(?:St|Ave|Dr|Rd|Ln|Ct|Pl|Way|Blvd)', row_text)
                        if address_match:
                            property_data['address'] = address_match.group().strip()
                        
                        # Look for monetary values
                        value_matches = re.findall(r'\$[\d,]+', row_text)
                        if value_matches:
                            property_data['estimated_value'] = self.extract_numeric_value(value_matches[0])
                        
                        # Look for property ID/account numbers
                        id_match = re.search(r'(?:ID|Account|Property)[\s#:]*(\d+)', row_text, re.I)
                        if id_match:
                            property_data['property_id'] = id_match.group(1)
                        
                        if property_data.get('address'):
                            properties.append(property_data)
                    break
                    
        except Exception as e:
            logger.debug(f"Error parsing generic CAD data: {e}")
        
        return properties
    
    def parse_property_details(self, details_text: str) -> Dict[str, any]:
        """Parse property details from text"""
        
        details = {}
        
        # Extract year built
        year_match = re.search(r'(?:Built|Year)[\s:]*(\d{4})', details_text, re.I)
        if year_match:
            details['year_built'] = int(year_match.group(1))
        
        # Extract square footage
        sqft_match = re.search(r'(\d{1,3}(?:,\d{3})*)[\s]*(?:sq\.?\s*ft|square feet)', details_text, re.I)
        if sqft_match:
            details['square_feet'] = self.extract_numeric_value(sqft_match.group(1))
        
        # Extract lot size
        lot_match = re.search(r'Lot[\s:]*(\d+(?:\.\d+)?)[\s]*(?:acres?|ac)', details_text, re.I)
        if lot_match:
            details['lot_size_acres'] = float(lot_match.group(1))
        
        # Extract bedrooms/bathrooms
        bed_match = re.search(r'(\d+)[\s]*(?:bed|br)', details_text, re.I)
        if bed_match:
            details['bedrooms'] = int(bed_match.group(1))
        
        bath_match = re.search(r'(\d+(?:\.\d+)?)[\s]*(?:bath|ba)', details_text, re.I)
        if bath_match:
            details['bathrooms'] = float(bath_match.group(1))
        
        return details
    
    def parse_property_characteristics(self, char_text: str) -> Dict[str, any]:
        """Parse property characteristics from CAD text"""
        
        characteristics = {}
        
        # Extract property type
        if re.search(r'single.family|residential|house', char_text, re.I):
            characteristics['property_type'] = 'Single Family'
        elif re.search(r'condo|townhouse', char_text, re.I):
            characteristics['property_type'] = 'Condo/Townhouse'
        elif re.search(r'commercial', char_text, re.I):
            characteristics['property_type'] = 'Commercial'
        
        # Extract construction details
        if re.search(r'brick', char_text, re.I):
            characteristics['exterior_material'] = 'Brick'
        elif re.search(r'wood|frame', char_text, re.I):
            characteristics['exterior_material'] = 'Wood Frame'
        
        # Extract additional features
        if re.search(r'garage', char_text, re.I):
            garage_match = re.search(r'(\d+)[\s]*car garage', char_text, re.I)
            if garage_match:
                characteristics['garage_spaces'] = int(garage_match.group(1))
            else:
                characteristics['has_garage'] = True
        
        return characteristics
    
    async def enhance_cad_property_data(self, property_data: Dict, source_url: str) -> Optional[Dict]:
        """Enhance CAD property data with additional processing"""
        
        try:
            # Skip if no address
            if not property_data.get('address'):
                return None
            
            # Standardize address format
            property_data['address_normalized'] = self.normalize_address(property_data['address'])
            
            # Parse address components if missing
            if not property_data.get('city'):
                address_parts = self.parse_address_components(property_data['address'])
                property_data.update(address_parts)
            
            # Ensure numeric fields are properly typed
            numeric_fields = ['assessed_value', 'market_value', 'appraised_value', 'total_appraised_value', 
                            'land_value', 'improvement_value', 'estimated_value', 'square_feet']
            for field in numeric_fields:
                if field in property_data:
                    property_data[field] = self.extract_numeric_value(property_data[field])
            
            # Calculate lead score based on CAD data
            property_data['lead_score'] = self.calculate_cad_lead_score(property_data)
            
            # Add property age
            if property_data.get('year_built'):
                property_data['property_age'] = datetime.now().year - int(property_data['year_built'])
            
            # Add data source metadata
            property_data['data_source'] = 'county_cad'
            property_data['verification_level'] = 'high'  # CAD data is official
            
            return property_data
            
        except Exception as e:
            logger.debug(f"Error enhancing CAD property data: {e}")
            return property_data
    
    def extract_numeric_value(self, value) -> Optional[float]:
        """Extract numeric value from string or return None"""
        
        if value is None:
            return None
        
        if isinstance(value, (int, float)):
            return float(value)
        
        if isinstance(value, str):
            # Remove currency symbols, commas, and other formatting
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
    
    def calculate_cad_lead_score(self, property_data: Dict) -> int:
        """Calculate lead quality score based on CAD data (1-10)"""
        
        score = 5  # Base score
        
        try:
            # Value-based scoring (CAD values are more reliable)
            value = (property_data.get('assessed_value') or 
                    property_data.get('market_value') or 
                    property_data.get('appraised_value') or 
                    property_data.get('total_appraised_value') or 
                    property_data.get('estimated_value', 0))
            
            if value:
                if value > 600000:
                    score += 4
                elif value > 400000:
                    score += 3
                elif value > 300000:
                    score += 2
                elif value > 200000:
                    score += 1
            
            # Age-based scoring
            year_built = property_data.get('year_built')
            if year_built:
                age = datetime.now().year - int(year_built)
                if age > 20:
                    score += 3
                elif age > 15:
                    score += 2
                elif age > 10:
                    score += 1
            
            # Property type scoring
            prop_type = property_data.get('property_type', '').lower()
            if 'single' in prop_type or 'house' in prop_type:
                score += 1
            
            # Official record bonus (CAD data is verified)
            score += 1
            
            return min(max(score, 1), 10)
            
        except Exception:
            return 5
    
    def get_supabase_table(self) -> str:
        """Get Supabase table name for CAD data"""
        return 'cad_leads'

async def main():
    """Main function to run the async CAD scraper"""
    
    logger.info("🚀 Starting High-Performance Async CAD Scraper")
    
    try:
        # Initialize scraper
        scraper = AsyncCADScraper(
            max_concurrent=75,  # Lower concurrency for CAD sites
            requests_per_hour=3000  # CAD sites have stricter limits
        )
        
        # Run the scraper
        results = await scraper.run_scraper()
        
        # Display results
        logger.info("📊 CAD SCRAPING RESULTS:")
        logger.info(f"   • Total Runtime: {results['total_runtime']:.1f} seconds")
        logger.info(f"   • Requests Made: {results['total_requests']}")
        logger.info(f"   • Success Rate: {results['success_rate_percent']:.1f}%")
        logger.info(f"   • Requests/Second: {results['requests_per_second']:.2f}")
        logger.info(f"   • Requests/Hour: {results['requests_per_hour']:.0f}")
        logger.info(f"   • Leads Extracted: {results['leads_extracted']}")
        logger.info(f"   • Leads Saved: {results['leads_saved']}")
        logger.info(f"   • Cache Hits: {results['cache_hits']}")
        
        logger.info("✅ CAD scraping completed successfully!")
        
        return results
        
    except Exception as e:
        logger.error(f"❌ CAD scraping failed: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())