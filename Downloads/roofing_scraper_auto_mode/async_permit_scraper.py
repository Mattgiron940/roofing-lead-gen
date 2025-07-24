#!/usr/bin/env python3
"""
High-Performance Async Permit Scraper
Supports 5,000+ requests/hour with asyncio, ScraperAPI integration,
intelligent retries, incremental fetching, and direct Supabase integration
"""

import asyncio
import json
import re
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from bs4 import BeautifulSoup
import logging

from async_scraper_framework import BaseAsyncScraper, ScraperConfig
from dfw_geo_filter import get_dfw_zip_codes, get_dfw_counties

logger = logging.getLogger(__name__)

class AsyncPermitScraper(BaseAsyncScraper):
    """High-performance async permit scraper for DFW building permits"""
    
    def __init__(self, max_concurrent: int = 100, requests_per_hour: int = 5000):
        config = ScraperConfig(
            name="async_permit_scraper",
            max_concurrent=max_concurrent,
            requests_per_hour=requests_per_hour,
            retry_attempts=4,  # Permit sites can be unreliable
            retry_backoff=1.5,
            timeout=40.0,  # Permit queries can be slow
            incremental_enabled=True,
            cache_duration_hours=8  # Permit data updates frequently
        )
        super().__init__(config)
        
        # DFW municipality permit systems
        self.dfw_permit_systems = {
            'Dallas': {
                'base_url': 'https://buildinginspection.dallascityhall.com',
                'search_url': 'https://buildinginspection.dallascityhall.com/PermitSearch/search.aspx',
                'counties': ['Dallas'],
                'permit_types': ['residential', 'roofing', 'repair', 'addition']
            },
            'Fort Worth': {
                'base_url': 'https://fortworthtexas.gov',
                'search_url': 'https://fortworthtexas.gov/departments/development-services/permits',
                'counties': ['Tarrant'],
                'permit_types': ['residential', 'roofing', 'structural']
            },
            'Arlington': {
                'base_url': 'https://www.arlingtontx.gov',
                'search_url': 'https://www.arlingtontx.gov/city_hall/departments/planning_development/building_inspection',
                'counties': ['Tarrant'],
                'permit_types': ['building', 'roofing', 'residential']
            },
            'Plano': {
                'base_url': 'https://www.plano.gov',
                'search_url': 'https://www.plano.gov/permits',
                'counties': ['Collin'],
                'permit_types': ['residential', 'roofing', 'repair']
            },
            'Irving': {
                'base_url': 'https://www.cityofirving.org',
                'search_url': 'https://www.cityofirving.org/permits-inspections',
                'counties': ['Dallas'],
                'permit_types': ['residential', 'roofing']
            },
            'Garland': {
                'base_url': 'https://www.garlandtx.gov',
                'search_url': 'https://www.garlandtx.gov/departments/building-inspection/permits',
                'counties': ['Dallas'],
                'permit_types': ['residential', 'roofing']
            }
        }
        
        self.dfw_zip_codes = list(get_dfw_zip_codes())[:75]  # Limit for permit searches
        
        # Roofing-related permit types
        self.roofing_permit_types = [
            'roofing', 'roof', 're-roof', 'reroof', 'roof repair', 
            'roof replacement', 'shingle', 'tile roof', 'metal roof',
            'storm damage', 'hail damage', 'wind damage'
        ]
        
        logger.info(f"🏗️ Async Permit scraper initialized for {len(self.dfw_permit_systems)} municipalities")
    
    async def generate_urls(self) -> List[str]:
        """Generate permit search URLs for DFW area"""
        
        urls = []
        
        # Recent permit URLs by municipality
        for city_name, permit_info in self.dfw_permit_systems.items():
            base_url = permit_info['base_url']
            
            # Recent permits (last 30 days)
            recent_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
            
            # Different URL patterns for different cities
            if 'dallas' in city_name.lower():
                # Dallas permit search patterns
                for permit_type in permit_info['permit_types']:
                    url = f"{base_url}/PermitSearch/Results?type={permit_type}&dateFrom={recent_date}"
                    urls.append(url)
                
                # ZIP code based searches
                for zip_code in self.dfw_zip_codes[:15]:  # Limit per city
                    url = f"{base_url}/PermitSearch/Results?zipcode={zip_code}&dateFrom={recent_date}"
                    urls.append(url)
                    
            elif 'fort-worth' in city_name.lower() or 'fortworth' in city_name.lower():
                # Fort Worth permit patterns
                for permit_type in permit_info['permit_types']:
                    url = f"{base_url}/permits/search?category={permit_type}&issued_after={recent_date}"
                    urls.append(url)
                    
            elif 'arlington' in city_name.lower():
                # Arlington permit patterns
                url = f"{base_url}/building-permits/recent?days=30"
                urls.append(url)
                
                # Specific roofing permits
                url = f"{base_url}/building-permits/search?type=roofing&recent=true"
                urls.append(url)
                
            elif 'plano' in city_name.lower():
                # Plano permit patterns
                for permit_type in permit_info['permit_types']:
                    url = f"{base_url}/permits/search?permit_type={permit_type}&date_range=30"
                    urls.append(url)
                    
            else:
                # Generic patterns for other cities
                url = f"{base_url}/permits/recent"
                urls.append(url)
                
                url = f"{base_url}/building-permits/search?recent=true"
                urls.append(url)
        
        # Add roofing-specific permit searches
        for city_name, permit_info in self.dfw_permit_systems.items():
            base_url = permit_info['base_url']
            
            for roofing_type in self.roofing_permit_types[:5]:  # Top 5 roofing types
                url = f"{base_url}/permits/search?description={roofing_type.replace(' ', '+')}"
                urls.append(url)
        
        # Weather-related permit surge searches (storm damage)
        storm_keywords = ['storm', 'hail', 'wind', 'weather', 'damage']
        for city_name, permit_info in self.dfw_permit_systems.items():
            base_url = permit_info['base_url']
            
            for keyword in storm_keywords:
                url = f"{base_url}/permits/search?keywords={keyword}&type=residential"
                urls.append(url)
        
        logger.info(f"🏗️ Generated {len(urls)} permit URLs")
        return urls
    
    async def extract_data(self, content: str, url: str) -> List[Dict]:
        """Extract permit data from page content"""
        
        try:
            permits = []
            
            # Parse HTML content
            soup = BeautifulSoup(content, 'html.parser')
            
            # Identify permit system type
            system_type = self.identify_permit_system(url)
            
            # Extract data based on system type
            if system_type == 'dallas':
                permits = await self.extract_dallas_permits(soup, url)
            elif system_type == 'fort_worth':  
                permits = await self.extract_fort_worth_permits(soup, url)
            elif system_type == 'arlington':
                permits = await self.extract_arlington_permits(soup, url)
            elif system_type == 'plano':
                permits = await self.extract_plano_permits(soup, url)
            else:
                # Generic permit parsing
                permits = await self.extract_generic_permits(soup, url)
            
            # Filter for roofing-related permits
            roofing_permits = []
            for permit in permits:
                if self.is_roofing_related_permit(permit):
                    enhanced_permit = await self.enhance_permit_data(permit, url)
                    if enhanced_permit:
                        roofing_permits.append(enhanced_permit)
            
            logger.debug(f"✅ Extracted {len(roofing_permits)} roofing permits from {url}")
            return roofing_permits
            
        except Exception as e:
            logger.error(f"❌ Error extracting permit data from {url}: {e}")
            return []
    
    def identify_permit_system(self, url: str) -> str:
        """Identify permit system type from URL"""
        
        url_lower = url.lower()
        
        if 'dallas' in url_lower:
            return 'dallas'
        elif 'fortworth' in url_lower or 'fort-worth' in url_lower:
            return 'fort_worth'
        elif 'arlington' in url_lower:
            return 'arlington'
        elif 'plano' in url_lower:
            return 'plano'
        elif 'irving' in url_lower:
            return 'irving'
        elif 'garland' in url_lower:
            return 'garland'
        else:
            return 'generic'
    
    async def extract_dallas_permits(self, soup: BeautifulSoup, url: str) -> List[Dict]:
        """Extract permits from Dallas permit system"""
        
        permits = []
        
        try:
            # Dallas-specific permit selectors
            permit_rows = soup.select('tr.PermitRow, .permit-record, .search-result-row')
            
            for row in permit_rows:
                permit_data = {
                    'source': 'dallas_permits',
                    'source_url': url,
                    'scraped_at': datetime.now().isoformat(),
                    'city': 'Dallas',
                    'county': 'Dallas'
                }
                
                # Extract permit number
                permit_num_elem = row.select_one('.PermitNumber, .permit-number')
                if permit_num_elem:
                    permit_data['permit_number'] = permit_num_elem.get_text().strip()
                
                # Extract address
                addr_elem = row.select_one('.PermitAddress, .address')
                if addr_elem:
                    permit_data['address'] = addr_elem.get_text().strip()
                
                # Extract permit type/description
                desc_elem = row.select_one('.PermitDescription, .work-description')
                if desc_elem:
                    permit_data['work_description'] = desc_elem.get_text().strip()
                
                # Extract permit value
                value_elem = row.select_one('.PermitValue, .construction-value')
                if value_elem:
                    permit_data['permit_value'] = self.extract_numeric_value(value_elem.get_text())
                
                # Extract permit date
                date_elem = row.select_one('.PermitDate, .issued-date')
                if date_elem:
                    permit_data['permit_date'] = date_elem.get_text().strip()
                
                # Extract contractor info
                contractor_elem = row.select_one('.Contractor, .contractor-name')
                if contractor_elem:
                    permit_data['contractor_name'] = contractor_elem.get_text().strip()
                
                if permit_data.get('address'):
                    permits.append(permit_data)
                    
        except Exception as e:
            logger.debug(f"Error parsing Dallas permits: {e}")
        
        return permits
    
    async def extract_fort_worth_permits(self, soup: BeautifulSoup, url: str) -> List[Dict]:
        """Extract permits from Fort Worth permit system"""
        
        permits = []
        
        try:
            # Fort Worth permit selectors
            permit_cards = soup.select('.permit-card, .permit-item, .application-record')
            
            for card in permit_cards:
                permit_data = {
                    'source': 'fort_worth_permits',
                    'source_url': url,
                    'scraped_at': datetime.now().isoformat(),
                    'city': 'Fort Worth',
                    'county': 'Tarrant'
                }
                
                # Extract application number
                app_num_elem = card.select_one('.application-number, .permit-id')
                if app_num_elem:
                    permit_data['permit_number'] = app_num_elem.get_text().strip()
                
                # Extract property address
                addr_elem = card.select_one('.property-address, .permit-address')
                if addr_elem:
                    permit_data['address'] = addr_elem.get_text().strip()
                
                # Extract work type
                work_elem = card.select_one('.work-type, .permit-type')
                if work_elem:
                    permit_data['work_description'] = work_elem.get_text().strip()
                
                # Extract valuation
                val_elem = card.select_one('.valuation, .estimated-cost')
                if val_elem:
                    permit_data['permit_value'] = self.extract_numeric_value(val_elem.get_text())
                
                # Extract status and dates
                status_elem = card.select_one('.permit-status, .status')
                if status_elem:
                    permit_data['permit_status'] = status_elem.get_text().strip()
                
                issue_date_elem = card.select_one('.issue-date, .issued')
                if issue_date_elem:
                    permit_data['permit_date'] = issue_date_elem.get_text().strip()
                
                if permit_data.get('address'):
                    permits.append(permit_data)
                    
        except Exception as e:
            logger.debug(f"Error parsing Fort Worth permits: {e}")
        
        return permits
    
    async def extract_arlington_permits(self, soup: BeautifulSoup, url: str) -> List[Dict]:
        """Extract permits from Arlington permit system"""
        
        permits = []
        
        try:
            # Arlington permit selectors
            permit_listings = soup.select('.permit-listing, .permit-record, .building-permit')
            
            for listing in permit_listings:
                permit_data = {
                    'source': 'arlington_permits',
                    'source_url': url,
                    'scraped_at': datetime.now().isoformat(),
                    'city': 'Arlington',
                    'county': 'Tarrant'
                }
                
                # Extract permit details
                permit_id_elem = listing.select_one('.permit-id, .case-number')
                if permit_id_elem:
                    permit_data['permit_number'] = permit_id_elem.get_text().strip()
                
                # Address extraction
                addr_elem = listing.select_one('.site-address, .permit-address')
                if addr_elem:
                    permit_data['address'] = addr_elem.get_text().strip()
                
                # Work description
                desc_elem = listing.select_one('.work-description, .permit-description')
                if desc_elem:
                    permit_data['work_description'] = desc_elem.get_text().strip()
                
                # Construction value
                value_elem = listing.select_one('.construction-cost, .project-value')
                if value_elem:
                    permit_data['permit_value'] = self.extract_numeric_value(value_elem.get_text())
                
                # Permit dates
                issued_elem = listing.select_one('.date-issued, .issue-date')
                if issued_elem:
                    permit_data['permit_date'] = issued_elem.get_text().strip()
                
                if permit_data.get('address'):
                    permits.append(permit_data)
                    
        except Exception as e:
            logger.debug(f"Error parsing Arlington permits: {e}")
        
        return permits
    
    async def extract_plano_permits(self, soup: BeautifulSoup, url: str) -> List[Dict]:
        """Extract permits from Plano permit system"""
        
        permits = []
        
        try:
            # Plano permit selectors
            permit_entries = soup.select('.permit-entry, .permit-data, .application-info')
            
            for entry in permit_entries:
                permit_data = {
                    'source': 'plano_permits',
                    'source_url': url,
                    'scraped_at': datetime.now().isoformat(),
                    'city': 'Plano',
                    'county': 'Collin'
                }
                
                # Permit number extraction
                num_elem = entry.select_one('.permit-number, .application-id')
                if num_elem:
                    permit_data['permit_number'] = num_elem.get_text().strip()
                
                # Property address
                prop_addr_elem = entry.select_one('.property-address, .site-location')
                if prop_addr_elem:
                    permit_data['address'] = prop_addr_elem.get_text().strip()
                
                # Work type and description
                work_elem = entry.select_one('.work-type, .project-description')
                if work_elem:
                    permit_data['work_description'] = work_elem.get_text().strip()
                
                # Project valuation
                val_elem = entry.select_one('.project-valuation, .construction-value')
                if val_elem:
                    permit_data['permit_value'] = self.extract_numeric_value(val_elem.get_text())
                
                # Permit status and timing
                status_elem = entry.select_one('.application-status, .permit-status')
                if status_elem:
                    permit_data['permit_status'] = status_elem.get_text().strip()
                
                if permit_data.get('address'):
                    permits.append(permit_data)
                    
        except Exception as e:
            logger.debug(f"Error parsing Plano permits: {e}")
        
        return permits
    
    async def extract_generic_permits(self, soup: BeautifulSoup, url: str) -> List[Dict]:
        """Generic permit extraction for unknown systems"""
        
        permits = []
        
        try:
            # Generic selectors that work across permit systems
            selectors = [
                'tr:has(td)',
                '.permit-row',
                '.application-row',
                '.record-row',
                '.search-result'
            ]
            
            for selector in selectors:
                rows = soup.select(selector)
                if rows:
                    for row in rows:
                        permit_data = {
                            'source': 'generic_permits',
                            'source_url': url,
                            'scraped_at': datetime.now().isoformat()
                        }
                        
                        # Extract text and parse for permit information
                        row_text = row.get_text()
                        
                        # Look for permit numbers
                        permit_match = re.search(r'(?:permit|application)[\s#:]*([A-Z0-9-]+)', row_text, re.I)
                        if permit_match:
                            permit_data['permit_number'] = permit_match.group(1)
                        
                        # Look for addresses
                        addr_match = re.search(r'\d+\s+[A-Za-z\s]+(?:St|Ave|Dr|Rd|Ln|Ct|Pl|Way|Blvd)', row_text)
                        if addr_match:
                            permit_data['address'] = addr_match.group().strip()
                        
                        # Look for monetary values
                        value_matches = re.findall(r'\$[\d,]+', row_text)
                        if value_matches:
                            permit_data['permit_value'] = self.extract_numeric_value(value_matches[0])
                        
                        # Look for dates
                        date_match = re.search(r'\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}', row_text)
                        if date_match:
                            permit_data['permit_date'] = date_match.group()
                        
                        if permit_data.get('address') and permit_data.get('permit_number'):
                            permits.append(permit_data)
                    break
                    
        except Exception as e:
            logger.debug(f"Error parsing generic permits: {e}")
        
        return permits
    
    def is_roofing_related_permit(self, permit_data: Dict) -> bool:
        """Check if permit is related to roofing work"""
        
        # Check work description
        description = permit_data.get('work_description', '').lower()
        
        # Check for roofing keywords
        roofing_keywords = [
            'roof', 'roofing', 're-roof', 'reroof', 'shingle', 'tile',
            'metal roof', 'roof repair', 'roof replacement', 'roofcover',
            'storm damage', 'hail damage', 'wind damage', 'weather damage',
            'roof covering', 'roof membrane', 'roof system'
        ]
        
        for keyword in roofing_keywords:
            if keyword in description:
                return True
        
        # Check permit type or category
        permit_type = permit_data.get('permit_type', '').lower()
        if 'roof' in permit_type:
            return True
        
        # Check for high-value permits that might include roofing
        permit_value = permit_data.get('permit_value', 0)
        if permit_value and permit_value > 10000:  # High-value permits often include roofing
            return True
        
        return False
    
    async def enhance_permit_data(self, permit_data: Dict, source_url: str) -> Optional[Dict]:
        """Enhance permit data with additional processing"""
        
        try:
            # Skip if no address
            if not permit_data.get('address'):
                return None
            
            # Standardize address format
            permit_data['address_normalized'] = self.normalize_address(permit_data['address'])
            
            # Parse address components
            if not permit_data.get('zip_code'):
                address_parts = self.parse_address_components(permit_data['address'])
                permit_data.update(address_parts)
            
            # Ensure numeric fields are properly typed
            numeric_fields = ['permit_value', 'construction_value', 'estimated_cost']
            for field in numeric_fields:
                if field in permit_data:
                    permit_data[field] = self.extract_numeric_value(permit_data[field])
            
            # Parse and standardize permit date
            if permit_data.get('permit_date'):
                permit_data['permit_date_parsed'] = self.parse_permit_date(permit_data['permit_date'])
            
            # Calculate lead score for permit
            permit_data['lead_score'] = self.calculate_permit_lead_score(permit_data)
            
            # Add urgency indicators
            permit_data['urgency_level'] = self.calculate_permit_urgency(permit_data)
            
            # Identify storm-related permits
            permit_data['storm_related'] = self.is_storm_related_permit(permit_data)
            
            # Add data verification level
            permit_data['verification_level'] = 'high'  # Permit data is official
            permit_data['data_source'] = 'municipal_permits'
            
            return permit_data
            
        except Exception as e:
            logger.debug(f"Error enhancing permit data: {e}")
            return permit_data
    
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
    
    def parse_permit_date(self, date_str: str) -> Optional[str]:
        """Parse permit date string to ISO format"""
        
        try:
            # Common date formats in permit systems
            date_patterns = [
                r'(\d{1,2})/(\d{1,2})/(\d{4})',  # MM/DD/YYYY
                r'(\d{1,2})-(\d{1,2})-(\d{4})',  # MM-DD-YYYY
                r'(\d{4})-(\d{1,2})-(\d{1,2})',  # YYYY-MM-DD
            ]
            
            for pattern in date_patterns:
                match = re.search(pattern, date_str.strip())
                if match:
                    if len(match.group(1)) == 4:  # YYYY-MM-DD format
                        year, month, day = match.groups()
                    else:  # MM/DD/YYYY or MM-DD-YYYY format
                        month, day, year = match.groups()
                    
                    # Create ISO date string
                    return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                    
        except Exception:
            pass
        
        return None
    
    def calculate_permit_lead_score(self, permit_data: Dict) -> int:
        """Calculate lead quality score based on permit data (1-10)"""
        
        score = 6  # Base score (permits are high-quality leads)
        
        try:
            # Value-based scoring
            permit_value = permit_data.get('permit_value', 0)
            if permit_value:
                if permit_value > 50000:
                    score += 4
                elif permit_value > 25000:
                    score += 3
                elif permit_value > 15000:
                    score += 2
                elif permit_value > 5000:
                    score += 1
            
            # Roofing-specific scoring
            description = permit_data.get('work_description', '').lower()
            if any(keyword in description for keyword in ['re-roof', 'reroof', 'roof replacement']):
                score += 2
            elif 'roof repair' in description:
                score += 1
            
            # Storm-related bonus
            if self.is_storm_related_permit(permit_data):
                score += 2
            
            # Recent permit bonus
            if permit_data.get('permit_date_parsed'):
                permit_date = datetime.fromisoformat(permit_data['permit_date_parsed'])
                days_old = (datetime.now() - permit_date).days
                if days_old <= 30:
                    score += 2
                elif days_old <= 90:
                    score += 1
            
            return min(max(score, 1), 10)
            
        except Exception:
            return 6
    
    def calculate_permit_urgency(self, permit_data: Dict) -> str:
        """Calculate urgency level for permit lead"""
        
        try:
            description = permit_data.get('work_description', '').lower()
            
            # High urgency indicators
            if any(keyword in description for keyword in ['emergency', 'storm', 'damage', 'leak', 'urgent']):
                return 'high'
            
            # Medium urgency indicators
            if any(keyword in description for keyword in ['repair', 'replace', 'fix']):
                return 'medium'
            
            # Check permit value
            permit_value = permit_data.get('permit_value', 0)
            if permit_value and permit_value > 30000:
                return 'high'
            elif permit_value and permit_value > 15000:
                return 'medium'
            
            return 'low'
            
        except Exception:
            return 'medium'
    
    def is_storm_related_permit(self, permit_data: Dict) -> bool:
        """Check if permit is related to storm damage"""
        
        description = permit_data.get('work_description', '').lower()
        storm_keywords = ['storm', 'hail', 'wind', 'weather', 'damage', 'emergency', 'insurance']
        
        return any(keyword in description for keyword in storm_keywords)
    
    def get_supabase_table(self) -> str:
        """Get Supabase table name for permit data"""
        return 'permit_leads'

async def main():
    """Main function to run the async permit scraper"""
    
    logger.info("🚀 Starting High-Performance Async Permit Scraper")
    
    try:
        # Initialize scraper
        scraper = AsyncPermitScraper(
            max_concurrent=80,  # Moderate concurrency for permit sites
            requests_per_hour=4000  # Permit sites have moderate limits
        )
        
        # Run the scraper
        results = await scraper.run_scraper()
        
        # Display results
        logger.info("📊 PERMIT SCRAPING RESULTS:")
        logger.info(f"   • Total Runtime: {results['total_runtime']:.1f} seconds")
        logger.info(f"   • Requests Made: {results['total_requests']}")
        logger.info(f"   • Success Rate: {results['success_rate_percent']:.1f}%")
        logger.info(f"   • Requests/Second: {results['requests_per_second']:.2f}")
        logger.info(f"   • Requests/Hour: {results['requests_per_hour']:.0f}")
        logger.info(f"   • Leads Extracted: {results['leads_extracted']}")
        logger.info(f"   • Leads Saved: {results['leads_saved']}")
        logger.info(f"   • Cache Hits: {results['cache_hits']}")
        
        logger.info("✅ Permit scraping completed successfully!")
        
        return results
        
    except Exception as e:
        logger.error(f"❌ Permit scraping failed: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())