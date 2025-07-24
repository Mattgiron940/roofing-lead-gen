#!/usr/bin/env python3
"""
Storm Integration Module using ScraperAPI
High-performance storm data collection with Supabase integration and real-time lead scoring
"""

import sys
import os
sys.path.append('..')

import re
import json
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import time
import logging
from typing import List, Dict, Any, Optional

# Import unified Supabase client
from supabase_client import supabase

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class StormIntegration:
    """High-performance storm data scraper with multi-threading and real-time insertion"""
    
    def __init__(self):
        self.scraper_api_key = self.get_scraper_api_key()
        self.session = requests.Session()
        self.processed_urls = set()
        
        # Storm data URLs for Texas weather services
        self.target_urls = self.generate_storm_urls()
    
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
    
    def generate_storm_urls(self) -> List[str]:
        """Generate storm data URLs for Texas weather services"""
        # Current date for recent storm data
        current_date = datetime.now()
        thirty_days_ago = current_date - timedelta(days=30)
        
        base_urls = [
            # National Weather Service Storm Reports - Texas
            f"https://www.spc.noaa.gov/climo/reports/{thirty_days_ago.strftime('%y%m%d')}_rpts_filtered.html",
            f"https://www.spc.noaa.gov/climo/reports/{current_date.strftime('%y%m%d')}_rpts_filtered.html",
            
            # Weather.gov Storm Events Database
            "https://www.ncdc.noaa.gov/stormevents/listevents.jsp?eventType=ALL&beginDate_mm=01&beginDate_dd=01&beginDate_yyyy=2024&endDate_mm=12&endDate_dd=31&endDate_yyyy=2024&county=DALLAS%3A57&hailfilter=0.00&tornfilter=0&windfilter=000&sort=DT&submitbutton=Search&statefips=48%2CTEXAS",
            
            # Texas Weather Service - Hail Reports
            "https://www.weather.gov/fwd/hailreports",
            "https://www.weather.gov/ewx/hailreports",
            
            # Storm Prediction Center - Texas Hail Reports
            "https://www.spc.noaa.gov/climo/reports/yesterday_filtered.html",
            "https://www.spc.noaa.gov/climo/reports/today_filtered.html",
            
            # AccuWeather Storm Center - Dallas/Fort Worth
            "https://www.accuweather.com/en/us/dallas-tx/75201/severe-weather/351194",
            "https://www.accuweather.com/en/us/fort-worth-tx/76102/severe-weather/351200",
            
            # Weather Underground Storm Reports - Texas
            "https://www.wunderground.com/severe/us/tx/dallas-county",
            "https://www.wunderground.com/severe/us/tx/tarrant-county",
            "https://www.wunderground.com/severe/us/tx/collin-county",
            
            # Local TV Weather Storm Reports
            "https://www.nbcdfw.com/weather/severe-weather-alerts/",
            "https://www.fox4news.com/weather/severe-weather",
            
            # Insurance Industry Storm Reports
            "https://www.iii.org/fact-statistic/facts-statistics-hail",
            
            # Add more storm tracking URLs here
        ]
        
        return base_urls
    
    def get_scraperapi_url(self, target_url: str) -> str:
        """Generate ScraperAPI URL with proper parameters"""
        return f"http://api.scraperapi.com?api_key={self.scraper_api_key}&url={target_url}&render=true"

    def parse_storm_listing(self, html: str, source_url: str) -> List[Dict[str, Any]]:
        """Parse storm reports page and extract individual storm events"""
        storm_events = []
        
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # Look for JSON data first (many weather sites use JavaScript)
            script_tags = soup.find_all('script')
            
            for script in script_tags:
                if script.string and ('storm' in script.string.lower() or 'hail' in script.string.lower() or 'tornado' in script.string.lower()):
                    try:
                        # Try to extract JSON storm data
                        json_match = re.search(r'(?:storms|events|reports)\s*[:=]\s*(\[.+?\])', script.string)
                        if json_match:
                            data = json.loads(json_match.group(1))
                            storm_events.extend(self.extract_storms_from_json(data, source_url))
                            break
                    except (json.JSONDecodeError, Exception) as e:
                        logger.debug(f"Error parsing JSON from script: {e}")
                        continue
            
            # Fallback to HTML parsing if JSON not found
            if not storm_events:
                storm_events = self.parse_storm_html_listings(soup, source_url)
            
            logger.info(f"Found {len(storm_events)} storm events from {source_url}")
            
        except Exception as e:
            logger.error(f"❌ Error parsing storm listings from {source_url}: {e}")
        
        return storm_events
    
    def extract_storms_from_json(self, data: list, source_url: str) -> List[Dict[str, Any]]:
        """Extract storm events from JSON data structure"""
        storm_events = []
        
        try:
            for storm_obj in data[:20]:  # Limit to first 20 per page
                storm_data = self.extract_storm_from_json_obj(storm_obj, source_url)
                if storm_data:
                    storm_events.append(storm_data)
                    
        except Exception as e:
            logger.debug(f"Error extracting from JSON: {e}")
        
        return storm_events
    
    def extract_storm_from_json_obj(self, storm_obj: dict, source_url: str) -> Optional[Dict[str, Any]]:
        """Extract storm data from a single JSON storm object"""
        try:
            # Extract event ID
            event_id = storm_obj.get('id', '') or storm_obj.get('event_id', '') or storm_obj.get('report_id', '')
            
            # Extract event type
            event_type = storm_obj.get('type', '') or storm_obj.get('event_type', '') or storm_obj.get('phenomenon', '')
            
            # Extract location information
            city = storm_obj.get('city', '') or storm_obj.get('location', '') or storm_obj.get('place', '')
            county = storm_obj.get('county', '') or storm_obj.get('county_name', '')
            state = storm_obj.get('state', '') or storm_obj.get('st', '') or 'TX'
            
            # Extract coordinates
            latitude = storm_obj.get('lat', None) or storm_obj.get('latitude', None)
            longitude = storm_obj.get('lon', None) or storm_obj.get('lng', None) or storm_obj.get('longitude', None)
            
            # Extract event details
            event_date = storm_obj.get('date', '') or storm_obj.get('event_date', '') or storm_obj.get('begin_date', '')
            event_time = storm_obj.get('time', '') or storm_obj.get('event_time', '') or storm_obj.get('begin_time', '')
            
            # Extract magnitude/severity
            magnitude = storm_obj.get('magnitude', '') or storm_obj.get('size', '') or storm_obj.get('max_hail_size', '')
            wind_speed = storm_obj.get('wind_speed', None) or storm_obj.get('max_wind', None)
            
            # Extract damage information
            damage_estimate = storm_obj.get('damage', 0) or storm_obj.get('damage_property', 0) or storm_obj.get('damage_crops', 0)
            
            # Extract description
            description = storm_obj.get('description', '') or storm_obj.get('narrative', '') or storm_obj.get('comments', '')
            
            # Only create storm event if we have essential data
            if event_id and event_type and city:
                return {
                    "event_id": event_id,
                    "event_type": self.categorize_event_type(event_type),
                    "event_date": self.parse_date(event_date),
                    "event_time": event_time,
                    "city": city,
                    "county": county or self.get_county_from_city(city),
                    "state": state,
                    "latitude": float(latitude) if latitude else None,
                    "longitude": float(longitude) if longitude else None,
                    "magnitude": magnitude,
                    "wind_speed_mph": int(wind_speed) if wind_speed else None,
                    "hail_size_inches": self.parse_hail_size(magnitude),
                    "damage_estimate": int(damage_estimate) if damage_estimate else None,
                    "affected_areas": city,
                    "weather_service_office": self.extract_weather_office(source_url),
                    "description": description[:500] if description else None,  # Limit length
                    "data_source": self.extract_source_name(source_url),
                    "source_url": source_url,
                    "severity_level": self.calculate_severity(event_type, magnitude, wind_speed, damage_estimate),
                    "impact_radius_miles": self.estimate_impact_radius(event_type, magnitude),
                    "roofing_lead_potential": self.calculate_roofing_potential(event_type, magnitude, damage_estimate),
                    "notes": f"Scraped from weather service on {datetime.now().strftime('%Y-%m-%d')}"
                }
            
            return None
            
        except Exception as e:
            logger.debug(f"Error extracting storm from JSON object: {e}")
            return None
    
    def parse_storm_html_listings(self, soup, source_url: str) -> List[Dict[str, Any]]:
        """Fallback HTML parsing if JSON data not available"""
        storm_events = []
        
        try:
            # Look for storm report tables or cards in HTML
            storm_selectors = [
                'table.storm-reports tbody tr',
                '.storm-event',
                '.weather-report',
                '.severe-weather-row',
                '[data-storm-id]',
                'tr[class*="storm"]',
                '.report-row'
            ]
            
            storm_rows = []
            for selector in storm_selectors:
                rows = soup.select(selector)
                if rows:
                    storm_rows = rows
                    break
            
            for row in storm_rows[:15]:  # Limit to first 15
                storm_data = self.extract_storm_from_html_row(row, source_url)
                if storm_data:
                    storm_events.append(storm_data)
                    
        except Exception as e:
            logger.debug(f"Error in HTML fallback parsing: {e}")
        
        return storm_events
    
    def extract_storm_from_html_row(self, row, source_url: str) -> Optional[Dict[str, Any]]:
        """Extract storm data from HTML table row or card"""
        try:
            # Extract text from all cells/elements
            row_text = row.get_text()
            cells = row.find_all(['td', 'div', 'span'])
            
            # Extract event type
            event_type = ""
            event_keywords = ['hail', 'tornado', 'wind', 'thunderstorm', 'severe', 'storm']
            for keyword in event_keywords:
                if keyword in row_text.lower():
                    event_type = keyword.title()
                    break
            
            # Extract location
            city = ""
            county = ""
            for cell in cells:
                cell_text = cell.get_text(strip=True)
                # Look for city names or county patterns
                if re.search(r'\\b[A-Z][a-z]+(?:\\s+[A-Z][a-z]+)*\\b', cell_text) and len(cell_text) < 30:
                    if 'county' in cell_text.lower():
                        county = cell_text
                    else:
                        city = cell_text
            
            # Extract date
            event_date = ""
            for cell in cells:
                cell_text = cell.get_text(strip=True)
                # Look for date patterns
                date_match = re.search(r'\\d{1,2}[/-]\\d{1,2}[/-]\\d{2,4}', cell_text)
                if date_match:
                    event_date = date_match.group()
                    break
            
            # Extract magnitude/size
            magnitude = ""
            for cell in cells:
                cell_text = cell.get_text(strip=True)
                # Look for hail size or wind speed
                size_match = re.search(r'(\\d+(?:\\.\\d+)?)\\s*(?:inch|in|mph)', cell_text, re.I)
                if size_match:
                    magnitude = size_match.group()
                    break
            
            # Generate a basic event ID from available data
            event_id = f"{event_type}_{city}_{event_date}".replace(' ', '_').replace('/', '_')
            
            if event_type and city:
                return {
                    "event_id": event_id,
                    "event_type": self.categorize_event_type(event_type),
                    "event_date": self.parse_date(event_date),
                    "event_time": None,
                    "city": city,
                    "county": county or self.get_county_from_city(city),
                    "state": "TX",
                    "latitude": None,
                    "longitude": None,
                    "magnitude": magnitude,
                    "wind_speed_mph": None,
                    "hail_size_inches": self.parse_hail_size(magnitude),
                    "damage_estimate": None,
                    "affected_areas": city,
                    "weather_service_office": self.extract_weather_office(source_url),
                    "description": None,
                    "data_source": self.extract_source_name(source_url),
                    "source_url": source_url,
                    "severity_level": self.calculate_severity(event_type, magnitude, None, None),
                    "impact_radius_miles": self.estimate_impact_radius(event_type, magnitude),
                    "roofing_lead_potential": self.calculate_roofing_potential(event_type, magnitude, None),
                    "notes": f"Scraped from weather HTML on {datetime.now().strftime('%Y-%m-%d')}"
                }
            
            return None
            
        except Exception as e:
            logger.debug(f"Error extracting from HTML row: {e}")
            return None
    
    def categorize_event_type(self, event_type: str) -> str:
        """Categorize storm event type"""
        event_lower = event_type.lower()
        
        if any(term in event_lower for term in ['hail', 'hailstorm']):
            return "Hail"
        elif any(term in event_lower for term in ['tornado', 'funnel']):
            return "Tornado"
        elif any(term in event_lower for term in ['wind', 'thunderstorm wind', 'straight-line']):
            return "High Wind"
        elif any(term in event_lower for term in ['thunderstorm', 'severe thunderstorm']):
            return "Severe Thunderstorm"
        elif any(term in event_lower for term in ['flood', 'flash flood']):
            return "Flood"
        else:
            return "Severe Weather"
    
    def get_county_from_city(self, city: str) -> str:
        """Map city names to counties for Texas"""
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
            'houston': 'Harris County',
            'austin': 'Travis County',
            'san antonio': 'Bexar County'
        }
        
        return county_map.get(city_lower, 'Unknown County')
    
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
                '%d/%m/%Y',
                '%Y%m%d'
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
    
    def parse_hail_size(self, magnitude: str) -> Optional[float]:
        """Extract hail size in inches from magnitude string"""
        if not magnitude:
            return None
        
        try:
            # Look for inch measurements
            inch_match = re.search(r'(\\d+(?:\\.\\d+)?)\\s*(?:inch|in)', magnitude, re.I)
            if inch_match:
                return float(inch_match.group(1))
            
            # Look for common hail size descriptions
            size_map = {
                'pea': 0.25,
                'marble': 0.5,
                'penny': 0.75,
                'nickel': 0.88,
                'quarter': 1.0,
                'ping pong': 1.5,
                'golf ball': 1.75,
                'tennis ball': 2.5,
                'baseball': 2.75,
                'softball': 4.0
            }
            
            magnitude_lower = magnitude.lower()
            for size_name, inches in size_map.items():
                if size_name in magnitude_lower:
                    return inches
            
            return None
        except:
            return None
    
    def extract_weather_office(self, url: str) -> str:
        """Extract weather service office from URL"""
        office_map = {
            'fwd': 'Fort Worth',
            'ewx': 'Austin/San Antonio',
            'hgx': 'Houston/Galveston',
            'lub': 'Lubbock',
            'maf': 'Midland',
            'sjt': 'San Angelo'
        }
        
        for code, office in office_map.items():
            if code in url:
                return office
        
        return 'National Weather Service'
    
    def extract_source_name(self, url: str) -> str:
        """Extract readable source name from URL"""
        if 'noaa.gov' in url or 'weather.gov' in url:
            return 'National Weather Service'
        elif 'spc.noaa.gov' in url:
            return 'Storm Prediction Center'
        elif 'accuweather.com' in url:
            return 'AccuWeather'
        elif 'wunderground.com' in url:
            return 'Weather Underground'
        elif 'nbcdfw.com' in url:
            return 'NBC 5 Dallas-Fort Worth'
        elif 'fox4news.com' in url:
            return 'FOX 4 Dallas-Fort Worth'
        else:
            return 'Weather Service'
    
    def calculate_severity(self, event_type: str, magnitude: str, wind_speed: Optional[int], damage: Optional[int]) -> str:
        """Calculate severity level based on storm characteristics"""
        event_lower = event_type.lower() if event_type else ""
        
        # High severity conditions
        if 'tornado' in event_lower:
            return 'Severe'
        
        if magnitude:
            hail_size = self.parse_hail_size(magnitude)
            if hail_size and hail_size >= 1.0:  # Quarter size or larger
                return 'Severe'
        
        if wind_speed and wind_speed >= 58:  # Severe thunderstorm criteria
            return 'Severe'
        
        if damage and damage > 10000:
            return 'Severe'
        
        # Moderate severity
        if any(term in event_lower for term in ['hail', 'thunderstorm', 'wind']):
            return 'Moderate'
        
        return 'Minor'
    
    def estimate_impact_radius(self, event_type: str, magnitude: str) -> Optional[int]:
        """Estimate impact radius in miles"""
        event_lower = event_type.lower() if event_type else ""
        
        if 'tornado' in event_lower:
            return 10  # Tornado damage path plus surrounding area
        
        if 'hail' in event_lower:
            hail_size = self.parse_hail_size(magnitude) if magnitude else 0
            if hail_size and hail_size >= 1.0:
                return 5  # Large hail storms affect wider areas
            else:
                return 2  # Small hail more localized
        
        if any(term in event_lower for term in ['wind', 'thunderstorm']):
            return 8  # Wind damage can be widespread
        
        return 3  # Default radius
    
    def calculate_roofing_potential(self, event_type: str, magnitude: str, damage: Optional[int]) -> str:
        """Calculate potential for roofing leads"""
        event_lower = event_type.lower() if event_type else ""
        
        # High potential for roofing damage
        if 'hail' in event_lower:
            hail_size = self.parse_hail_size(magnitude) if magnitude else 0
            if hail_size and hail_size >= 1.0:
                return 'High'
            else:
                return 'Medium'
        
        if 'tornado' in event_lower:
            return 'High'
        
        if any(term in event_lower for term in ['wind', 'thunderstorm']):
            return 'Medium'
        
        if damage and damage > 5000:
            return 'High'
        
        return 'Low'
    
    def scrape_single_url(self, url: str) -> List[Dict[str, Any]]:
        """Scrape a single URL and return extracted storm events"""
        if url in self.processed_urls:
            return []
        
        try:
            logger.info(f"🔍 Scraping {url}")
            
            # Get page with ScraperAPI
            scraper_url = self.get_scraperapi_url(url)
            response = self.session.get(scraper_url, timeout=60)
            response.raise_for_status()
            
            # Parse storm events from page
            storm_events = self.parse_storm_listing(response.text, url)
            
            # Insert each storm event into Supabase
            successful_inserts = 0
            for storm in storm_events:
                if supabase.safe_insert("storm_events", storm):
                    successful_inserts += 1
            
            self.processed_urls.add(url)
            logger.info(f"✅ {url}: {successful_inserts}/{len(storm_events)} storm events inserted")
            
            return storm_events
            
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
        logger.info(f"🚀 Starting threaded storm data collection with {max_workers} workers")
        logger.info(f"📊 Targeting {len(self.target_urls)} URLs")
        
        start_time = time.time()
        all_storm_events = []
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
                    storm_events = future.result()
                    if storm_events:
                        all_storm_events.extend(storm_events)
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
            'total_storm_events': len(all_storm_events),
            'runtime_seconds': runtime,
            'average_per_url': runtime / len(self.target_urls) if self.target_urls else 0,
            'events_per_minute': (len(all_storm_events) / runtime) * 60 if runtime > 0 else 0
        }
        
        logger.info(f"📊 Scraping completed: {results['total_storm_events']} storm events in {runtime:.2f}s")
        return results



def main():
    """Main execution function"""
    print("⛈️ STORM INTEGRATION STARTING")
    print("=" * 50)
    
    # Check if storm_events table exists
    if not supabase.check_table_exists("storm_events"):
        print("❌ storm_events table does not exist!")
        print("💡 Run the Supabase schema deployment first")
        return 1
    
    # Initialize storm integration
    storm_scraper = StormIntegration()
    
    # Run scraping
    results = storm_scraper.run_threaded_scraping(max_workers=3)
    
    # Print final report
    print("\n" + "=" * 50)
    print("🎯 STORM INTEGRATION FINAL REPORT")
    print("=" * 50)
    print(f"📊 URLs Processed: {results['successful_urls']}/{results['total_urls']}")
    print(f"✅ Total Storm Events Found: {results['total_storm_events']}")
    print(f"⏱️ Runtime: {results['runtime_seconds']:.2f} seconds")
    print(f"🚀 Speed: {results['events_per_minute']:.1f} events/minute")
    print(f"📈 Success Rate: {(results['successful_urls']/results['total_urls']*100):.1f}%")
    
    # Check final database count
    final_count = supabase.get_table_count("storm_events")
    print(f"🗄️ Total Storm Events in Database: {final_count}")
    
    print("✅ STORM INTEGRATION COMPLETED!")
    return 0


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)