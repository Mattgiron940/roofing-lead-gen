#!/usr/bin/env python3
"""
Playwright-based DFW Zillow Scraper for Roofing Leads
Uses browser automation to scrape recently sold properties
"""

import asyncio
import csv
import json
import logging
import random
import time
from datetime import datetime
from typing import List, Dict, Any
from playwright.async_api import async_playwright, Browser, Page

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PlaywrightZillowScraper:
    def __init__(self):
        self.browser: Browser = None
        self.page: Page = None
        self.all_properties = []
        
        # DFW Counties and ZIP codes for better targeting
        self.dfw_areas = {
            'Dallas County': ['75201', '75202', '75203', '75204', '75205', '75206', '75207', '75208', '75209', '75210', '75211', '75212', '75214', '75215', '75216', '75217', '75218', '75219', '75220', '75221', '75222', '75223', '75224', '75225', '75226', '75227', '75228', '75229', '75230', '75231', '75232', '75233', '75234', '75235', '75236', '75237', '75238', '75240', '75241', '75243', '75244', '75246', '75247', '75248', '75249', '75250', '75251', '75252', '75253', '75254', '75260', '75261', '75262', '75263', '75264', '75265', '75266', '75267', '75270', '75275', '75277', '75283', '75284', '75285', '75286', '75287', '75295', '75301', '75303', '75310', '75312', '75313', '75315', '75320', '75323', '75326', '75336', '75339', '75342', '75354', '75355', '75356', '75357', '75359', '75360', '75363', '75364', '75367', '75368', '75370', '75371', '75372', '75373', '75374', '75376', '75378', '75379', '75380', '75381', '75382', '75390', '75391', '75392', '75393', '75394', '75395', '75396', '75397', '75398'],
            'Tarrant County': ['76001', '76002', '76003', '76004', '76005', '76006', '76007', '76008', '76009', '76010', '76011', '76012', '76013', '76014', '76015', '76016', '76017', '76018', '76019', '76020', '76021', '76022', '76023', '76028', '76030', '76031', '76032', '76033', '76034', '76035', '76036', '76037', '76039', '76040', '76048', '76050', '76051', '76052', '76053', '76054', '76058', '76059', '76060', '76061', '76062', '76063', '76064', '76065', '76066', '76067', '76070', '76071', '76073', '76074', '76078', '76082', '76084', '76092', '76093', '76094', '76095', '76096', '76097', '76098', '76099', '76101', '76102', '76103', '76104', '76105', '76106', '76107', '76108', '76109', '76110', '76111', '76112', '76113', '76114', '76115', '76116', '76117', '76118', '76119', '76120', '76121', '76122', '76123', '76124', '76126', '76127', '76129', '76130', '76131', '76132', '76133', '76134', '76135', '76136', '76137', '76140', '76147', '76148', '76150', '76155', '76161', '76162', '76163', '76164', '76177', '76179', '76180', '76181', '76182', '76185', '76191', '76192', '76193', '76196', '76197', '76199'],
            'Collin County': ['75002', '75009', '75013', '75023', '75024', '75025', '75026', '75070', '75071', '75072', '75074', '75075', '75078', '75085', '75086', '75093', '75094', '75098', '75166', '75173', '75252', '75287', '75407', '75409', '75424', '75442', '75454', '75460', '75496'],
            'Denton County': ['76201', '76202', '76203', '76204', '76205', '76206', '76207', '76208', '76209', '76210', '76226', '76227', '76234', '76240', '76247', '76249', '76258', '76259', '76262', '76266', '76271', '76273', '75007', '75010', '75019', '75022', '75027', '75028', '75029', '75034', '75056', '75057', '75065', '75067', '75068', '75077', '75087', '75088', '75099'],
            'Rockwall County': ['75032', '75087', '75189', '75223'],
            'Ellis County': ['75104', '75119', '75134', '75154', '75165', '75167']
        }

    async def init_browser(self):
        """Initialize Playwright browser"""
        playwright = await async_playwright().start()
        self.browser = await playwright.chromium.launch(
            headless=True,
            args=[
                '--no-sandbox',
                '--disable-dev-shm-usage',
                '--disable-gpu',
                '--no-first-run',
                '--disable-extensions',
                '--disable-default-apps'
            ]
        )
        
        # Create a new page with realistic settings
        self.page = await self.browser.new_page(
            viewport={'width': 1366, 'height': 768},
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        
        # Block images and unnecessary resources to speed up scraping
        await self.page.route("**/*.{png,jpg,jpeg,gif,svg,ico,webp}", lambda route: route.abort())
        await self.page.route("**/*.{css,woff,woff2,ttf}", lambda route: route.abort())

    async def scrape_zipcode(self, zipcode: str, county: str) -> List[Dict]:
        """Scrape recently sold properties for a specific ZIP code"""
        properties = []
        
        try:
            # Build Zillow URL for recently sold homes in the ZIP code
            url = f"https://www.zillow.com/homes/recently_sold/{zipcode}_rb/"
            logger.info(f"Scraping {zipcode} ({county})...")
            
            # Navigate to the page
            await self.page.goto(url, wait_until='networkidle', timeout=30000)
            
            # Wait for the page to load
            await asyncio.sleep(random.uniform(2, 5))
            
            # Look for property cards
            property_cards = await self.page.query_selector_all('[data-test="property-card"]')
            
            if not property_cards:
                logger.info(f"No property cards found for {zipcode}")
                return properties
            
            logger.info(f"Found {len(property_cards)} property cards in {zipcode}")
            
            for i, card in enumerate(property_cards[:10]):  # Limit to first 10 properties per ZIP
                try:
                    # Extract property data
                    address_elem = await card.query_selector('[data-test="property-card-addr"]')
                    price_elem = await card.query_selector('[data-test="property-card-price"]')
                    details_elem = await card.query_selector('[data-test="property-card-details"]')
                    link_elem = await card.query_selector('a')
                    
                    # Get text content
                    address = await address_elem.inner_text() if address_elem else ''
                    price_text = await price_elem.inner_text() if price_elem else ''
                    details = await details_elem.inner_text() if details_elem else ''
                    link = await link_elem.get_attribute('href') if link_elem else ''
                    
                    # Parse price (remove $ and commas)
                    price = 0
                    if price_text:
                        price_clean = price_text.replace('$', '').replace(',', '').replace('Sold ', '')
                        try:
                            price = int(price_clean.split()[0]) if price_clean.split() else 0
                        except:
                            price = 0
                    
                    # Parse details (bedrooms, bathrooms, sqft)
                    bedrooms, bathrooms, sqft = '', '', ''
                    if details:
                        parts = details.split('•')
                        for part in parts:
                            part = part.strip()
                            if 'bd' in part:
                                bedrooms = part.replace('bd', '').strip()
                            elif 'ba' in part:
                                bathrooms = part.replace('ba', '').strip()
                            elif 'sqft' in part:
                                sqft = part.replace('sqft', '').replace(',', '').strip()
                    
                    property_data = {
                        'address': address.strip(),
                        'zipcode': zipcode,
                        'county': county,
                        'price': price,
                        'price_text': price_text,
                        'bedrooms': bedrooms,
                        'bathrooms': bathrooms,
                        'square_feet': sqft,
                        'details': details,
                        'zillow_url': f"https://www.zillow.com{link}" if link and not link.startswith('http') else link,
                        'scraped_at': datetime.now().isoformat()
                    }
                    
                    properties.append(property_data)
                    
                except Exception as e:
                    logger.error(f"Error extracting property {i} from {zipcode}: {e}")
                    continue
            
        except Exception as e:
            logger.error(f"Error scraping {zipcode}: {e}")
        
        return properties

    async def scrape_all_dfw(self, max_zips_per_county: int = 5) -> None:
        """Scrape all DFW counties"""
        await self.init_browser()
        
        try:
            for county, zipcodes in self.dfw_areas.items():
                logger.info(f"Starting {county} ({len(zipcodes)} ZIP codes available)")
                
                # Limit ZIP codes to avoid excessive requests
                selected_zips = zipcodes[:max_zips_per_county]
                
                for zipcode in selected_zips:
                    try:
                        zip_properties = await self.scrape_zipcode(zipcode, county)
                        self.all_properties.extend(zip_properties)
                        
                        # Rate limiting between ZIP codes
                        await asyncio.sleep(random.uniform(3, 7))
                        
                    except Exception as e:
                        logger.error(f"Error scraping {zipcode}: {e}")
                        continue
                
                logger.info(f"Completed {county}: {len([p for p in self.all_properties if p['county'] == county])} properties")
                
                # Save progress after each county
                await self.save_progress()
        
        finally:
            if self.browser:
                await self.browser.close()

    async def save_progress(self):
        """Save current progress to CSV"""
        if not self.all_properties:
            return
            
        filename = 'leads.csv'
        fieldnames = list(self.all_properties[0].keys())
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(self.all_properties)
        
        logger.info(f"Progress saved to {filename} ({len(self.all_properties)} properties)")

    def get_summary_stats(self) -> Dict[str, Any]:
        """Get summary statistics"""
        if not self.all_properties:
            return {}
        
        counties = {}
        zipcodes = {}
        price_ranges = {'under_200k': 0, '200k_400k': 0, '400k_600k': 0, 'over_600k': 0}
        
        for prop in self.all_properties:
            county = prop.get('county', 'Unknown')
            counties[county] = counties.get(county, 0) + 1
            
            zipcode = prop.get('zipcode', 'Unknown')
            zipcodes[zipcode] = zipcodes.get(zipcode, 0) + 1
            
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
            'top_zipcodes': dict(sorted(zipcodes.items(), key=lambda x: x[1], reverse=True)[:10]),
            'price_ranges': price_ranges
        }


async def main():
    """Main execution function"""
    logger.info("🏠 Starting Playwright DFW Zillow Scraper")
    
    scraper = PlaywrightZillowScraper()
    
    try:
        # Scrape with limited ZIP codes for CI/CD efficiency
        await scraper.scrape_all_dfw(max_zips_per_county=3)
        
        # Final save
        await scraper.save_progress()
        
        # Print summary
        stats = scraper.get_summary_stats()
        logger.info(f"✅ Scraping completed!")
        logger.info(f"Total Properties: {len(scraper.all_properties)}")
        logger.info(f"Counties: {stats.get('counties', {})}")
        logger.info(f"Price Ranges: {stats.get('price_ranges', {})}")
        
        return scraper.all_properties
        
    except Exception as e:
        logger.error(f"❌ Scraping failed: {e}")
        return []


if __name__ == "__main__":
    properties = asyncio.run(main())