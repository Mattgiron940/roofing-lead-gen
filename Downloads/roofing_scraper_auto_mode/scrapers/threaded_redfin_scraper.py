#!/usr/bin/env python3
"""
Threaded Redfin Scraper using ScraperAPI
High-performance parallel scraping with Supabase integration
"""

import sys
import os
sys.path.append('..')

import re
import json
from bs4 import BeautifulSoup
from base_scraper import threaded_scrape

# Target URLs - Redfin property listing pages
TARGET_URLS = [
    # Dallas area listings
    "https://www.redfin.com/TX/Dallas/75201/filter/property-type=house",
    "https://www.redfin.com/TX/Dallas/75204/filter/property-type=house",
    "https://www.redfin.com/TX/Dallas/75206/filter/property-type=house",
    "https://www.redfin.com/TX/Dallas/75214/filter/property-type=house",
    
    # Fort Worth area
    "https://www.redfin.com/TX/Fort-Worth/76101/filter/property-type=house",
    "https://www.redfin.com/TX/Fort-Worth/76104/filter/property-type=house",
    
    # Plano area
    "https://www.redfin.com/TX/Plano/75023/filter/property-type=house",
    "https://www.redfin.com/TX/Plano/75024/filter/property-type=house",
    
    # Irving area
    "https://www.redfin.com/TX/Irving/75038/filter/property-type=house",
    "https://www.redfin.com/TX/Irving/75039/filter/property-type=house",
    
    # Add more ZIP codes as needed
]

def parse_redfin_data(html, source_url):
    """
    Parse Redfin property data from HTML
    Returns dict matching redfin_leads table schema
    """
    try:
        soup = BeautifulSoup(html, 'html.parser')
        
        # Redfin often loads data via JavaScript/JSON
        # Look for JSON data in script tags
        script_tags = soup.find_all('script')
        property_data = None
        
        for script in script_tags:
            if script.string and 'window.__INITIAL_STATE__' in script.string:
                # Extract JSON data
                json_match = re.search(r'window\.__INITIAL_STATE__\s*=\s*({.+?});', script.string)
                if json_match:
                    try:
                        data = json.loads(json_match.group(1))
                        # Navigate through the data structure to find property listings
                        if 'searchResults' in data and 'properties' in data['searchResults']:
                            properties = data['searchResults']['properties']
                            if properties:
                                property_data = properties[0]  # Take first property
                                break
                    except json.JSONDecodeError:
                        continue
        
        # If no JSON data found, try parsing HTML directly
        if not property_data:
            return parse_redfin_html(soup, source_url)
        
        # Extract data from JSON structure
        address_text = ""
        if 'address' in property_data:
            addr = property_data['address']
            street = addr.get('streetAddress', '')
            city = addr.get('city', '')
            state = addr.get('state', '')
            zip_code = addr.get('zip', '')
            address_text = f"{street}, {city}, {state} {zip_code}".strip()
        
        # Extract other fields
        price = property_data.get('price', 0)
        bedrooms = property_data.get('beds', 0)
        bathrooms = property_data.get('baths', 0)
        square_feet = property_data.get('sqFt', 0)
        year_built = property_data.get('yearBuilt', None)
        lot_size = property_data.get('lotSize', 0)
        
        # Property type
        property_type = property_data.get('propertyType', 'Unknown')
        
        # URL
        redfin_url = f"https://www.redfin.com{property_data.get('url', '')}" if property_data.get('url') else source_url
        
        # MLS number
        mls_number = property_data.get('mlsId', '')
        
        # Days on market
        days_on_redfin = property_data.get('dom', 0)
        
        # Calculate lead score
        lead_score = calculate_redfin_lead_score(price, year_built, square_feet)
        
        if address_text and price > 0:
            return {
                "address_text": address_text,
                "city": addr.get('city', '') if 'address' in property_data else extract_city_from_url(source_url),
                "state": addr.get('state', 'TX') if 'address' in property_data else 'TX',
                "zip_code": addr.get('zip', '') if 'address' in property_data else extract_zip_from_url(source_url),
                "county": extract_county_from_city(addr.get('city', '') if 'address' in property_data else extract_city_from_url(source_url)),
                "price": price,
                "num_bedrooms": bedrooms,
                "num_bathrooms": bathrooms,
                "square_feet": square_feet,
                "year_built": year_built,
                "property_type": property_type,
                "lot_size_sqft": lot_size,
                "sold_date": None,  # Would need additional parsing
                "days_on_redfin": days_on_redfin,
                "mls_number": mls_number,
                "price_per_sqft": f"${round(price / square_feet, 2)}" if square_feet > 0 else "",
                "redfin_url": redfin_url,
                "lead_score": lead_score,
                "hoa_fee": None,  # Would need additional parsing
                "parking_spaces": None  # Would need additional parsing
            }
        
        return None
        
    except Exception as e:
        print(f"❌ Error parsing Redfin data from {source_url}: {e}")
        return None

def parse_redfin_html(soup, source_url):
    """Fallback HTML parsing if JSON not available"""
    try:
        # Look for property cards or listings in HTML
        property_cards = soup.find_all('div', class_=re.compile(r'HomeCard|PropertyCard|listing'))
        
        if not property_cards:
            return None
        
        # Take first property card
        card = property_cards[0]
        
        # Extract address
        address_elem = card.find('div', class_=re.compile(r'address|Address'))
        address_text = address_elem.get_text(strip=True) if address_elem else ""
        
        # Extract price
        price_elem = card.find('span', class_=re.compile(r'price|Price'))
        price = 0
        if price_elem:
            price_text = price_elem.get_text(strip=True)
            price_match = re.search(r'\$?([\d,]+)', price_text.replace(',', ''))
            if price_match:
                price = int(price_match.group(1).replace(',', ''))
        
        # Extract beds/baths
        beds_baths_elem = card.find('div', class_=re.compile(r'beds|baths|stats'))
        bedrooms = 0
        bathrooms = 0
        if beds_baths_elem:
            text = beds_baths_elem.get_text()
            bed_match = re.search(r'(\d+)\s*bed', text, re.IGNORECASE)
            bath_match = re.search(r'(\d+(?:\.\d+)?)\s*bath', text, re.IGNORECASE)
            if bed_match:
                bedrooms = int(bed_match.group(1))
            if bath_match:
                bathrooms = float(bath_match.group(1))
        
        if address_text and price > 0:
            return {
                "address_text": address_text,
                "city": extract_city_from_url(source_url),
                "state": "TX",
                "zip_code": extract_zip_from_url(source_url),
                "county": extract_county_from_city(extract_city_from_url(source_url)),
                "price": price,
                "num_bedrooms": bedrooms,
                "num_bathrooms": bathrooms,
                "square_feet": None,
                "year_built": None,
                "property_type": "House",
                "lot_size_sqft": None,
                "sold_date": None,
                "days_on_redfin": None,
                "mls_number": "",
                "price_per_sqft": "",
                "redfin_url": source_url,
                "lead_score": calculate_redfin_lead_score(price, None, None),
                "hoa_fee": None,
                "parking_spaces": None
            }
        
        return None
        
    except Exception as e:
        print(f"❌ Error in HTML fallback parsing: {e}")
        return None

def extract_city_from_url(url):
    """Extract city name from Redfin URL"""
    city_match = re.search(r'/TX/([^/]+)/', url)
    if city_match:
        return city_match.group(1).replace('-', ' ')
    return "Unknown"

def extract_zip_from_url(url):
    """Extract ZIP code from Redfin URL"""
    zip_match = re.search(r'/(\d{5})/', url)
    if zip_match:
        return zip_match.group(1)
    return ""

def extract_county_from_city(city):
    """Map city to county"""
    city_county_map = {
        'Dallas': 'Dallas County',
        'Fort Worth': 'Tarrant County',
        'Plano': 'Collin County',
        'Irving': 'Dallas County',
        'Arlington': 'Tarrant County',
        'Garland': 'Dallas County',
        'Mesquite': 'Dallas County',
        'Richardson': 'Dallas County',
        'Grand Prairie': 'Dallas County',
        'McKinney': 'Collin County',
        'Frisco': 'Collin County',
        'Allen': 'Collin County'
    }
    return city_county_map.get(city, 'Unknown County')

def calculate_redfin_lead_score(price, year_built, square_feet):
    """Calculate lead score based on property characteristics"""
    score = 5  # Base score
    
    # Price-based scoring
    if price > 500000:
        score += 3
    elif price > 300000:
        score += 2
    elif price > 200000:
        score += 1
    
    # Age-based scoring
    if year_built:
        current_year = 2024
        age = current_year - year_built
        if age > 15:
            score += 3
        elif age > 10:
            score += 2
        elif age > 5:
            score += 1
    
    # Size-based scoring
    if square_feet:
        if square_feet > 3000:
            score += 2
        elif square_feet > 2000:
            score += 1
    
    return min(score, 10)

def main():
    """Main execution function"""
    print("🏘️ Starting Threaded Redfin Scraper")
    print(f"📊 Targeting {len(TARGET_URLS)} Redfin search URLs")
    
    # Run threaded scraping
    threaded_scrape(
        urls=TARGET_URLS,
        parse_func=parse_redfin_data,
        table_name="redfin_leads",
        threads=5
    )
    
    print("✅ Threaded Redfin scraping completed!")

if __name__ == "__main__":
    main()