#!/usr/bin/env python3
"""
Threaded Texas CAD Scraper using ScraperAPI
High-performance parallel scraping with Supabase integration
"""

import sys
import os
sys.path.append('..')

import re
from bs4 import BeautifulSoup
from base_scraper import threaded_scrape

# Target URLs - Texas CAD property search pages
TARGET_URLS = [
    # Dallas County CAD - sample property search URLs
    "https://www.dallascad.org/SearchResults.aspx?search=advanced&account=12345-001",
    "https://www.dallascad.org/SearchResults.aspx?search=advanced&account=12345-002",
    "https://www.dallascad.org/SearchResults.aspx?search=advanced&account=12345-003",
    
    # Tarrant County CAD - sample URLs
    "https://www.tad.org/property-search?account=54321-001",
    "https://www.tad.org/property-search?account=54321-002",
    
    # Harris County CAD - sample URLs
    "https://hcad.org/property-search?account=98765-001",
    "https://hcad.org/property-search?account=98765-002",
    
    # Add more real URLs here based on actual CAD search patterns
]

def parse_cad_data(html, source_url):
    """
    Parse CAD property data from HTML
    Returns dict matching cad_leads table schema
    """
    try:
        soup = BeautifulSoup(html, 'html.parser')
        
        # Extract account number from URL or page
        account_match = re.search(r'account=([^&]+)', source_url)
        account_number = account_match.group(1) if account_match else ""
        
        # Extract property details - these selectors need to be updated for real CAD sites
        # This is a template - actual implementation depends on specific CAD website structure
        
        # Owner name
        owner_element = soup.find('span', {'id': 'owner-name'}) or soup.find('td', text=re.compile('Owner'))
        owner_name = ""
        if owner_element:
            if owner_element.name == 'td':
                owner_name = owner_element.find_next_sibling('td').get_text(strip=True) if owner_element.find_next_sibling('td') else ""
            else:
                owner_name = owner_element.get_text(strip=True)
        
        # Property address
        address_element = soup.find('span', {'id': 'property-address'}) or soup.find('td', text=re.compile('Property Address'))
        address_text = ""
        if address_element:
            if address_element.name == 'td':
                address_text = address_element.find_next_sibling('td').get_text(strip=True) if address_element.find_next_sibling('td') else ""
            else:
                address_text = address_element.get_text(strip=True)
        
        # Parse city, zip from address
        city = ""
        zip_code = ""
        if address_text:
            # Extract city and zip from address string
            parts = address_text.split(',')
            if len(parts) >= 2:
                city_state_zip = parts[-1].strip()
                zip_match = re.search(r'(\d{5})', city_state_zip)
                if zip_match:
                    zip_code = zip_match.group(1)
                city = parts[-2].strip() if len(parts) >= 3 else city_state_zip.replace(zip_code, '').strip()
        
        # Appraised value
        value_element = soup.find('span', {'id': 'appraised-value'}) or soup.find('td', text=re.compile('Appraised Value'))
        appraised_value = 0
        if value_element:
            value_text = ""
            if value_element.name == 'td':
                value_text = value_element.find_next_sibling('td').get_text(strip=True) if value_element.find_next_sibling('td') else ""
            else:
                value_text = value_element.get_text(strip=True)
            
            # Extract numeric value
            value_match = re.search(r'[\$]?([\d,]+)', value_text.replace(',', ''))
            if value_match:
                appraised_value = int(value_match.group(1).replace(',', ''))
        
        # Year built
        year_element = soup.find('span', {'id': 'year-built'}) or soup.find('td', text=re.compile('Year Built'))
        year_built = None
        if year_element:
            year_text = ""
            if year_element.name == 'td':
                year_text = year_element.find_next_sibling('td').get_text(strip=True) if year_element.find_next_sibling('td') else ""
            else:
                year_text = year_element.get_text(strip=True)
            
            year_match = re.search(r'(\d{4})', year_text)
            if year_match:
                year_built = int(year_match.group(1))
        
        # Property type
        prop_type_element = soup.find('span', {'id': 'property-type'}) or soup.find('td', text=re.compile('Property Type'))
        property_type = ""
        if prop_type_element:
            if prop_type_element.name == 'td':
                property_type = prop_type_element.find_next_sibling('td').get_text(strip=True) if prop_type_element.find_next_sibling('td') else ""
            else:
                property_type = prop_type_element.get_text(strip=True)
        
        # Only return data if we have essential fields
        if account_number and (owner_name or address_text):
            return {
                "account_number": account_number,
                "owner_name": owner_name,
                "address_text": address_text,
                "city": city,
                "county": extract_county_from_url(source_url),
                "zip_code": zip_code,
                "property_type": property_type or "Unknown",
                "year_built": year_built,
                "square_feet": None,  # Would need additional parsing
                "lot_size_acres": None,  # Would need additional parsing
                "appraised_value": appraised_value,
                "market_value": appraised_value,  # Often same as appraised
                "homestead_exemption": False,  # Would need additional parsing
                "last_sale_date": None,  # Would need additional parsing
                "last_sale_price": None,  # Would need additional parsing
                "cad_url": source_url,
                "lead_score": calculate_lead_score(appraised_value, year_built)
            }
        
        return None
        
    except Exception as e:
        print(f"❌ Error parsing CAD data from {source_url}: {e}")
        return None

def extract_county_from_url(url):
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
    else:
        return "Unknown County"

def calculate_lead_score(appraised_value, year_built):
    """Calculate lead score based on property characteristics"""
    score = 5  # Base score
    
    # Value-based scoring
    if appraised_value > 500000:
        score += 3
    elif appraised_value > 300000:
        score += 2
    elif appraised_value > 200000:
        score += 1
    
    # Age-based scoring (older homes more likely to need roofing)
    if year_built:
        current_year = 2024
        age = current_year - year_built
        if age > 15:
            score += 3
        elif age > 10:
            score += 2
        elif age > 5:
            score += 1
    
    return min(score, 10)

def main():
    """Main execution function"""
    print("🏛️ Starting Threaded Texas CAD Scraper")
    print(f"📊 Targeting {len(TARGET_URLS)} CAD property URLs")
    
    # Run threaded scraping
    threaded_scrape(
        urls=TARGET_URLS,
        parse_func=parse_cad_data,
        table_name="cad_leads",
        threads=5
    )
    
    print("✅ Threaded CAD scraping completed!")

if __name__ == "__main__":
    main()