#!/usr/bin/env python3
"""
Threaded Building Permit Scraper using ScraperAPI
High-performance parallel scraping with Supabase integration
"""

import sys
import os
sys.path.append('..')

import re
from bs4 import BeautifulSoup
from base_scraper import threaded_scrape

# Target URLs - Building permit databases for DFW area
TARGET_URLS = [
    # Dallas permits
    "https://www.dallascityhall.com/departments/sustainabledevelopment/Pages/permits-search.aspx?permit_type=roofing",
    "https://www.dallascityhall.com/departments/sustainabledevelopment/Pages/permits-search.aspx?permit_type=residential",
    
    # Fort Worth permits
    "https://www.fortworthtexas.gov/departments/development-services/building-inspection/permits?type=roofing",
    "https://www.fortworthtexas.gov/departments/development-services/building-inspection/permits?type=residential",
    
    # Plano permits
    "https://www.plano.gov/1207/Building-Permits?category=roofing",
    "https://www.plano.gov/1207/Building-Permits?category=residential",
    
    # Irving permits
    "https://www.cityofirving.org/building-permits?type=roofing",
    "https://www.cityofirving.org/building-permits?type=residential",
    
    # Arlington permits
    "https://www.arlingtontx.gov/building-permits?category=roofing",
    "https://www.arlingtontx.gov/building-permits?category=residential",
    
    # Garland permits
    "https://www.garlandtx.gov/permits?filter=roofing",
    "https://www.garlandtx.gov/permits?filter=residential",
    
    # Richardson permits
    "https://www.cor.net/building-permits?type=roofing",
    "https://www.cor.net/building-permits?type=residential",
]

def parse_permit_data(html, source_url):
    """
    Parse building permit data from HTML
    Returns dict matching permit_leads table schema
    """
    try:
        soup = BeautifulSoup(html, 'html.parser')
        
        # Look for permit tables, rows, or cards
        permit_tables = soup.find_all('table', class_=re.compile(r'permit|result|data'))
        permit_rows = soup.find_all('tr', class_=re.compile(r'permit|result|row'))
        permit_divs = soup.find_all('div', class_=re.compile(r'permit|result|card'))
        
        permits = []
        
        # Try parsing table structure
        if permit_tables:
            for table in permit_tables:
                rows = table.find_all('tr')[1:]  # Skip header
                for row in rows:
                    permit = parse_permit_row(row, source_url)
                    if permit:
                        permits.append(permit)
        
        # Try parsing individual rows
        elif permit_rows:
            for row in permit_rows:
                permit = parse_permit_row(row, source_url)
                if permit:
                    permits.append(permit)
        
        # Try parsing div cards
        elif permit_divs:
            for div in permit_divs:
                permit = parse_permit_div(div, source_url)
                if permit:
                    permits.append(permit)
        
        # Return first valid permit found
        return permits[0] if permits else None
        
    except Exception as e:
        print(f"❌ Error parsing permit data from {source_url}: {e}")
        return None

def parse_permit_row(row, source_url):
    """Parse permit data from table row"""
    try:
        cells = row.find_all(['td', 'th'])
        if len(cells) < 4:
            return None
        
        # Common patterns for permit data
        permit_id = ""
        address_text = ""
        permit_type = ""
        permit_value = ""
        date_filed = ""
        contractor_name = ""
        status = ""
        
        # Extract data based on common patterns
        for i, cell in enumerate(cells):
            text = cell.get_text(strip=True)
            
            # Permit ID (usually first column)
            if i == 0 and re.match(r'^[A-Z0-9\-]+$', text):
                permit_id = text
            
            # Address (usually contains street number and name)
            elif re.search(r'\d+\s+\w+\s+(st|street|ave|avenue|dr|drive|blvd|boulevard|ln|lane|rd|road)', text, re.IGNORECASE):
                address_text = text
            
            # Permit type (contains roofing, residential, etc.)
            elif re.search(r'(roof|residential|commercial|repair|replacement)', text, re.IGNORECASE):
                permit_type = text
            
            # Value (contains $ or numeric with commas)
            elif re.search(r'[\$]?[\d,]+\.?\d*', text) and not re.match(r'^\d{1,2}\/\d{1,2}\/\d{4}$', text):
                permit_value = text
            
            # Date (MM/DD/YYYY or similar)
            elif re.search(r'\d{1,2}\/\d{1,2}\/\d{4}', text):
                date_filed = text
            
            # Status
            elif text.lower() in ['approved', 'pending', 'issued', 'completed', 'active', 'expired']:
                status = text
            
            # Contractor (might be a company name)
            elif len(text) > 5 and not any(x in text.lower() for x in ['permit', 'roof', 'residential']) and permit_id and address_text:
                contractor_name = text
        
        # Extract city from address or URL
        city = extract_city_from_permit_url(source_url)
        zip_code = extract_zip_from_address(address_text)
        
        # Calculate lead priority
        lead_priority = calculate_permit_lead_priority(permit_type, permit_value, date_filed)
        
        if permit_id and address_text:
            return {
                "permit_id": permit_id,
                "address_text": address_text,
                "city": city,
                "zip_code": zip_code,
                "permit_type": permit_type or "Unknown",
                "work_description": permit_type,  # Use permit type as description
                "date_filed": date_filed,
                "permit_value": permit_value,
                "contractor_name": contractor_name,
                "status": status or "Unknown",
                "lead_priority": lead_priority
            }
        
        return None
        
    except Exception as e:
        print(f"❌ Error parsing permit row: {e}")
        return None

def parse_permit_div(div, source_url):
    """Parse permit data from div card"""
    try:
        text = div.get_text()
        
        # Extract permit ID
        permit_id_match = re.search(r'permit\s*#?\s*:?\s*([A-Z0-9\-]+)', text, re.IGNORECASE)
        permit_id = permit_id_match.group(1) if permit_id_match else ""
        
        # Extract address
        address_match = re.search(r'address\s*:?\s*([^\n]+)', text, re.IGNORECASE)
        address_text = address_match.group(1).strip() if address_match else ""
        
        # Extract permit type
        type_match = re.search(r'type\s*:?\s*([^\n]+)', text, re.IGNORECASE)
        permit_type = type_match.group(1).strip() if type_match else ""
        
        # Extract value
        value_match = re.search(r'value\s*:?\s*\$?([\d,]+)', text, re.IGNORECASE)
        permit_value = f"${value_match.group(1)}" if value_match else ""
        
        # Extract date
        date_match = re.search(r'date\s*:?\s*(\d{1,2}\/\d{1,2}\/\d{4})', text, re.IGNORECASE)
        date_filed = date_match.group(1) if date_match else ""
        
        city = extract_city_from_permit_url(source_url)
        zip_code = extract_zip_from_address(address_text)
        lead_priority = calculate_permit_lead_priority(permit_type, permit_value, date_filed)
        
        if permit_id and address_text:
            return {
                "permit_id": permit_id,
                "address_text": address_text,
                "city": city,
                "zip_code": zip_code,
                "permit_type": permit_type or "Unknown",
                "work_description": permit_type,
                "date_filed": date_filed,
                "permit_value": permit_value,
                "contractor_name": "",
                "status": "Unknown",
                "lead_priority": lead_priority
            }
        
        return None
        
    except Exception as e:
        print(f"❌ Error parsing permit div: {e}")
        return None

def extract_city_from_permit_url(url):
    """Extract city name from permit URL"""
    if 'dallas' in url.lower():
        return "Dallas"
    elif 'fortworth' in url.lower():
        return "Fort Worth"
    elif 'plano' in url.lower():
        return "Plano"
    elif 'irving' in url.lower():
        return "Irving"
    elif 'arlington' in url.lower():
        return "Arlington"
    elif 'garland' in url.lower():
        return "Garland"
    elif 'richardson' in url.lower() or 'cor.net' in url.lower():
        return "Richardson"
    else:
        return "Unknown"

def extract_zip_from_address(address):
    """Extract ZIP code from address string"""
    zip_match = re.search(r'(\d{5})', address)
    return zip_match.group(1) if zip_match else ""

def calculate_permit_lead_priority(permit_type, permit_value, date_filed):
    """Calculate lead priority based on permit characteristics"""
    priority = 5  # Base priority
    
    # Type-based priority
    if permit_type:
        type_lower = permit_type.lower()
        if 'roof' in type_lower:
            priority += 4  # Roofing permits are highest priority
        elif 'residential' in type_lower:
            priority += 2
        elif 'repair' in type_lower or 'replacement' in type_lower:
            priority += 3
    
    # Value-based priority
    if permit_value:
        value_str = re.sub(r'[^\d]', '', permit_value)
        if value_str.isdigit():
            value = int(value_str)
            if value > 20000:
                priority += 3
            elif value > 10000:
                priority += 2
            elif value > 5000:
                priority += 1
    
    # Recency-based priority
    if date_filed:
        try:
            from datetime import datetime
            permit_date = datetime.strptime(date_filed, '%m/%d/%Y')
            days_ago = (datetime.now() - permit_date).days
            
            if days_ago <= 30:
                priority += 2  # Very recent
            elif days_ago <= 90:
                priority += 1  # Recent
        except:
            pass
    
    return min(priority, 10)

def main():
    """Main execution function"""
    print("🏗️ Starting Threaded Permit Scraper")
    print(f"📊 Targeting {len(TARGET_URLS)} permit database URLs")
    
    # Run threaded scraping
    threaded_scrape(
        urls=TARGET_URLS,
        parse_func=parse_permit_data,
        table_name="permit_leads",
        threads=3  # Lower thread count for permit sites
    )
    
    print("✅ Threaded permit scraping completed!")

if __name__ == "__main__":
    main()