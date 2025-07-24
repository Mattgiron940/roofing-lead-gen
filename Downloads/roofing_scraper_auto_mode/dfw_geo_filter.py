#!/usr/bin/env python3
"""
DFW Geographic Filtering Module
Filters leads to only include Dallas-Fort Worth metropolitan area
"""

import logging
from typing import Dict, List, Set, Optional
import re

logger = logging.getLogger(__name__)

class DFWGeoFilter:
    """Geographic filter for DFW metropolitan area"""
    
    def __init__(self):
        # Target counties in DFW metropolitan area
        self.target_counties = {
            'Dallas County', 'Dallas', 'DALLAS',
            'Tarrant County', 'Tarrant', 'TARRANT', 
            'Denton County', 'Denton', 'DENTON',
            'Collin County', 'Collin', 'COLLIN',
            'Rockwall County', 'Rockwall', 'ROCKWALL',
            'Ellis County', 'Ellis', 'ELLIS',
            'Kaufman County', 'Kaufman', 'KAUFMAN',
            'Parker County', 'Parker', 'PARKER',
            'Johnson County', 'Johnson', 'JOHNSON'
        }
        
        # DFW ZIP codes (comprehensive list)
        self.target_zip_codes = {
            # Dallas County
            '75001', '75006', '75007', '75010', '75019', '75038', '75039', '75041', 
            '75042', '75043', '75044', '75048', '75050', '75051', '75052', '75060', 
            '75061', '75062', '75063', '75067', '75068', '75069', '75070', '75071', 
            '75074', '75075', '75080', '75081', '75082', '75087', '75088', '75089', 
            '75104', '75115', '75116', '75126', '75134', '75137', '75141', '75149', 
            '75150', '75159', '75160', '75180', '75201', '75202', '75203', '75204', 
            '75205', '75206', '75207', '75208', '75209', '75210', '75211', '75212', 
            '75213', '75214', '75215', '75216', '75217', '75218', '75219', '75220', 
            '75221', '75222', '75223', '75224', '75225', '75226', '75227', '75228', 
            '75229', '75230', '75231', '75232', '75233', '75234', '75235', '75236', 
            '75237', '75238', '75239', '75240', '75241', '75242', '75243', '75244', 
            '75245', '75246', '75247', '75248', '75249', '75250', '75251', '75252', 
            '75253', '75254', '75255', '75256', '75257', '75258', '75260', '75261', 
            '75262', '75263', '75264', '75265', '75266', '75267', '75270', '75275', 
            '75277', '75283', '75284', '75285', '75286', '75287', '75294', '75295', 
            '75301', '75303', '75312', '75313', '75315', '75320', '75323', '75326', 
            '75336', '75339', '75342', '75346', '75347', '75348', '75349', '75350', 
            '75353', '75354', '75355', '75356', '75357', '75358', '75359', '75360', 
            '75363', '75364', '75367', '75368', '75370', '75371', '75372', '75373', 
            '75374', '75376', '75378', '75379', '75380', '75381', '75382', '75389', 
            '75390', '75391', '75392', '75393', '75394', '75395', '75396', '75397', 
            '75398',
            
            # Tarrant County
            '76001', '76002', '76003', '76004', '76005', '76006', '76007', '76008', 
            '76009', '76010', '76011', '76012', '76013', '76014', '76015', '76016', 
            '76017', '76018', '76019', '76020', '76021', '76022', '76028', '76031', 
            '76032', '76033', '76034', '76035', '76036', '76039', '76040', '76051', 
            '76052', '76053', '76054', '76055', '76058', '76059', '76060', '76061', 
            '76063', '76064', '76065', '76066', '76067', '76071', '76073', '76078', 
            '76082', '76092', '76094', '76095', '76096', '76097', '76099', '76101', 
            '76102', '76103', '76104', '76105', '76106', '76107', '76108', '76109', 
            '76110', '76111', '76112', '76113', '76114', '76115', '76116', '76117', 
            '76118', '76119', '76120', '76121', '76122', '76123', '76124', '76126', 
            '76127', '76129', '76130', '76131', '76132', '76133', '76134', '76135', 
            '76136', '76137', '76140', '76147', '76148', '76150', '76155', '76161', 
            '76162', '76163', '76164', '76166', '76177', '76179', '76180', '76181', 
            '76182', '76185', '76191', '76192', '76193', '76195', '76196', '76197', 
            '76198', '76199',
            
            # Collin County  
            '75002', '75009', '75013', '75023', '75024', '75025', '75026', '75030', 
            '75071', '75072', '75078', '75086', '75093', '75094', '75166', '75173', 
            '75189', '75252', '75287', '75407', '75409', '75442', '75454', '75460', 
            '75485', '75495',
            
            # Denton County
            '75019', '75022', '75028', '75056', '75057', '75065', '75077', '75165', 
            '76201', '76202', '76203', '76204', '76205', '76206', '76207', '76208', 
            '76209', '76210', '76225', '76226', '76227', '76244', '76247', '76249', 
            '76258', '76262', '76266', '76272',
            
            # Rockwall County
            '75032', '75087', '75189', '75496',
            
            # Ellis County
            '75104', '75119', '75154', '75165', '75167', '76065',
            
            # Kaufman County
            '75126', '75142', '75143', '75147', '75156', '75169', '75187', '75117',
            
            # Parker County
            '76008', '76020', '76073', '76078', '76085', '76086', '76087', '76088',
            
            # Johnson County
            '76028', '76031', '76033', '76049', '76050', '76059', '76070'
        }
        
        # Major DFW cities for additional filtering
        self.target_cities = {
            # Dallas County
            'Dallas', 'Irving', 'Garland', 'Mesquite', 'Richardson', 'Carrollton',
            'Plano', 'Grand Prairie', 'Addison', 'Balch Springs', 'Cedar Hill',
            'Cockrell Hill', 'Combine', 'Coppell', 'DeSoto', 'Duncanville',
            'Farmers Branch', 'Ferris', 'Glenn Heights', 'Highland Park',
            'Hutchins', 'Lancaster', 'Rowlett', 'Sachse', 'Seagoville',
            'Sunnyvale', 'University Park', 'Wilmer', 'Wylie',
            
            # Tarrant County
            'Fort Worth', 'Arlington', 'Mansfield', 'Euless', 'Bedford',
            'Grapevine', 'Hurst', 'Keller', 'North Richland Hills', 'Southlake',
            'Colleyville', 'Haltom City', 'Watauga', 'Richland Hills',
            'Forest Hill', 'Kennedale', 'Saginaw', 'White Settlement',
            'Azle', 'Benbrook', 'Blue Mound', 'Crowley', 'Everman',
            'Lake Worth', 'River Oaks', 'Sansom Park', 'Westworth Village',
            
            # Collin County
            'McKinney', 'Frisco', 'Allen', 'Wylie', 'Murphy', 'Prosper',
            'Celina', 'Anna', 'Fairview', 'Josephine', 'Little Elm',
            'Lowry Crossing', 'Lucas', 'Melissa', 'Nevada', 'New Hope',
            'Parker', 'Princeton', 'St. Paul', 'Westminster',
            
            # Denton County
            'Denton', 'Lewisville', 'Flower Mound', 'Coppell', 'Highland Village',
            'Carrollton', 'The Colony', 'Little Elm', 'Corinth', 'Lake Dallas',
            'Hickory Creek', 'Shady Shores', 'Copper Canyon', 'Double Oak',
            'Bartonville', 'Northlake', 'Roanoke', 'Trophy Club', 'Argyle',
            
            # Rockwall County
            'Rockwall', 'Rowlett', 'Royse City', 'Heath', 'McLendon-Chisholm',
            'Mobile City', 'Fate',
            
            # Ellis County
            'Waxahachie', 'Ennis', 'Midlothian', 'Red Oak', 'Cedar Hill',
            'DeSoto', 'Duncanville', 'Lancaster', 'Glenn Heights', 'Ovilla',
            'Ferris', 'Italy', 'Maypearl', 'Milford', 'Palmer', 'Pecan Hill',
            
            # Kaufman County
            'Kaufman', 'Forney', 'Terrell', 'Crandall', 'Combine', 'Seagoville',
            'Sunnyvale', 'Balch Springs', 'Mesquite', 'Garland',
            
            # Parker County
            'Weatherford', 'Azle', 'Aledo', 'Hudson Oaks', 'Willow Park',
            'Annetta', 'Annetta North', 'Annetta South', 'Cool', 'Millsap',
            'Springtown',
            
            # Johnson County
            'Cleburne', 'Burleson', 'Crowley', 'Joshua', 'Keene', 'Alvarado',
            'Venus', 'Grandview', 'Godley', 'Rio Vista'
        }
        
        # Normalize all city names to lowercase for matching
        self.target_cities_normalized = {city.lower() for city in self.target_cities}
        
        logger.info(f"Initialized DFW filter with {len(self.target_counties)} counties, "
                   f"{len(self.target_zip_codes)} ZIP codes, and {len(self.target_cities)} cities")
    
    def is_dfw_county(self, county: str) -> bool:
        """Check if county is in DFW area"""
        if not county:
            return False
        
        # Normalize county string
        county_clean = county.strip()
        
        # Check direct match or with "County" suffix
        return (county_clean in self.target_counties or 
                f"{county_clean} County" in self.target_counties or
                county_clean.replace(" County", "") in self.target_counties)
    
    def is_dfw_zip_code(self, zip_code: str) -> bool:
        """Check if ZIP code is in DFW area"""
        if not zip_code:
            return False
        
        # Extract 5-digit ZIP code
        zip_clean = re.sub(r'[^\d]', '', str(zip_code))[:5]
        return zip_clean in self.target_zip_codes
    
    def is_dfw_city(self, city: str) -> bool:
        """Check if city is in DFW area"""
        if not city:
            return False
        
        city_clean = city.strip().lower()
        return city_clean in self.target_cities_normalized
    
    def is_dfw_lead(self, lead_data: Dict) -> bool:
        """
        Comprehensive check if lead is in DFW area
        Uses county, ZIP code, and city matching with priority order
        """
        # Extract location fields from lead data
        county = lead_data.get('county') or lead_data.get('County')
        zip_code = (lead_data.get('zip_code') or 
                   lead_data.get('zipcode') or 
                   lead_data.get('ZIP') or 
                   lead_data.get('zip'))
        city = lead_data.get('city') or lead_data.get('City')
        
        # Priority 1: County match (most reliable)
        if county and self.is_dfw_county(county):
            logger.debug(f"Lead matched by county: {county}")
            return True
        
        # Priority 2: ZIP code match (very reliable)
        if zip_code and self.is_dfw_zip_code(zip_code):
            logger.debug(f"Lead matched by ZIP code: {zip_code}")
            return True
        
        # Priority 3: City match (good backup)
        if city and self.is_dfw_city(city):
            logger.debug(f"Lead matched by city: {city}")
            return True
        
        # No match found
        logger.debug(f"Lead not in DFW area - County: {county}, ZIP: {zip_code}, City: {city}")
        return False
    
    def filter_leads(self, leads: List[Dict]) -> tuple[List[Dict], List[Dict]]:
        """
        Filter leads into DFW and non-DFW lists
        Returns: (dfw_leads, non_dfw_leads)
        """
        dfw_leads = []
        non_dfw_leads = []
        
        for lead in leads:
            if self.is_dfw_lead(lead):
                # Add DFW flag to lead data
                lead['dfw'] = True
                dfw_leads.append(lead)
            else:
                lead['dfw'] = False
                non_dfw_leads.append(lead)
        
        logger.info(f"Filtered {len(leads)} leads: {len(dfw_leads)} DFW, {len(non_dfw_leads)} non-DFW")
        return dfw_leads, non_dfw_leads
    
    def get_dfw_zip_codes_by_county(self) -> Dict[str, List[str]]:
        """Get ZIP codes organized by county for targeted scraping"""
        county_zips = {
            'Dallas County': [
                '75001', '75006', '75007', '75010', '75019', '75038', '75039', '75041', 
                '75042', '75043', '75044', '75048', '75050', '75051', '75052', '75060', 
                '75061', '75062', '75063', '75067', '75068', '75069', '75070', '75071', 
                '75074', '75075', '75080', '75081', '75082', '75087', '75088', '75089', 
                '75201', '75202', '75203', '75204', '75205', '75206', '75207', '75208', 
                '75209', '75210', '75211', '75212', '75213', '75214', '75215', '75216', 
                '75217', '75218', '75219', '75220', '75221', '75222', '75223', '75224', 
                '75225', '75226', '75227', '75228', '75229', '75230', '75231', '75232', 
                '75233', '75234', '75235', '75236', '75237', '75238', '75239', '75240', 
                '75241', '75242', '75243', '75244', '75245', '75246', '75247', '75248', 
                '75249', '75250', '75251', '75252', '75253', '75254', '75255', '75256', 
                '75257', '75258', '75260', '75261', '75262', '75263', '75264', '75265', 
                '75266', '75267', '75270', '75275', '75277', '75283', '75284', '75285', 
                '75286', '75287', '75294', '75295'
            ],
            'Tarrant County': [
                '76001', '76002', '76003', '76004', '76005', '76006', '76007', '76008', 
                '76009', '76010', '76011', '76012', '76013', '76014', '76015', '76016', 
                '76017', '76018', '76019', '76020', '76021', '76022', '76028', '76031', 
                '76032', '76033', '76034', '76035', '76036', '76039', '76040', '76101', 
                '76102', '76103', '76104', '76105', '76106', '76107', '76108', '76109', 
                '76110', '76111', '76112', '76113', '76114', '76115', '76116', '76117', 
                '76118', '76119', '76120', '76121', '76122', '76123', '76124', '76126', 
                '76127', '76129', '76130', '76131', '76132', '76133', '76134', '76135', 
                '76136', '76137', '76140', '76147', '76148', '76150', '76155'
            ],
            'Collin County': [
                '75002', '75009', '75013', '75023', '75024', '75025', '75026', '75030', 
                '75071', '75072', '75078', '75086', '75093', '75094', '75166', '75173', 
                '75189', '75407', '75409', '75442', '75454', '75460', '75485', '75495'
            ],
            'Denton County': [
                '75019', '75022', '75028', '75056', '75057', '75065', '75077', '75165', 
                '76201', '76202', '76203', '76204', '76205', '76206', '76207', '76208', 
                '76209', '76210', '76225', '76226', '76227', '76244', '76247', '76249', 
                '76258', '76262', '76266', '76272'
            ],
            'Rockwall County': [
                '75032', '75087', '75189', '75496'
            ],
            'Ellis County': [
                '75104', '75119', '75154', '75165', '75167', '76065'
            ],
            'Kaufman County': [
                '75126', '75142', '75143', '75147', '75156', '75169', '75187', '75117'
            ],
            'Parker County': [
                '76008', '76020', '76073', '76078', '76085', '76086', '76087', '76088'
            ],
            'Johnson County': [
                '76028', '76031', '76033', '76049', '76050', '76059', '76070'
            ]
        }
        
        return county_zips
    
    def get_filter_stats(self) -> Dict[str, int]:
        """Get statistics about the filter configuration"""
        return {
            'target_counties': len(self.target_counties),
            'target_zip_codes': len(self.target_zip_codes),
            'target_cities': len(self.target_cities),
            'total_filters': len(self.target_counties) + len(self.target_zip_codes) + len(self.target_cities)
        }

# Global filter instance
dfw_filter = DFWGeoFilter()

def filter_lead_for_dfw(lead_data: Dict) -> bool:
    """
    Convenience function to check if a single lead is in DFW area
    """
    return dfw_filter.is_dfw_lead(lead_data)

def filter_leads_for_dfw(leads: List[Dict]) -> tuple[List[Dict], List[Dict]]:
    """
    Convenience function to filter a list of leads
    Returns: (dfw_leads, non_dfw_leads)
    """
    return dfw_filter.filter_leads(leads)

def get_dfw_zip_codes() -> Set[str]:
    """
    Get all DFW ZIP codes for scraper targeting
    """
    return dfw_filter.target_zip_codes.copy()

def get_dfw_counties() -> Set[str]:
    """
    Get all DFW counties for scraper targeting
    """
    return dfw_filter.target_counties.copy()

def get_dfw_cities() -> Set[str]:
    """
    Get all DFW cities for scraper targeting
    """
    return dfw_filter.target_cities.copy()

if __name__ == "__main__":
    # Test the filter
    test_leads = [
        {'address': '123 Main St', 'city': 'Dallas', 'county': 'Dallas County', 'zip_code': '75201'},
        {'address': '456 Oak Ave', 'city': 'Fort Worth', 'county': 'Tarrant County', 'zip_code': '76101'},
        {'address': '789 Pine St', 'city': 'Houston', 'county': 'Harris County', 'zip_code': '77001'},
        {'address': '321 Elm St', 'city': 'Austin', 'county': 'Travis County', 'zip_code': '78701'},
        {'address': '654 Cedar Ln', 'city': 'Plano', 'county': 'Collin County', 'zip_code': '75023'}
    ]
    
    dfw_leads, non_dfw_leads = filter_leads_for_dfw(test_leads)
    
    print(f"DFW Leads: {len(dfw_leads)}")
    for lead in dfw_leads:
        print(f"  - {lead['city']}, {lead['county']} {lead['zip_code']}")
    
    print(f"Non-DFW Leads: {len(non_dfw_leads)}")
    for lead in non_dfw_leads:
        print(f"  - {lead['city']}, {lead['county']} {lead['zip_code']}")
    
    print(f"Filter stats: {dfw_filter.get_filter_stats()}")