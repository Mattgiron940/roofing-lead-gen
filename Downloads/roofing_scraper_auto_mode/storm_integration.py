#!/usr/bin/env python3
"""
Storm/Hail Damage Data Integration
Integrates HailTrace and NOAA feeds with scraped properties
Matches ZIP codes to identify recent storm-hit areas for prioritization
"""

import requests
import json
import csv
from datetime import datetime, timedelta
from typing import List, Dict, Any, Set
import logging
from supabase_config import insert_lead

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class StormDataIntegrator:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
        
        # Storm data sources
        self.data_sources = {
            'noaa': {
                'base_url': 'https://api.weather.gov',
                'storm_endpoint': '/alerts',
                'radar_endpoint': '/radar'
            },
            'hailtrace': {
                'base_url': 'https://api.hailtrace.com',
                'hail_endpoint': '/v1/hail-reports'
            }
        }
        
        self.storm_events = []
        self.affected_zipcodes = set()

    def create_sample_storm_data(self) -> List[Dict]:
        """Create realistic storm event data for Texas"""
        
        # Recent storm events in Texas (sample data)
        sample_storms = [
            {
                'event_id': 'TX-HAIL-2024-001',
                'event_type': 'Hail Storm',
                'date': '2024-11-15',
                'time': '15:30',
                'severity': 'Severe',
                'hail_size': '2.5 inches',
                'wind_speed': '70 mph',
                'affected_counties': ['Dallas', 'Tarrant', 'Collin'],
                'affected_zipcodes': ['75201', '75204', '75206', '76101', '76104', '75002', '75009'],
                'damage_estimate': 'Moderate to Severe',
                'insurance_claims_expected': 'High'
            },
            {
                'event_id': 'TX-STORM-2024-002', 
                'event_type': 'Severe Thunderstorm',
                'date': '2024-10-28',
                'time': '18:45',
                'severity': 'Moderate',
                'hail_size': '1.75 inches',
                'wind_speed': '65 mph',
                'affected_counties': ['Denton', 'Ellis'],
                'affected_zipcodes': ['76201', '76205', '75104', '75119'],
                'damage_estimate': 'Moderate',
                'insurance_claims_expected': 'Medium'
            },
            {
                'event_id': 'TX-HAIL-2024-003',
                'event_type': 'Hail Storm',
                'date': '2024-09-12',
                'time': '14:20',
                'severity': 'Extreme', 
                'hail_size': '3.0 inches',
                'wind_speed': '80 mph',
                'affected_counties': ['Harris', 'Fort Bend'],
                'affected_zipcodes': ['77001', '77002', '77003', '77469', '77478'],
                'damage_estimate': 'Severe',
                'insurance_claims_expected': 'Very High'
            },
            {
                'event_id': 'TX-STORM-2024-004',
                'event_type': 'Tornado/Hail',
                'date': '2024-08-05',
                'time': '19:15',
                'severity': 'Severe',
                'hail_size': '2.0 inches',
                'wind_speed': '85 mph',
                'affected_counties': ['Bexar', 'Travis'],
                'affected_zipcodes': ['78201', '78202', '78701', '78702'],
                'damage_estimate': 'Severe',
                'insurance_claims_expected': 'High'
            },
            {
                'event_id': 'TX-HAIL-2024-005',
                'event_type': 'Hail Storm',
                'date': '2024-07-20',
                'time': '16:30',
                'severity': 'Moderate',
                'hail_size': '1.5 inches', 
                'wind_speed': '55 mph',
                'affected_counties': ['Montgomery', 'Rockwall'],
                'affected_zipcodes': ['77301', '77302', '75032', '75087'],
                'damage_estimate': 'Light to Moderate',
                'insurance_claims_expected': 'Medium'
            }
        ]
        
        return sample_storms

    def get_recent_storm_events(self, days_back: int = 90) -> List[Dict]:
        """Get storm events from the last N days"""
        logger.info(f"📡 Fetching storm events from last {days_back} days...")
        
        try:
            # For now, use sample data
            # In production, this would call real APIs
            storm_events = self.create_sample_storm_data()
            
            # Filter by date range
            cutoff_date = datetime.now() - timedelta(days=days_back)
            recent_storms = []
            
            for storm in storm_events:
                storm_date = datetime.strptime(storm['date'], '%Y-%m-%d')
                if storm_date >= cutoff_date:
                    recent_storms.append(storm)
                    
            logger.info(f"Found {len(recent_storms)} recent storm events")
            self.storm_events = recent_storms
            
            # Insert storm events into Supabase
            for storm in recent_storms:
                supabase_storm_data = {
                    'event_id': storm['event_id'],
                    'event_type': storm['event_type'],
                    'event_date': storm['date'],
                    'event_time': storm['time'],
                    'severity': storm['severity'],
                    'hail_size': storm['hail_size'],
                    'wind_speed': storm['wind_speed'],
                    'affected_counties': ','.join(storm['affected_counties']),
                    'affected_zipcodes': ','.join(storm['affected_zipcodes']),
                    'damage_estimate': storm['damage_estimate'],
                    'insurance_claims_expected': storm['insurance_claims_expected']
                }
                
                if insert_lead('storm_events', supabase_storm_data):
                    logger.debug(f"✅ Inserted storm event: {storm['event_id']}")
            
            # Extract affected ZIP codes
            for storm in recent_storms:
                self.affected_zipcodes.update(storm['affected_zipcodes'])
            
            logger.info(f"Total affected ZIP codes: {len(self.affected_zipcodes)}")
            return recent_storms
            
        except Exception as e:
            logger.error(f"Error fetching storm data: {e}")
            return []

    def match_properties_to_storms(self, properties: List[Dict]) -> List[Dict]:
        """Match property data with storm events"""
        logger.info("🎯 Matching properties to storm events...")
        
        if not self.storm_events:
            logger.warning("No storm events to match against")
            return properties
        
        enhanced_properties = []
        storm_matched_count = 0
        
        for prop in properties:
            prop_zipcode = str(prop.get('zipcode', ''))
            enhanced_prop = prop.copy()
            
            # Check if property is in storm-affected area
            if prop_zipcode in self.affected_zipcodes:
                storm_matched_count += 1
                
                # Find all storms that affected this ZIP code
                affecting_storms = []
                for storm in self.storm_events:
                    if prop_zipcode in storm['affected_zipcodes']:
                        affecting_storms.append(storm)
                
                # Add storm data to property
                enhanced_prop['storm_affected'] = True
                enhanced_prop['storm_count'] = len(affecting_storms)
                enhanced_prop['recent_storms'] = affecting_storms
                
                # Calculate storm priority boost
                storm_priority = self.calculate_storm_priority(affecting_storms)
                enhanced_prop['storm_priority'] = storm_priority
                
                # Boost lead score based on storm damage
                original_score = enhanced_prop.get('lead_score', 5)
                enhanced_prop['lead_score'] = min(original_score + storm_priority, 10)
                enhanced_prop['storm_boost'] = storm_priority
                
                # Add most recent/severe storm details
                if affecting_storms:
                    most_recent = max(affecting_storms, key=lambda x: x['date'])
                    enhanced_prop['latest_storm_date'] = most_recent['date']
                    enhanced_prop['latest_storm_severity'] = most_recent['severity']
                    enhanced_prop['latest_hail_size'] = most_recent['hail_size']
            else:
                enhanced_prop['storm_affected'] = False
                enhanced_prop['storm_count'] = 0
                enhanced_prop['storm_priority'] = 0
                enhanced_prop['storm_boost'] = 0
            
            enhanced_properties.append(enhanced_prop)
        
        logger.info(f"✅ Matched {storm_matched_count} properties to storm events")
        return enhanced_properties

    def calculate_storm_priority(self, storms: List[Dict]) -> int:
        """Calculate priority boost based on storm severity and recency"""
        if not storms:
            return 0
        
        priority_boost = 0
        
        for storm in storms:
            # Severity-based priority
            severity = storm.get('severity', '').lower()
            if 'extreme' in severity:
                priority_boost += 4
            elif 'severe' in severity:
                priority_boost += 3
            elif 'moderate' in severity:
                priority_boost += 2
            else:
                priority_boost += 1
            
            # Hail size priority
            hail_size = storm.get('hail_size', '0 inches')
            try:
                size = float(hail_size.split()[0])
                if size >= 2.5:
                    priority_boost += 3
                elif size >= 2.0:
                    priority_boost += 2
                elif size >= 1.5:
                    priority_boost += 1
            except:
                pass
            
            # Recency bonus
            try:
                storm_date = datetime.strptime(storm['date'], '%Y-%m-%d')
                days_ago = (datetime.now() - storm_date).days
                
                if days_ago <= 30:
                    priority_boost += 2  # Very recent
                elif days_ago <= 60:
                    priority_boost += 1  # Recent
            except:
                pass
        
        return min(priority_boost, 5)  # Cap storm boost at 5

    def generate_storm_report(self, enhanced_properties: List[Dict]) -> Dict[str, Any]:
        """Generate comprehensive storm impact report"""
        if not enhanced_properties:
            return {}
        
        storm_affected = [p for p in enhanced_properties if p.get('storm_affected', False)]
        high_priority = [p for p in storm_affected if p.get('storm_priority', 0) >= 4]
        
        # County-level storm impact
        county_impact = {}
        for prop in storm_affected:
            county = prop.get('county', 'Unknown')
            if county not in county_impact:
                county_impact[county] = {
                    'properties': 0,
                    'avg_priority': 0,
                    'storm_events': set()
                }
            
            county_impact[county]['properties'] += 1
            county_impact[county]['avg_priority'] += prop.get('storm_priority', 0)
            
            # Track storm events per county
            for storm in prop.get('recent_storms', []):
                county_impact[county]['storm_events'].add(storm['event_id'])
        
        # Calculate averages
        for county_data in county_impact.values():
            if county_data['properties'] > 0:
                county_data['avg_priority'] = round(
                    county_data['avg_priority'] / county_data['properties'], 2
                )
                county_data['storm_events'] = len(county_data['storm_events'])
        
        return {
            'total_properties': len(enhanced_properties),
            'storm_affected_properties': len(storm_affected),
            'storm_affected_percentage': round(len(storm_affected) / len(enhanced_properties) * 100, 1),
            'high_priority_storm_leads': len(high_priority),
            'total_storm_events': len(self.storm_events),
            'affected_zipcodes': len(self.affected_zipcodes),
            'county_impact': county_impact,
            'recent_major_storms': [s for s in self.storm_events if s.get('severity') in ['Severe', 'Extreme']],
            'generated_at': datetime.now().isoformat()
        }

    def save_enhanced_properties_csv(self, enhanced_properties: List[Dict], filename: str = 'storm_enhanced_properties.csv'):
        """Save storm-enhanced property data to CSV"""
        if not enhanced_properties:
            return
        
        fieldnames = list(enhanced_properties[0].keys())
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(enhanced_properties)
        
        logger.info(f"💾 Saved {len(enhanced_properties)} storm-enhanced properties to {filename}")

    def save_storm_report_json(self, report: Dict, filename: str = 'storm_impact_report.json'):
        """Save storm impact report to JSON"""
        with open(filename, 'w', encoding='utf-8') as jsonfile:
            json.dump(report, jsonfile, indent=2, default=str)
        
        logger.info(f"📊 Saved storm impact report to {filename}")


def integrate_storm_data_with_properties(properties: List[Dict], days_back: int = 90) -> tuple:
    """Main function to integrate storm data with property listings"""
    logger.info("⛈️  Starting Storm Data Integration")
    logger.info("=" * 50)
    
    integrator = StormDataIntegrator()
    
    try:
        # Get recent storm events
        storm_events = integrator.get_recent_storm_events(days_back)
        
        if not storm_events:
            logger.warning("No recent storm events found")
            return properties, {}
        
        # Match properties to storms
        enhanced_properties = integrator.match_properties_to_storms(properties)
        
        # Generate comprehensive report
        storm_report = integrator.generate_storm_report(enhanced_properties)
        
        # Save results
        integrator.save_enhanced_properties_csv(enhanced_properties)
        integrator.save_storm_report_json(storm_report)
        
        # Log summary
        logger.info("📊 STORM INTEGRATION SUMMARY:")
        logger.info(f"   • Total Properties: {storm_report.get('total_properties', 0)}")
        logger.info(f"   • Storm-Affected: {storm_report.get('storm_affected_properties', 0)} ({storm_report.get('storm_affected_percentage', 0)}%)")
        logger.info(f"   • High Priority Leads: {storm_report.get('high_priority_storm_leads', 0)}")
        logger.info(f"   • Recent Storm Events: {storm_report.get('total_storm_events', 0)}")
        logger.info(f"   • Affected ZIP Codes: {storm_report.get('affected_zipcodes', 0)}")
        
        logger.info("🎯 High-Impact Counties:")
        county_impact = storm_report.get('county_impact', {})
        for county, impact in sorted(county_impact.items(), key=lambda x: x[1]['properties'], reverse=True)[:5]:
            logger.info(f"   • {county}: {impact['properties']} properties, avg priority {impact['avg_priority']}")
        
        logger.info("✅ Storm integration completed successfully!")
        
        return enhanced_properties, storm_report
        
    except Exception as e:
        logger.error(f"❌ Storm integration failed: {e}")
        return properties, {}


def main():
    """Test function"""
    # Sample property data for testing
    sample_properties = [
        {'address': '123 Main St, Dallas, TX', 'zipcode': '75201', 'county': 'Dallas County', 'lead_score': 6},
        {'address': '456 Oak Ave, Fort Worth, TX', 'zipcode': '76101', 'county': 'Tarrant County', 'lead_score': 7},
        {'address': '789 Elm St, Houston, TX', 'zipcode': '77001', 'county': 'Harris County', 'lead_score': 5}
    ]
    
    enhanced_props, report = integrate_storm_data_with_properties(sample_properties)
    
    return len(enhanced_props)


if __name__ == "__main__":
    main()