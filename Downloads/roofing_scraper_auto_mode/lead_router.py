#!/usr/bin/env python3
"""
Smart Lead Routing & Prioritization System
Advanced lead scoring with geographic, demographic, and behavioral analysis
"""

import os
import json
import re
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
import logging
from dataclasses import dataclass, asdict
from enum import Enum

from supabase_config import SupabaseConnection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class LeadStatus(Enum):
    HOT = "hot"
    WARM = "warm"
    COLD = "cold"
    FOLLOW_UP = "follow_up"
    CONTACTED = "contacted"
    QUALIFIED = "qualified"
    DISQUALIFIED = "disqualified"
    CONVERTED = "converted"

class Priority(Enum):
    URGENT = "urgent"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"

@dataclass
class RoutingRule:
    name: str
    conditions: Dict[str, Any]
    actions: Dict[str, Any]
    priority: int = 1
    enabled: bool = True

@dataclass
class LeadScore:
    base_score: int
    property_value_score: int
    age_score: int
    location_score: int
    source_score: int
    storm_score: int
    permit_score: int
    urgency_score: int
    total_score: int
    priority: Priority
    status: LeadStatus
    routing_tags: List[str]
    next_action: str
    assigned_to: Optional[str] = None

class LeadRouter:
    def __init__(self):
        self.supabase_conn = SupabaseConnection()
        self.routing_rules = self.load_routing_rules()
        self.priority_zip_codes = self.load_priority_zip_codes()
        self.high_value_areas = self.load_high_value_areas()
        self.storm_affected_areas = self.load_storm_affected_areas()
        
    def load_routing_rules(self) -> List[RoutingRule]:
        """Load routing rules from configuration"""
        return [
            # Urgent - Recent storm damage + high value
            RoutingRule(
                name="Storm_HighValue_Urgent",
                conditions={
                    "storm_affected": True,
                    "min_property_value": 400000,
                    "max_days_old": 3
                },
                actions={
                    "status": LeadStatus.HOT,
                    "priority": Priority.URGENT,
                    "tags": ["storm-damage", "high-value", "urgent"],
                    "next_action": "immediate_call",
                    "assigned_to": "senior_closer"
                },
                priority=1
            ),
            
            # Hot - New roofing permits
            RoutingRule(
                name="Active_Roofing_Permits", 
                conditions={
                    "source": "permit",
                    "permit_type_contains": ["roof", "roofing", "re-roof"],
                    "max_days_old": 7
                },
                actions={
                    "status": LeadStatus.HOT,
                    "priority": Priority.HIGH,
                    "tags": ["active-roofing", "permits"],
                    "next_action": "call_within_24h",
                    "assigned_to": "permit_specialist"
                },
                priority=2
            ),
            
            # High Priority - High-value properties in target areas
            RoutingRule(
                name="HighValue_TargetAreas",
                conditions={
                    "min_property_value": 350000,
                    "zip_codes": ["75201", "75204", "75206", "75024", "75070", "76101"],
                    "max_days_old": 14
                },
                actions={
                    "status": LeadStatus.WARM,
                    "priority": Priority.HIGH,
                    "tags": ["high-value", "target-area"],
                    "next_action": "call_within_48h",
                    "assigned_to": "senior_rep"
                },
                priority=3
            ),
            
            # Warm - Older homes likely needing roof work
            RoutingRule(
                name="Aging_Properties",
                conditions={
                    "max_year_built": 2005,
                    "min_property_value": 200000,
                    "property_types": ["Single Family Residence", "Townhouse"]
                },
                actions={
                    "status": LeadStatus.WARM,
                    "priority": Priority.MEDIUM,
                    "tags": ["aging-roof", "residential"],
                    "next_action": "call_within_week",
                    "assigned_to": "standard_rep"
                },
                priority=4
            ),
            
            # Follow-up - Previously contacted leads
            RoutingRule(
                name="Follow_Up_Cycle",
                conditions={
                    "last_contact_days_ago": {"min": 7, "max": 30},
                    "previous_status": ["contacted", "warm"]
                },
                actions={
                    "status": LeadStatus.FOLLOW_UP,
                    "priority": Priority.MEDIUM,
                    "tags": ["follow-up", "nurture"],
                    "next_action": "follow_up_call",
                    "assigned_to": "follow_up_team"
                },
                priority=5
            ),
            
            # Cold - Low priority leads
            RoutingRule(
                name="Standard_Processing",
                conditions={},  # Default catch-all
                actions={
                    "status": LeadStatus.COLD,
                    "priority": Priority.LOW,
                    "tags": ["standard"],
                    "next_action": "add_to_drip_campaign",
                    "assigned_to": "junior_rep"
                },
                priority=10
            )
        ]
    
    def load_priority_zip_codes(self) -> Dict[str, int]:
        """Load ZIP codes with priority scores"""
        return {
            # High-income Dallas suburbs
            "75201": 10, "75204": 9, "75206": 8, "75214": 7,
            "75230": 9, "75240": 8, "75248": 7,
            
            # Plano/Frisco area
            "75023": 10, "75024": 10, "75025": 9, "75070": 9,
            "75071": 8, "75093": 9,
            
            # Fort Worth high-value areas
            "76101": 8, "76104": 7, "76107": 9, "76109": 8,
            
            # Other target areas
            "75034": 8, "75035": 7, "75075": 9, "75080": 8,
            "75081": 7, "75082": 8, "75254": 9
        }
    
    def load_high_value_areas(self) -> Dict[str, Dict[str, Any]]:
        """Load high-value area definitions"""
        return {
            "North_Dallas": {
                "zip_codes": ["75201", "75204", "75206"],
                "avg_home_value": 450000,
                "market_score": 9
            },
            "Plano_Frisco": {
                "zip_codes": ["75023", "75024", "75025"],
                "avg_home_value": 520000,
                "market_score": 10
            },
            "West_Fort_Worth": {
                "zip_codes": ["76107", "76109", "76116"],
                "avg_home_value": 380000,
                "market_score": 8
            }
        }
    
    def load_storm_affected_areas(self) -> List[str]:
        """Get storm-affected ZIP codes from database"""
        if not self.supabase_conn.supabase:
            return []
        
        try:
            # Get recent storm events
            cutoff_date = (datetime.now() - timedelta(days=90)).isoformat()
            
            result = self.supabase_conn.supabase.table('storm_events')\
                .select('affected_zipcodes')\
                .gte('created_at', cutoff_date)\
                .execute()
            
            storm_zips = set()
            if result.data:
                for event in result.data:
                    zip_codes = event.get('affected_zipcodes', '')
                    if zip_codes:
                        storm_zips.update(zip_codes.split(','))
            
            return list(storm_zips)
            
        except Exception as e:
            logger.error(f"Error loading storm-affected areas: {e}")
            return []
    
    def calculate_property_value_score(self, price: int) -> int:
        """Calculate score based on property value"""
        if price >= 600000:
            return 10
        elif price >= 450000:
            return 9
        elif price >= 350000:
            return 8
        elif price >= 250000:
            return 6
        elif price >= 150000:
            return 4
        else:
            return 2
    
    def calculate_age_score(self, year_built: Optional[int]) -> int:
        """Calculate score based on property age (older = higher roof replacement likelihood)"""
        if not year_built:
            return 5
        
        current_year = datetime.now().year
        age = current_year - year_built
        
        if age >= 25:
            return 10  # Very likely to need roof replacement
        elif age >= 20:
            return 9
        elif age >= 15:
            return 8
        elif age >= 10:
            return 6
        elif age >= 5:
            return 4
        else:
            return 2  # New roof, unlikely to need work
    
    def calculate_location_score(self, zip_code: str, city: str) -> int:
        """Calculate score based on location desirability"""
        # ZIP code priority
        zip_score = self.priority_zip_codes.get(zip_code, 5)
        
        # City-based adjustments
        city_multipliers = {
            "Plano": 1.2,
            "Frisco": 1.2,
            "Dallas": 1.1,
            "Fort Worth": 1.0,
            "Irving": 1.0,
            "Richardson": 1.1,
            "Allen": 1.1,
            "McKinney": 1.1
        }
        
        multiplier = city_multipliers.get(city, 0.9)
        return min(int(zip_score * multiplier), 10)
    
    def calculate_source_score(self, source: str, lead_data: Dict[str, Any]) -> int:
        """Calculate score based on lead source"""
        source_scores = {
            "permit": 10,  # Active construction = highest priority
            "storm": 9,    # Storm damage = urgent need
            "cad": 7,      # Property owner data = good quality
            "redfin": 6,   # Real estate listings = medium quality
            "zillow": 6    # Real estate listings = medium quality
        }
        
        base_score = source_scores.get(source, 5)
        
        # Boost for specific permit types
        if source == "permit":
            permit_type = lead_data.get("permit_type", "").lower()
            if any(keyword in permit_type for keyword in ["roof", "roofing", "re-roof"]):
                base_score = 10
            elif any(keyword in permit_type for keyword in ["repair", "replacement", "damage"]):
                base_score = 9
        
        return base_score
    
    def calculate_storm_score(self, zip_code: str, storm_affected: bool) -> int:
        """Calculate score based on storm activity"""
        if storm_affected or zip_code in self.storm_affected_areas:
            return 8
        return 0
    
    def calculate_permit_score(self, lead_data: Dict[str, Any]) -> int:
        """Calculate score for permit-specific factors"""
        if lead_data.get("source") != "permit":
            return 0
        
        score = 0
        
        # Recent permit filing
        date_filed = lead_data.get("date_filed")
        if date_filed:
            try:
                filed_date = datetime.strptime(date_filed, "%m/%d/%Y")
                days_ago = (datetime.now() - filed_date).days
                
                if days_ago <= 7:
                    score += 5
                elif days_ago <= 30:
                    score += 3
                elif days_ago <= 90:
                    score += 1
            except:
                pass
        
        # Permit value
        permit_value = lead_data.get("permit_value", "")
        if permit_value:
            value_str = re.sub(r'[^\d]', '', permit_value)
            if value_str.isdigit():
                value = int(value_str)
                if value >= 25000:
                    score += 4
                elif value >= 15000:
                    score += 3
                elif value >= 10000:
                    score += 2
                elif value >= 5000:
                    score += 1
        
        return min(score, 10)
    
    def calculate_urgency_score(self, lead_data: Dict[str, Any]) -> int:
        """Calculate urgency based on various factors"""
        score = 0
        
        # How recent is the lead
        created_at = lead_data.get("created_at")
        if created_at:
            try:
                created_date = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                hours_ago = (datetime.now() - created_date.replace(tzinfo=None)).total_seconds() / 3600
                
                if hours_ago <= 1:
                    score += 5
                elif hours_ago <= 6:
                    score += 4
                elif hours_ago <= 24:
                    score += 3
                elif hours_ago <= 72:
                    score += 2
                elif hours_ago <= 168:  # 1 week
                    score += 1
            except:
                pass
        
        # Competitive factors
        source = lead_data.get("source", "")
        if source in ["zillow", "redfin"]:
            score += 2  # Real estate leads need quick action
        
        return min(score, 10)
    
    def calculate_lead_score(self, lead_data: Dict[str, Any]) -> LeadScore:
        """Calculate comprehensive lead score"""
        # Extract key fields
        price = lead_data.get("price", 0) or lead_data.get("appraised_value", 0) or 0
        year_built = lead_data.get("year_built")
        zip_code = lead_data.get("zip_code", "")
        city = lead_data.get("city", "")
        source = lead_data.get("source") or lead_data.get("source_type", "")
        storm_affected = lead_data.get("storm_affected", False)
        
        # Calculate component scores
        base_score = lead_data.get("lead_score", 5) or lead_data.get("lead_priority", 5) or 5
        property_value_score = self.calculate_property_value_score(price)
        age_score = self.calculate_age_score(year_built)
        location_score = self.calculate_location_score(zip_code, city)
        source_score = self.calculate_source_score(source, lead_data)
        storm_score = self.calculate_storm_score(zip_code, storm_affected)
        permit_score = self.calculate_permit_score(lead_data)
        urgency_score = self.calculate_urgency_score(lead_data)
        
        # Calculate weighted total score
        total_score = min(int(
            base_score * 0.1 +
            property_value_score * 0.2 +
            age_score * 0.15 +
            location_score * 0.15 +
            source_score * 0.15 +
            storm_score * 0.1 +
            permit_score * 0.1 +
            urgency_score * 0.05
        ), 10)
        
        # Determine priority and status
        if total_score >= 9:
            priority = Priority.URGENT
            status = LeadStatus.HOT
        elif total_score >= 7:
            priority = Priority.HIGH
            status = LeadStatus.WARM
        elif total_score >= 5:
            priority = Priority.MEDIUM
            status = LeadStatus.WARM
        else:
            priority = Priority.LOW
            status = LeadStatus.COLD
        
        # Generate routing tags
        tags = []
        if storm_affected or storm_score > 0:
            tags.append("storm-affected")
        if property_value_score >= 8:
            tags.append("high-value")
        if source == "permit":
            tags.append("active-construction")
        if age_score >= 8:
            tags.append("aging-roof")
        if location_score >= 8:
            tags.append("target-area")
        if urgency_score >= 4:
            tags.append("time-sensitive")
        
        # Determine next action
        if priority == Priority.URGENT:
            next_action = "immediate_call"
        elif priority == Priority.HIGH:
            next_action = "call_within_24h"
        elif priority == Priority.MEDIUM:
            next_action = "call_within_48h"
        else:
            next_action = "add_to_drip_campaign"
        
        return LeadScore(
            base_score=base_score,
            property_value_score=property_value_score,
            age_score=age_score,
            location_score=location_score,
            source_score=source_score,
            storm_score=storm_score,
            permit_score=permit_score,
            urgency_score=urgency_score,
            total_score=total_score,
            priority=priority,
            status=status,
            routing_tags=tags,
            next_action=next_action
        )
    
    def apply_routing_rules(self, lead_data: Dict[str, Any], lead_score: LeadScore) -> LeadScore:
        """Apply routing rules to modify lead scoring"""
        
        for rule in sorted(self.routing_rules, key=lambda x: x.priority):
            if not rule.enabled:
                continue
            
            # Check if lead matches rule conditions
            if self.matches_conditions(lead_data, lead_score, rule.conditions):
                logger.debug(f"Applying rule: {rule.name}")
                
                # Apply rule actions
                actions = rule.actions
                
                if "status" in actions:
                    lead_score.status = actions["status"]
                
                if "priority" in actions:
                    lead_score.priority = actions["priority"]
                
                if "tags" in actions:
                    lead_score.routing_tags.extend(actions["tags"])
                    lead_score.routing_tags = list(set(lead_score.routing_tags))  # Remove duplicates
                
                if "next_action" in actions:
                    lead_score.next_action = actions["next_action"]
                
                if "assigned_to" in actions:
                    lead_score.assigned_to = actions["assigned_to"]
                
                # Stop at first matching rule (highest priority)
                break
        
        return lead_score
    
    def matches_conditions(self, lead_data: Dict[str, Any], lead_score: LeadScore, conditions: Dict[str, Any]) -> bool:
        """Check if lead matches rule conditions"""
        for condition, value in conditions.items():
            
            if condition == "storm_affected":
                if lead_data.get("storm_affected", False) != value:
                    return False
            
            elif condition == "min_property_value":
                price = lead_data.get("price", 0) or lead_data.get("appraised_value", 0)
                if price < value:
                    return False
            
            elif condition == "max_days_old":
                created_at = lead_data.get("created_at")
                if created_at:
                    try:
                        created_date = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                        days_old = (datetime.now() - created_date.replace(tzinfo=None)).days
                        if days_old > value:
                            return False
                    except:
                        return False
            
            elif condition == "source":
                source = lead_data.get("source") or lead_data.get("source_type", "")
                if source != value:
                    return False
            
            elif condition == "permit_type_contains":
                permit_type = lead_data.get("permit_type", "").lower()
                if not any(keyword in permit_type for keyword in value):
                    return False
            
            elif condition == "zip_codes":
                zip_code = lead_data.get("zip_code", "")
                if zip_code not in value:
                    return False
            
            elif condition == "max_year_built":
                year_built = lead_data.get("year_built")
                if not year_built or year_built > value:
                    return False
            
            elif condition == "property_types":
                property_type = lead_data.get("property_type", "")
                if property_type not in value:
                    return False
        
        return True
    
    def update_lead_status(self, table_name: str, lead_id: int, lead_score: LeadScore) -> bool:
        """Update lead status in database"""
        if not self.supabase_conn.supabase:
            return False
        
        try:
            update_data = {
                "lead_status": lead_score.status.value,
                "priority": lead_score.priority.value,
                "routing_tags": ",".join(lead_score.routing_tags),
                "next_action": lead_score.next_action,
                "assigned_to": lead_score.assigned_to,
                "score_breakdown": json.dumps(asdict(lead_score)),
                "last_routed_at": datetime.now().isoformat()
            }
            
            self.supabase_conn.supabase.table(table_name)\
                .update(update_data)\
                .eq("id", lead_id)\
                .execute()
            
            return True
            
        except Exception as e:
            logger.error(f"Error updating lead status: {e}")
            return False
    
    def process_leads(self, source: Optional[str] = None, days_back: int = 1) -> Dict[str, Any]:
        """Process and route leads"""
        if not self.supabase_conn.supabase:
            logger.error("Supabase not available")
            return {}
        
        cutoff_date = (datetime.now() - timedelta(days=days_back)).isoformat()
        
        # Tables to process
        tables = ['zillow_leads', 'redfin_leads', 'cad_leads', 'permit_leads']
        
        if source:
            source_table = f"{source}_leads"
            if source_table in tables:
                tables = [source_table]
        
        results = {
            'processed': 0,
            'by_status': {},
            'by_priority': {},
            'by_source': {}
        }
        
        for table in tables:
            try:
                # Get unprocessed leads
                result = self.supabase_conn.supabase.table(table)\
                    .select("*")\
                    .gte('created_at', cutoff_date)\
                    .is_('lead_status', 'null')\
                    .execute()
                
                if not result.data:
                    continue
                
                source_name = table.replace('_leads', '')
                logger.info(f"Processing {len(result.data)} leads from {source_name}")
                
                for lead in result.data:
                    # Add source information
                    lead['source'] = source_name
                    
                    # Calculate lead score
                    lead_score = self.calculate_lead_score(lead)
                    
                    # Apply routing rules
                    lead_score = self.apply_routing_rules(lead, lead_score)
                    
                    # Update database
                    success = self.update_lead_status(table, lead['id'], lead_score)
                    
                    if success:
                        results['processed'] += 1
                        
                        # Track statistics
                        status = lead_score.status.value
                        priority = lead_score.priority.value
                        
                        results['by_status'][status] = results['by_status'].get(status, 0) + 1
                        results['by_priority'][priority] = results['by_priority'].get(priority, 0) + 1
                        results['by_source'][source_name] = results['by_source'].get(source_name, 0) + 1
                        
                        logger.debug(f"Routed {source_name} lead {lead['id']}: {status}/{priority}")
                
            except Exception as e:
                logger.error(f"Error processing {table}: {e}")
        
        return results
    
    def get_routing_summary(self) -> Dict[str, Any]:
        """Get summary of current lead routing status"""
        if not self.supabase_conn.supabase:
            return {}
        
        tables = ['zillow_leads', 'redfin_leads', 'cad_leads', 'permit_leads']
        summary = {
            'total_leads': 0,
            'routed_leads': 0,
            'unrouted_leads': 0,
            'by_status': {},
            'by_priority': {},
            'by_source': {}
        }
        
        for table in tables:
            try:
                # Get all leads
                all_result = self.supabase_conn.supabase.table(table)\
                    .select("id, lead_status, priority")\
                    .execute()
                
                if not all_result.data:
                    continue
                
                source_name = table.replace('_leads', '')
                source_count = len(all_result.data)
                
                summary['total_leads'] += source_count
                summary['by_source'][source_name] = source_count
                
                # Count routed vs unrouted
                routed = [lead for lead in all_result.data if lead.get('lead_status')]
                unrouted = [lead for lead in all_result.data if not lead.get('lead_status')]
                
                summary['routed_leads'] += len(routed)
                summary['unrouted_leads'] += len(unrouted)
                
                # Status breakdown
                for lead in routed:
                    status = lead.get('lead_status', 'unknown')
                    priority = lead.get('priority', 'unknown')
                    
                    summary['by_status'][status] = summary['by_status'].get(status, 0) + 1
                    summary['by_priority'][priority] = summary['by_priority'].get(priority, 0) + 1
                
            except Exception as e:
                logger.error(f"Error getting summary for {table}: {e}")
        
        return summary

def main():
    """Main CLI interface"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Smart lead routing and prioritization')
    parser.add_argument('--source', choices=['zillow', 'redfin', 'cad', 'permit'], 
                       help='Process leads from specific source only')
    parser.add_argument('--days', type=int, default=1, 
                       help='Number of days back to process (default: 1)')
    parser.add_argument('--summary', action='store_true', 
                       help='Show routing summary')
    
    args = parser.parse_args()
    
    router = LeadRouter()
    
    if args.summary:
        summary = router.get_routing_summary()
        
        print("\n🧠 LEAD ROUTING SUMMARY")
        print("=" * 40)
        print(f"Total Leads: {summary['total_leads']}")
        print(f"Routed: {summary['routed_leads']}")
        print(f"Unrouted: {summary['unrouted_leads']}")
        
        if summary['by_status']:
            print("\nBy Status:")
            for status, count in summary['by_status'].items():
                print(f"  {status}: {count}")
        
        if summary['by_priority']:
            print("\nBy Priority:")
            for priority, count in summary['by_priority'].items():
                print(f"  {priority}: {count}")
        
        if summary['by_source']:
            print("\nBy Source:")
            for source, count in summary['by_source'].items():
                print(f"  {source}: {count}")
        
        return 0
    
    # Process leads
    logger.info("🧠 Starting smart lead routing...")
    results = router.process_leads(args.source, args.days)
    
    print(f"\n✅ ROUTING COMPLETED")
    print(f"Processed: {results['processed']} leads")
    
    if results['by_status']:
        print("\nBy Status:")
        for status, count in results['by_status'].items():
            print(f"  {status}: {count}")
    
    if results['by_priority']:
        print("\nBy Priority:")
        for priority, count in results['by_priority'].items():
            print(f"  {priority}: {count}")
    
    return 0

if __name__ == "__main__":
    import sys
    exit_code = main()
    sys.exit(exit_code)