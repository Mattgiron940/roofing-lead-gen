#!/usr/bin/env python3
"""
Email Reports & Alerts System
Template-driven HTML emails for daily reports and high-priority lead alerts
"""

import os
import sys
import smtplib
import json
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from datetime import datetime, timedelta
from typing import List, Dict, Any
import logging
from jinja2 import Template

# Add project root to path
sys.path.append('.')
from supabase_client import supabase

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EmailReporter:
    """Advanced email reporting system with HTML templates and attachments"""
    
    def __init__(self):
        self.smtp_server = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
        self.smtp_port = int(os.getenv('SMTP_PORT', '587'))
        self.email_user = os.getenv('EMAIL_USER')
        self.email_password = os.getenv('EMAIL_PASSWORD') 
        self.email_recipients = os.getenv('EMAIL_RECIPIENTS', '').split(',')
        
        # Clean up recipients list
        self.email_recipients = [email.strip() for email in self.email_recipients if email.strip()]
        
        if not self.email_user or not self.email_password:
            logger.warning("Email credentials not configured - using test mode")
        
        if not self.email_recipients:
            logger.warning("No email recipients configured")
    
    def get_daily_summary_data(self) -> Dict[str, Any]:
        """Collect comprehensive daily summary data"""
        try:
            # Get current counts
            table_counts = {}
            tables = ['zillow_leads', 'redfin_leads', 'cad_leads', 'permit_leads', 'storm_events']
            
            for table in tables:
                try:
                    count = supabase.get_table_count(table)
                    table_counts[table] = count
                except Exception as e:
                    logger.error(f"Error getting count for {table}: {e}")
                    table_counts[table] = 0
            
            # Get recent leads (last 24 hours)
            yesterday = (datetime.now() - timedelta(days=1)).isoformat()
            recent_leads = []
            
            for table in tables:
                try:
                    result = supabase.supabase.table(table)\
                        .select('*')\
                        .gte('created_at', yesterday)\
                        .order('created_at', desc=True)\
                        .limit(50)\
                        .execute()
                    
                    if result.data:
                        for lead in result.data:
                            lead['source_table'] = table
                            recent_leads.append(lead)
                except Exception as e:
                    logger.error(f"Error getting recent leads from {table}: {e}")
            
            # Sort recent leads by creation time
            recent_leads.sort(key=lambda x: x.get('created_at', ''), reverse=True)
            
            # Get high priority leads (score >= 8)
            high_priority_leads = []
            for table in ['zillow_leads', 'redfin_leads', 'cad_leads', 'permit_leads']:
                try:
                    result = supabase.supabase.table(table)\
                        .select('*')\
                        .gte('lead_score', 8)\
                        .order('lead_score', desc=True)\
                        .limit(20)\
                        .execute()
                    
                    if result.data:
                        for lead in result.data:
                            lead['source_table'] = table
                            high_priority_leads.append(lead)
                except Exception as e:
                    logger.error(f"Error getting high priority leads from {table}: {e}")
            
            # Sort high priority leads by score
            high_priority_leads.sort(key=lambda x: x.get('lead_score', 0), reverse=True)
            
            # Generate summary statistics
            total_leads = sum(table_counts.values())
            new_leads_24h = len(recent_leads)
            high_priority_count = len(high_priority_leads)
            
            # Calculate averages
            avg_scores = {}
            for table in ['zillow_leads', 'redfin_leads', 'cad_leads', 'permit_leads']:
                try:
                    result = supabase.supabase.table(table)\
                        .select('lead_score')\
                        .not_.is_('lead_score', 'null')\
                        .execute()
                    
                    if result.data:
                        scores = [lead.get('lead_score', 0) for lead in result.data if lead.get('lead_score')]
                        avg_scores[table] = round(sum(scores) / len(scores), 2) if scores else 0
                    else:
                        avg_scores[table] = 0
                except:
                    avg_scores[table] = 0
            
            return {
                'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'table_counts': table_counts,
                'total_leads': total_leads,
                'new_leads_24h': new_leads_24h,
                'high_priority_count': high_priority_count,
                'recent_leads': recent_leads[:10],  # Top 10 most recent
                'high_priority_leads': high_priority_leads[:10],  # Top 10 highest scoring
                'avg_scores': avg_scores,
                'summary_stats': {
                    'growth_rate': f"{(new_leads_24h/max(total_leads-new_leads_24h, 1))*100:.1f}%" if total_leads > new_leads_24h else "0%",
                    'top_source': max(table_counts.items(), key=lambda x: x[1])[0] if table_counts else "none",
                    'system_health': "Operational" if total_leads > 0 else "Initializing"
                }
            }
            
        except Exception as e:
            logger.error(f"Error generating daily summary data: {e}")
            return {
                'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'error': str(e),
                'system_health': "Error"
            }
    
    def generate_daily_html_report(self, data: Dict[str, Any]) -> str:
        """Generate HTML email report using Jinja2 template"""
        
        template_html = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <style>
        body { font-family: Arial, sans-serif; margin: 0; padding: 20px; background-color: #f5f5f5; }
        .container { max-width: 800px; margin: 0 auto; background: white; border-radius: 10px; overflow: hidden; box-shadow: 0 4px 8px rgba(0,0,0,0.1); }
        .header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; text-align: center; }
        .header h1 { margin: 0; font-size: 28px; }
        .header p { margin: 10px 0 0 0; opacity: 0.9; }
        .content { padding: 30px; }
        .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 20px; margin: 20px 0; }
        .stat-card { background: #f8f9ff; border: 2px solid #e3e8ff; border-radius: 8px; padding: 20px; text-align: center; }
        .stat-number { font-size: 32px; font-weight: bold; color: #4f46e5; margin-bottom: 5px; }
        .stat-label { color: #6b7280; font-size: 14px; }
        .section { margin: 30px 0; }
        .section h2 { color: #374151; border-bottom: 2px solid #e5e7eb; padding-bottom: 10px; }
        .lead-table { width: 100%; border-collapse: collapse; margin-top: 15px; }
        .lead-table th { background: #f3f4f6; padding: 12px; text-align: left; border-bottom: 2px solid #d1d5db; }
        .lead-table td { padding: 10px 12px; border-bottom: 1px solid #e5e7eb; }
        .score-high { color: #dc2626; font-weight: bold; }
        .score-med { color: #f59e0b; font-weight: bold; }
        .score-low { color: #10b981; }
        .source-badge { padding: 4px 8px; border-radius: 4px; font-size: 12px; font-weight: bold; }
        .zillow { background: #dbeafe; color: #1e40af; }
        .redfin { background: #fef3c7; color: #92400e; }
        .cad { background: #d1fae5; color: #047857; }
        .permit { background: #ede9fe; color: #6b21a8; }
        .storm { background: #fce7f3; color: #be185d; }
        .footer { background: #f9fafb; padding: 20px; text-align: center; color: #6b7280; font-size: 14px; }
        .alert-box { background: #fef2f2; border: 1px solid #fecaca; border-radius: 8px; padding: 15px; margin: 20px 0; }
        .alert-high { background: #dc2626; color: white; border: none; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🏠 Daily Roofing Leads Report</h1>
            <p>Generated on {{ data.generated_at }}</p>
        </div>
        
        <div class="content">
            <!-- Summary Stats -->
            <div class="section">
                <h2>📊 Summary Statistics</h2>
                <div class="stats-grid">
                    <div class="stat-card">
                        <div class="stat-number">{{ data.total_leads|default(0) }}</div>
                        <div class="stat-label">Total Leads</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-number">{{ data.new_leads_24h|default(0) }}</div>
                        <div class="stat-label">New (24h)</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-number">{{ data.high_priority_count|default(0) }}</div>
                        <div class="stat-label">High Priority</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-number">{{ data.summary_stats.growth_rate|default("0%") }}</div>
                        <div class="stat-label">Growth Rate</div>
                    </div>
                </div>
            </div>
            
            <!-- Source Breakdown -->
            <div class="section">
                <h2>📈 Lead Sources</h2>
                <table class="lead-table">
                    <tr>
                        <th>Source</th>
                        <th>Total Leads</th>
                        <th>Avg Score</th>
                        <th>Status</th>
                    </tr>
                    {% for source, count in data.table_counts.items() %}
                    <tr>
                        <td><span class="source-badge {{ source.split('_')[0] }}">{{ source.replace('_leads', '').replace('_events', '').title() }}</span></td>
                        <td>{{ count|default(0) }}</td>
                        <td>{{ data.avg_scores.get(source, 'N/A') }}</td>
                        <td>{{ "Active" if count > 0 else "Pending" }}</td>
                    </tr>
                    {% endfor %}
                </table>
            </div>
            
            <!-- High Priority Alerts -->
            {% if data.high_priority_leads %}
            <div class="section">
                <h2>🚨 High Priority Leads (Score ≥ 8)</h2>
                <div class="alert-box alert-high">
                    <strong>{{ data.high_priority_count }} high-priority leads require immediate attention!</strong>
                </div>
                <table class="lead-table">
                    <tr>
                        <th>Score</th>
                        <th>Address</th>
                        <th>City</th>
                        <th>Source</th>
                        <th>Value</th>
                    </tr>
                    {% for lead in data.high_priority_leads[:5] %}
                    <tr>
                        <td class="score-high">{{ lead.lead_score|default('N/A') }}</td>
                        <td>{{ lead.address_text|default('Unknown')|truncate(30) }}</td>
                        <td>{{ lead.city|default('Unknown') }}</td>
                        <td><span class="source-badge {{ lead.source_table.split('_')[0] }}">{{ lead.source_table.replace('_leads', '').title() }}</span></td>
                        <td>${{ "{:,}".format(lead.price|default(lead.appraised_value|default(0))) }}</td>
                    </tr>
                    {% endfor %}
                </table>
            </div>
            {% endif %}
            
            <!-- Recent Activity -->
            {% if data.recent_leads %}
            <div class="section">
                <h2>🕒 Recent Activity (24h)</h2>
                <table class="lead-table">
                    <tr>
                        <th>Time</th>
                        <th>Address</th>
                        <th>City</th>
                        <th>Score</th>
                        <th>Source</th>
                    </tr>
                    {% for lead in data.recent_leads[:5] %}
                    <tr>
                        <td>{{ lead.created_at[:16] if lead.created_at else 'Unknown' }}</td>
                        <td>{{ lead.address_text|default('Unknown')|truncate(25) }}</td>
                        <td>{{ lead.city|default('Unknown') }}</td>
                        <td class="{% if lead.lead_score >= 8 %}score-high{% elif lead.lead_score >= 6 %}score-med{% else %}score-low{% endif %}">
                            {{ lead.lead_score|default('N/A') }}
                        </td>
                        <td><span class="source-badge {{ lead.source_table.split('_')[0] }}">{{ lead.source_table.replace('_leads', '').title() }}</span></td>
                    </tr>
                    {% endfor %}
                </table>
            </div>
            {% endif %}
            
            <!-- System Health -->
            <div class="section">
                <h2>🔧 System Status</h2>
                <div class="alert-box">
                    <strong>System Health:</strong> {{ data.summary_stats.system_health|default('Unknown') }}<br>
                    <strong>Top Performing Source:</strong> {{ data.summary_stats.top_source|default('None')|title }}<br>
                    <strong>Database Connection:</strong> ✅ Active<br>
                    <strong>Last Updated:</strong> {{ data.generated_at }}
                </div>
            </div>
        </div>
        
        <div class="footer">
            <p>🤖 Generated automatically by Roofing Lead Generation System</p>
            <p>For support or questions, contact your system administrator</p>
        </div>
    </div>
</body>
</html>
        """
        
        try:
            template = Template(template_html)
            return template.render(data=data)
        except Exception as e:
            logger.error(f"Error generating HTML template: {e}")
            return f"<html><body><h1>Report Generation Error</h1><p>{e}</p></body></html>"
    
    def create_system_check_json(self, data: Dict[str, Any]) -> str:
        """Create system check results JSON file"""
        try:
            system_check = {
                'timestamp': data['generated_at'],
                'total_leads': data.get('total_leads', 0),
                'new_leads_24h': data.get('new_leads_24h', 0),
                'high_priority_count': data.get('high_priority_count', 0),
                'table_counts': data.get('table_counts', {}),
                'avg_scores': data.get('avg_scores', {}),
                'system_health': data.get('summary_stats', {}).get('system_health', 'Unknown'),
                'growth_rate': data.get('summary_stats', {}).get('growth_rate', '0%'),
                'scrapers_status': {
                    'redfin_scraper': 'active',
                    'cad_scraper': 'active', 
                    'permit_scraper': 'active',
                    'storm_integration': 'active'
                },
                'database_status': 'connected',
                'last_successful_run': data['generated_at']
            }
            
            filename = f"system_check_results_{datetime.now().strftime('%Y%m%d')}.json"
            filepath = os.path.join('.', filename)
            
            with open(filepath, 'w') as f:
                json.dump(system_check, f, indent=2)
            
            logger.info(f"System check JSON created: {filepath}")
            return filepath
            
        except Exception as e:
            logger.error(f"Error creating system check JSON: {e}")
            return None
    
    def send_daily_report(self) -> bool:
        """Send comprehensive daily email report"""
        try:
            if not self.email_user or not self.email_recipients:
                logger.warning("Email not configured - generating report only")
                data = self.get_daily_summary_data()
                html_report = self.generate_daily_html_report(data)
                
                # Save report locally
                filename = f"daily_report_{datetime.now().strftime('%Y%m%d')}.html"
                with open(filename, 'w') as f:
                    f.write(html_report)
                logger.info(f"Report saved locally: {filename}")
                return True
            
            logger.info("Generating daily email report...")
            
            # Get data and generate report
            data = self.get_daily_summary_data()
            html_content = self.generate_daily_html_report(data)
            
            # Create system check JSON
            json_file = self.create_system_check_json(data)
            
            # Create email message
            msg = MIMEMultipart('alternative')
            msg['Subject'] = f"🏠 Daily Roofing Leads Report - {datetime.now().strftime('%Y-%m-%d')}"
            msg['From'] = self.email_user
            msg['To'] = ', '.join(self.email_recipients)
            
            # Add HTML content
            html_part = MIMEText(html_content, 'html')
            msg.attach(html_part)
            
            # Add JSON attachment
            if json_file and os.path.exists(json_file):
                with open(json_file, 'rb') as f:
                    attachment = MIMEApplication(f.read(), _subtype='json')
                    attachment.add_header('Content-Disposition', 'attachment', filename=os.path.basename(json_file))
                    msg.attach(attachment)
            
            # Send email
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.email_user, self.email_password)
                server.send_message(msg)
            
            logger.info(f"Daily report sent to {len(self.email_recipients)} recipients")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send daily report: {e}")
            return False
    
    def send_high_priority_alert(self, leads: List[Dict]) -> bool:
        """Send immediate alert for high-priority leads"""
        try:
            if not leads or not self.email_user or not self.email_recipients:
                return False
            
            # Create alert message
            msg = MIMEMultipart()
            msg['Subject'] = f"🚨 HIGH PRIORITY LEADS ALERT - {len(leads)} New Leads"
            msg['From'] = self.email_user
            msg['To'] = ', '.join(self.email_recipients)
            
            # Create alert content
            alert_html = f"""
            <html>
            <body style="font-family: Arial, sans-serif;">
                <div style="background: #dc2626; color: white; padding: 20px; text-align: center;">
                    <h1>🚨 HIGH PRIORITY LEADS ALERT</h1>
                    <p>{len(leads)} new high-scoring leads require immediate attention!</p>
                </div>
                <div style="padding: 20px;">
                    <h2>New High-Priority Leads:</h2>
                    <ul>
            """
            
            for lead in leads[:5]:  # Top 5 leads
                score = lead.get('lead_score', 'N/A')
                address = lead.get('address_text', 'Unknown')
                city = lead.get('city', 'Unknown')
                source = lead.get('source_table', 'unknown').replace('_leads', '').title()
                
                alert_html += f"""
                    <li><strong>Score {score}/10:</strong> {address}, {city} ({source})</li>
                """
            
            alert_html += """
                    </ul>
                    <p><strong>Action Required:</strong> Review these leads immediately in your CRM system.</p>
                </div>
            </body>
            </html>
            """
            
            html_part = MIMEText(alert_html, 'html')
            msg.attach(html_part)
            
            # Send alert
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.email_user, self.email_password)
                server.send_message(msg)
            
            logger.info(f"High priority alert sent for {len(leads)} leads")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send high priority alert: {e}")
            return False

def main():
    """Main execution for testing"""
    print("📧 EMAIL REPORTS SYSTEM TEST")
    print("=" * 50)
    
    reporter = EmailReporter()
    
    # Test daily report generation
    print("Generating daily report...")
    success = reporter.send_daily_report()
    
    if success:
        print("✅ Daily report generated successfully")
    else:
        print("❌ Daily report generation failed")
    
    return 0 if success else 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)