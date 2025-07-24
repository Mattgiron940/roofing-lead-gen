#!/usr/bin/env python3
"""
Lead Export & Monitoring System
Daily automated exports with multi-format support and email notifications
"""

import os
import sys
import csv
import json
import smtplib
import argparse
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from typing import List, Dict, Any, Optional
import logging

# Google Sheets integration
try:
    import gspread
    from google.oauth2.service_account import Credentials
    SHEETS_AVAILABLE = True
except ImportError:
    SHEETS_AVAILABLE = False

from supabase_config import SupabaseConnection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class LeadExporter:
    def __init__(self):
        self.supabase_conn = SupabaseConnection()
        self.sheets_client = None
        self.setup_sheets_client()
        
        # Email configuration
        self.email_config = {
            'smtp_server': os.getenv('SMTP_SERVER', 'smtp.gmail.com'),
            'smtp_port': int(os.getenv('SMTP_PORT', '587')),
            'email_user': os.getenv('EMAIL_USER'),
            'email_password': os.getenv('EMAIL_PASSWORD'),
            'email_recipients': os.getenv('EMAIL_RECIPIENTS', '').split(',') if os.getenv('EMAIL_RECIPIENTS') else []
        }
    
    def setup_sheets_client(self):
        """Setup Google Sheets client"""
        if not SHEETS_AVAILABLE:
            logger.warning("Google Sheets not available")
            return
        
        try:
            creds_file = os.getenv('GOOGLE_SHEETS_CREDENTIALS', 'service_account.json')
            if os.path.exists(creds_file):
                scopes = [
                    'https://www.googleapis.com/auth/spreadsheets',
                    'https://www.googleapis.com/auth/drive'
                ]
                creds = Credentials.from_service_account_file(creds_file, scopes=scopes)
                self.sheets_client = gspread.authorize(creds)
                logger.info("✅ Google Sheets client initialized")
        except Exception as e:
            logger.error(f"Failed to setup Google Sheets: {e}")
    
    def get_new_leads(self, source: Optional[str] = None, days_back: int = 1) -> Dict[str, List[Dict]]:
        """Get new leads from the last N days"""
        if not self.supabase_conn.supabase:
            logger.error("Supabase not available")
            return {}
        
        cutoff_date = (datetime.now() - timedelta(days=days_back)).isoformat()
        
        # Define tables to query
        tables = {
            'zillow_leads': 'zillow',
            'redfin_leads': 'redfin', 
            'cad_leads': 'cad',
            'permit_leads': 'permit',
            'storm_events': 'storm'
        }
        
        # Filter by source if specified
        if source:
            source_table = f"{source}_leads" if source != 'storm' else 'storm_events'
            if source_table in tables:
                tables = {source_table: source}
            else:
                logger.error(f"Unknown source: {source}")
                return {}
        
        new_leads = {}
        total_count = 0
        
        for table_name, source_name in tables.items():
            try:
                result = self.supabase_conn.supabase.table(table_name)\
                    .select("*")\
                    .gte('created_at', cutoff_date)\
                    .order('created_at', desc=True)\
                    .execute()
                
                leads = result.data if result.data else []
                new_leads[source_name] = leads
                total_count += len(leads)
                
                logger.info(f"📊 {source_name}: {len(leads)} new leads")
                
            except Exception as e:
                logger.error(f"Error fetching {source_name} leads: {e}")
                new_leads[source_name] = []
        
        logger.info(f"✅ Total new leads: {total_count}")
        return new_leads
    
    def export_to_csv(self, leads_data: Dict[str, List[Dict]], output_dir: str = "exports") -> List[str]:
        """Export leads to CSV files"""
        os.makedirs(output_dir, exist_ok=True)
        exported_files = []
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        for source, leads in leads_data.items():
            if not leads:
                continue
            
            filename = f"{output_dir}/leads_{source}_{timestamp}.csv"
            
            try:
                with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                    if leads:
                        fieldnames = list(leads[0].keys())
                        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                        writer.writeheader()
                        writer.writerows(leads)
                
                exported_files.append(filename)
                logger.info(f"📄 Exported {len(leads)} {source} leads to {filename}")
                
            except Exception as e:
                logger.error(f"Error exporting {source} to CSV: {e}")
        
        return exported_files
    
    def export_to_sheets(self, leads_data: Dict[str, List[Dict]], spreadsheet_name: Optional[str] = None) -> Optional[str]:
        """Export leads to Google Sheets"""
        if not self.sheets_client:
            logger.warning("Google Sheets not configured")
            return None
        
        if not spreadsheet_name:
            spreadsheet_name = f"Roofing Leads Export - {datetime.now().strftime('%Y-%m-%d')}"
        
        try:
            # Create or open spreadsheet
            try:
                spreadsheet = self.sheets_client.open(spreadsheet_name)
            except gspread.SpreadsheetNotFound:
                spreadsheet = self.sheets_client.create(spreadsheet_name)
                logger.info(f"Created new spreadsheet: {spreadsheet_name}")
            
            # Create worksheets for each source
            for source, leads in leads_data.items():
                if not leads:
                    continue
                
                worksheet_name = f"{source.title()} Leads"
                
                try:
                    worksheet = spreadsheet.worksheet(worksheet_name)
                    worksheet.clear()
                except gspread.WorksheetNotFound:
                    worksheet = spreadsheet.add_worksheet(worksheet_name, rows=1000, cols=20)
                
                # Prepare data
                if leads:
                    headers = list(leads[0].keys())
                    rows = [headers]
                    
                    for lead in leads:
                        row = [str(lead.get(header, '')) for header in headers]
                        rows.append(row)
                    
                    # Update worksheet
                    worksheet.update(rows)
                    logger.info(f"📊 Updated {worksheet_name} with {len(leads)} leads")
            
            # Create summary worksheet
            self.create_summary_worksheet(spreadsheet, leads_data)
            
            logger.info(f"✅ Export to Google Sheets completed")
            return spreadsheet.url
            
        except Exception as e:
            logger.error(f"Error exporting to Google Sheets: {e}")
            return None
    
    def create_summary_worksheet(self, spreadsheet, leads_data: Dict[str, List[Dict]]):
        """Create summary worksheet with lead statistics"""
        try:
            try:
                summary_sheet = spreadsheet.worksheet('Summary')
                summary_sheet.clear()
            except gspread.WorksheetNotFound:
                summary_sheet = spreadsheet.add_worksheet('Summary', rows=50, cols=10)
            
            # Calculate summary statistics
            total_leads = sum(len(leads) for leads in leads_data.values())
            
            summary_data = [
                ['Lead Export Summary', ''],
                ['Export Date', datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
                ['Total Leads', total_leads],
                ['', ''],
                ['Source Breakdown', 'Count']
            ]
            
            for source, leads in leads_data.items():
                summary_data.append([source.title(), len(leads)])
            
            # Add high-priority leads count
            high_priority_count = 0
            for leads in leads_data.values():
                for lead in leads:
                    lead_score = lead.get('lead_score', 0) or lead.get('lead_priority', 0)
                    if lead_score >= 8:
                        high_priority_count += 1
            
            summary_data.extend([
                ['', ''],
                ['High Priority Leads (8+)', high_priority_count],
                ['High Priority Percentage', f"{(high_priority_count/total_leads*100):.1f}%" if total_leads > 0 else "0%"]
            ])
            
            summary_sheet.update(summary_data)
            
        except Exception as e:
            logger.error(f"Error creating summary worksheet: {e}")
    
    def send_email_summary(self, leads_data: Dict[str, List[Dict]], exported_files: List[str], sheets_url: Optional[str] = None):
        """Send email summary of exported leads"""
        if not self.email_config['email_user'] or not self.email_config['email_recipients']:
            logger.warning("Email not configured")
            return
        
        try:
            # Create email content
            total_leads = sum(len(leads) for leads in leads_data.values())
            high_priority_count = 0
            
            for leads in leads_data.values():
                for lead in leads:
                    lead_score = lead.get('lead_score', 0) or lead.get('lead_priority', 0)
                    if lead_score >= 8:
                        high_priority_count += 1
            
            # Email body
            email_body = f"""
            <html>
            <body>
                <h2>🏠 Daily Roofing Leads Report</h2>
                <p><strong>Export Date:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                
                <h3>📊 Summary</h3>
                <ul>
                    <li><strong>Total New Leads:</strong> {total_leads}</li>
                    <li><strong>High Priority Leads:</strong> {high_priority_count}</li>
                    <li><strong>Priority Rate:</strong> {(high_priority_count/total_leads*100):.1f}%</li>
                </ul>
                
                <h3>📋 Source Breakdown</h3>
                <table border="1" style="border-collapse: collapse;">
                    <tr><th>Source</th><th>Count</th><th>Percentage</th></tr>
            """
            
            for source, leads in leads_data.items():
                count = len(leads)
                percentage = (count/total_leads*100) if total_leads > 0 else 0
                email_body += f"<tr><td>{source.title()}</td><td>{count}</td><td>{percentage:.1f}%</td></tr>"
            
            email_body += "</table>"
            
            if sheets_url:
                email_body += f'<p><strong>📊 Google Sheets:</strong> <a href="{sheets_url}">View Online</a></p>'
            
            if exported_files:
                email_body += f"<p><strong>📁 Exported Files:</strong> {len(exported_files)} CSV files attached</p>"
            
            email_body += """
                <p>This is an automated report from your roofing lead generation system.</p>
                </body>
                </html>
            """
            
            # Create email message
            msg = MIMEMultipart()
            msg['From'] = self.email_config['email_user']
            msg['To'] = ', '.join(self.email_config['email_recipients'])
            msg['Subject'] = f"Daily Roofing Leads Report - {total_leads} New Leads"
            
            msg.attach(MIMEText(email_body, 'html'))
            
            # Attach CSV files
            for file_path in exported_files:
                if os.path.exists(file_path):
                    with open(file_path, "rb") as attachment:
                        part = MIMEBase('application', 'octet-stream')
                        part.set_payload(attachment.read())
                    
                    encoders.encode_base64(part)
                    part.add_header(
                        'Content-Disposition',
                        f'attachment; filename= {os.path.basename(file_path)}'
                    )
                    msg.attach(part)
            
            # Send email
            server = smtplib.SMTP(self.email_config['smtp_server'], self.email_config['smtp_port'])
            server.starttls()
            server.login(self.email_config['email_user'], self.email_config['email_password'])
            text = msg.as_string()
            server.sendmail(self.email_config['email_user'], self.email_config['email_recipients'], text)
            server.quit()
            
            logger.info(f"✅ Email sent to {len(self.email_config['email_recipients'])} recipients")
            
        except Exception as e:
            logger.error(f"Error sending email: {e}")
    
    def run_daily_export(self, source: Optional[str] = None, days_back: int = 1, 
                        export_csv: bool = True, export_sheets: bool = True, 
                        send_email: bool = True) -> Dict[str, Any]:
        """Run complete daily export process"""
        logger.info("🚀 Starting daily lead export process")
        
        # Get new leads
        leads_data = self.get_new_leads(source, days_back)
        
        if not any(leads_data.values()):
            logger.info("ℹ️ No new leads found")
            return {'status': 'no_leads', 'total_leads': 0}
        
        total_leads = sum(len(leads) for leads in leads_data.values())
        
        results = {
            'status': 'success',
            'total_leads': total_leads,
            'source_breakdown': {k: len(v) for k, v in leads_data.items()},
            'exported_files': [],
            'sheets_url': None
        }
        
        # Export to CSV
        if export_csv:
            exported_files = self.export_to_csv(leads_data)
            results['exported_files'] = exported_files
        
        # Export to Google Sheets
        sheets_url = None
        if export_sheets and SHEETS_AVAILABLE:
            sheets_url = self.export_to_sheets(leads_data)
            results['sheets_url'] = sheets_url
        
        # Send email summary
        if send_email:
            self.send_email_summary(leads_data, results['exported_files'], sheets_url)
        
        logger.info(f"✅ Daily export completed: {total_leads} leads processed")
        return results

def main():
    """Main CLI interface"""
    parser = argparse.ArgumentParser(description='Export roofing leads from Supabase')
    
    parser.add_argument('--source', choices=['zillow', 'redfin', 'cad', 'permit', 'storm'], 
                       help='Export leads from specific source only')
    parser.add_argument('--days', type=int, default=1, 
                       help='Number of days back to export (default: 1)')
    parser.add_argument('--no-csv', action='store_true', 
                       help='Skip CSV export')
    parser.add_argument('--no-sheets', action='store_true', 
                       help='Skip Google Sheets export')
    parser.add_argument('--no-email', action='store_true', 
                       help='Skip email notification')
    parser.add_argument('--output-dir', default='exports', 
                       help='Directory for CSV exports (default: exports)')
    
    args = parser.parse_args()
    
    # Create exporter
    exporter = LeadExporter()
    
    # Run export
    results = exporter.run_daily_export(
        source=args.source,
        days_back=args.days,
        export_csv=not args.no_csv,
        export_sheets=not args.no_sheets and SHEETS_AVAILABLE,
        send_email=not args.no_email
    )
    
    # Print results
    print("\n📊 EXPORT SUMMARY:")
    print(f"Status: {results['status']}")
    print(f"Total Leads: {results['total_leads']}")
    
    if results['source_breakdown']:
        print("\nSource Breakdown:")
        for source, count in results['source_breakdown'].items():
            print(f"  • {source}: {count} leads")
    
    if results['exported_files']:
        print(f"\nExported Files: {len(results['exported_files'])}")
    
    if results['sheets_url']:
        print(f"Google Sheets: {results['sheets_url']}")
    
    return 0 if results['status'] == 'success' else 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)