#!/usr/bin/env python3
"""
Automation Scripts for Roofing Lead Generation System
GitHub Actions, cron jobs, and automated workflows
"""

import os
import sys
import yaml
import json
from datetime import datetime
from typing import Dict, List, Any

def generate_github_actions_workflow() -> str:
    """Generate GitHub Actions workflow for automated scraping"""
    
    workflow = {
        'name': 'Roofing Lead Generation',
        'on': {
            'schedule': [
                {'cron': '0 6 * * *'},  # Daily at 6 AM
                {'cron': '0 18 * * *'}  # Daily at 6 PM
            ],
            'workflow_dispatch': {},
            'push': {
                'branches': ['main'],
                'paths': ['scrapers/**', '*.py']
            }
        },
        'jobs': {
            'scrape-leads': {
                'runs-on': 'ubuntu-latest',
                'timeout-minutes': 30,
                'steps': [
                    {
                        'name': 'Checkout code',
                        'uses': 'actions/checkout@v4'
                    },
                    {
                        'name': 'Set up Python',
                        'uses': 'actions/setup-python@v4',
                        'with': {
                            'python-version': '3.11'
                        }
                    },
                    {
                        'name': 'Install dependencies',
                        'run': 'pip install -r requirements.txt'
                    },
                    {
                        'name': 'Run lead scrapers',
                        'env': {
                            'SUPABASE_URL': '${{ secrets.SUPABASE_URL }}',
                            'SUPABASE_KEY': '${{ secrets.SUPABASE_KEY }}',
                            'SCRAPER_API_KEY': '${{ secrets.SCRAPER_API_KEY }}'
                        },
                        'run': 'python master_threaded_scraper.py'
                    },
                    {
                        'name': 'Route leads',
                        'env': {
                            'SUPABASE_URL': '${{ secrets.SUPABASE_URL }}',
                            'SUPABASE_KEY': '${{ secrets.SUPABASE_KEY }}'
                        },
                        'run': 'python lead_router.py'
                    },
                    {
                        'name': 'Export daily report',
                        'env': {
                            'SUPABASE_URL': '${{ secrets.SUPABASE_URL }}',
                            'SUPABASE_KEY': '${{ secrets.SUPABASE_KEY }}',
                            'EMAIL_USER': '${{ secrets.EMAIL_USER }}',
                            'EMAIL_PASSWORD': '${{ secrets.EMAIL_PASSWORD }}',
                            'EMAIL_RECIPIENTS': '${{ secrets.EMAIL_RECIPIENTS }}',
                            'GOOGLE_SHEETS_CREDENTIALS': '${{ secrets.GOOGLE_SHEETS_CREDENTIALS }}'
                        },
                        'run': 'python lead_export.py --days 1'
                    },
                    {
                        'name': 'Send webhook notifications',
                        'env': {
                            'SUPABASE_URL': '${{ secrets.SUPABASE_URL }}',
                            'SUPABASE_KEY': '${{ secrets.SUPABASE_KEY }}',
                            'GHL_WEBHOOK_URL': '${{ secrets.GHL_WEBHOOK_URL }}',
                            'ZAPIER_WEBHOOK_URL': '${{ secrets.ZAPIER_WEBHOOK_URL }}'
                        },
                        'run': 'python webhook_integration.py'
                    }
                ]
            },
            'monitor-system': {
                'runs-on': 'ubuntu-latest',
                'needs': 'scrape-leads',
                'if': 'always()',
                'steps': [
                    {
                        'name': 'Checkout code',
                        'uses': 'actions/checkout@v4'
                    },
                    {
                        'name': 'System health check',
                        'env': {
                            'SUPABASE_URL': '${{ secrets.SUPABASE_URL }}',
                            'SUPABASE_KEY': '${{ secrets.SUPABASE_KEY }}'
                        },
                        'run': '''
                        python -c "
                        from supabase_config import SupabaseConnection
                        conn = SupabaseConnection()
                        if conn.supabase:
                            print('✅ Supabase connection: OK')
                        else:
                            print('❌ Supabase connection: FAILED')
                            exit(1)
                        "
                        '''
                    }
                ]
            }
        }
    }
    
    return yaml.dump(workflow, default_flow_style=False, sort_keys=False)

def generate_docker_compose() -> str:
    """Generate Docker Compose for local development"""
    
    compose = {
        'version': '3.8',
        'services': {
            'roofing-scraper': {
                'build': '.',
                'container_name': 'roofing-scraper',
                'environment': [
                    'SUPABASE_URL=${SUPABASE_URL}',
                    'SUPABASE_KEY=${SUPABASE_KEY}',
                    'SCRAPER_API_KEY=${SCRAPER_API_KEY}',
                    'EMAIL_USER=${EMAIL_USER}',
                    'EMAIL_PASSWORD=${EMAIL_PASSWORD}',
                    'EMAIL_RECIPIENTS=${EMAIL_RECIPIENTS}'
                ],
                'volumes': [
                    './exports:/app/exports',
                    './logs:/app/logs',
                    './.env:/app/.env'
                ],
                'command': 'python master_threaded_scraper.py',
                'restart': 'unless-stopped'
            },
            'lead-router': {
                'build': '.',
                'container_name': 'lead-router',
                'environment': [
                    'SUPABASE_URL=${SUPABASE_URL}',
                    'SUPABASE_KEY=${SUPABASE_KEY}'
                ],
                'volumes': [
                    './.env:/app/.env'
                ],
                'command': 'python lead_router.py',
                'depends_on': ['roofing-scraper'],
                'restart': 'unless-stopped'
            },
            'webhook-processor': {
                'build': '.',
                'container_name': 'webhook-processor',
                'environment': [
                    'SUPABASE_URL=${SUPABASE_URL}',
                    'SUPABASE_KEY=${SUPABASE_KEY}',
                    'GHL_WEBHOOK_URL=${GHL_WEBHOOK_URL}',
                    'ZAPIER_WEBHOOK_URL=${ZAPIER_WEBHOOK_URL}'
                ],
                'volumes': [
                    './.env:/app/.env'
                ],
                'command': 'python webhook_integration.py --monitor',
                'restart': 'unless-stopped'
            },
            'dashboard': {
                'build': '.',
                'container_name': 'lead-dashboard',
                'environment': [
                    'SUPABASE_URL=${SUPABASE_URL}',
                    'SUPABASE_KEY=${SUPABASE_KEY}'
                ],
                'volumes': [
                    './.env:/app/.env'
                ],
                'ports': ['8501:8501'],
                'command': 'streamlit run lead_dashboard.py --server.port=8501 --server.address=0.0.0.0',
                'restart': 'unless-stopped'
            }
        },
        'networks': {
            'default': {
                'name': 'roofing-network'
            }
        }
    }
    
    return yaml.dump(compose, default_flow_style=False, sort_keys=False)

def generate_dockerfile() -> str:
    """Generate Dockerfile for containerization"""
    
    dockerfile = """FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \\
    curl \\
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright dependencies
RUN playwright install chromium
RUN playwright install-deps

# Copy application code
COPY . .

# Create directories
RUN mkdir -p exports logs

# Set permissions
RUN chmod +x *.py

# Default command
CMD ["python", "master_threaded_scraper.py"]
"""
    
    return dockerfile

def generate_requirements_txt() -> str:
    """Generate requirements.txt with all dependencies"""
    
    requirements = [
        "supabase>=1.0.0",
        "python-dotenv>=1.0.0",
        "requests>=2.31.0",
        "beautifulsoup4>=4.12.0",
        "playwright>=1.40.0",
        "pandas>=2.0.0",
        "streamlit>=1.28.0",
        "plotly>=5.17.0",
        "gspread>=5.12.0",
        "google-auth>=2.23.0",
        "pyyaml>=6.0",
        "lxml>=4.9.0",
        "selenium>=4.15.0",
        "fake-useragent>=1.4.0",
        "schedule>=1.2.0",
        "psutil>=5.9.0"
    ]
    
    return '\n'.join(requirements)

def generate_cron_scripts() -> Dict[str, str]:
    """Generate cron job scripts"""
    
    scripts = {}
    
    # Daily scraping script
    scripts['daily_scrape.sh'] = """#!/bin/bash
# Daily Lead Scraping Script
# Add to crontab: 0 6,18 * * * /path/to/daily_scrape.sh

cd /path/to/roofing_scraper_auto_mode

# Activate virtual environment if using one
# source venv/bin/activate

# Run scrapers
python master_threaded_scraper.py

# Route leads
python lead_router.py

# Export daily report
python lead_export.py --days 1

# Send webhook notifications
python webhook_integration.py

echo "Daily scraping completed at $(date)" >> logs/cron.log
"""

    # Weekly maintenance script
    scripts['weekly_maintenance.sh'] = """#!/bin/bash
# Weekly Maintenance Script
# Add to crontab: 0 2 * * 0 /path/to/weekly_maintenance.sh

cd /path/to/roofing_scraper_auto_mode

# Backup database
python deploy_supabase.py backup

# Export weekly report
python lead_export.py --days 7

# Clean old logs
find logs/ -name "*.log" -mtime +30 -delete

# Clean old exports
find exports/ -name "*.csv" -mtime +7 -delete

echo "Weekly maintenance completed at $(date)" >> logs/maintenance.log
"""

    # System monitoring script
    scripts['health_check.sh'] = """#!/bin/bash
# System Health Check Script
# Add to crontab: */15 * * * * /path/to/health_check.sh

cd /path/to/roofing_scraper_auto_mode

# Check Supabase connection
python -c "
from supabase_config import SupabaseConnection
import sys
conn = SupabaseConnection()
if not conn.supabase:
    print('ALERT: Supabase connection failed at $(date)')
    sys.exit(1)
print('✅ Health check passed at $(date)')
" >> logs/health.log 2>&1

# Check disk space
df -h | awk '$5 > 80 {print "ALERT: Disk usage high on " $1 " at $(date)"}' >> logs/health.log

# Check memory usage
free | awk 'NR==2{printf "Memory usage: %s/%sMB (%.2f%%)\n", $3,$2,$3*100/$2 }' >> logs/health.log
"""

    return scripts

def generate_systemd_service() -> str:
    """Generate systemd service file for webhook processor"""
    
    service = """[Unit]
Description=Roofing Lead Webhook Processor
After=network.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/path/to/roofing_scraper_auto_mode
Environment=PATH=/path/to/roofing_scraper_auto_mode/venv/bin
ExecStart=/path/to/roofing_scraper_auto_mode/venv/bin/python webhook_integration.py --monitor
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
"""
    
    return service

def generate_env_template() -> str:
    """Generate .env template file"""
    
    template = """# Supabase Configuration
SUPABASE_URL=your_supabase_url_here
SUPABASE_KEY=your_supabase_anon_key_here
SUPABASE_PROJECT_ID=your_project_id_here
SUPABASE_ACCESS_TOKEN=your_access_token_here
SUPABASE_DB_PASSWORD=your_db_password_here

# ScraperAPI Configuration
SCRAPER_API_KEY=your_scraper_api_key_here

# Email Configuration (for reports)
EMAIL_USER=your_email@example.com
EMAIL_PASSWORD=your_email_password_here
EMAIL_RECIPIENTS=recipient1@example.com,recipient2@example.com
SMTP_SERVER=smtp.gmail.com
SMTP_PORT=587

# Google Sheets Configuration (optional)
GOOGLE_SHEETS_CREDENTIALS=service_account.json

# Webhook URLs
GHL_WEBHOOK_URL=your_gohighlevel_webhook_url
GHL_API_KEY=your_ghl_api_key
ZAPIER_WEBHOOK_URL=your_zapier_webhook_url
MAKE_WEBHOOK_URL=your_make_webhook_url
CUSTOM_WEBHOOK_URL=your_custom_webhook_url
CUSTOM_API_KEY=your_custom_api_key

# Optional: Proxy Configuration
HTTP_PROXY=
HTTPS_PROXY=
"""
    
    return template

def create_automation_files():
    """Create all automation files"""
    
    files_to_create = {
        '.github/workflows/lead-generation.yml': generate_github_actions_workflow(),
        'docker-compose.yml': generate_docker_compose(),
        'Dockerfile': generate_dockerfile(),
        'requirements.txt': generate_requirements_txt(),
        '.env.template': generate_env_template(),
        'systemd/roofing-webhook.service': generate_systemd_service()
    }
    
    # Create cron scripts
    cron_scripts = generate_cron_scripts()
    for script_name, content in cron_scripts.items():
        files_to_create[f'scripts/{script_name}'] = content
    
    created_files = []
    
    for file_path, content in files_to_create.items():
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        # Write file
        with open(file_path, 'w') as f:
            f.write(content)
        
        # Make shell scripts executable
        if file_path.endswith('.sh'):
            os.chmod(file_path, 0o755)
        
        created_files.append(file_path)
        print(f"✅ Created: {file_path}")
    
    return created_files

def main():
    """Main execution"""
    print("🚀 Generating automation files...")
    
    created_files = create_automation_files()
    
    print(f"\n✅ Successfully created {len(created_files)} automation files!")
    print("\n📋 Next Steps:")
    print("1. Copy .env.template to .env and fill in your credentials")
    print("2. Set up GitHub secrets for Actions workflow")
    print("3. Configure cron jobs using the scripts in scripts/")
    print("4. Deploy to production using Docker Compose")
    print("5. Set up systemd service for webhook monitoring")
    
    print("\n🔧 Useful Commands:")
    print("  # Deploy with Docker Compose")
    print("  docker-compose up -d")
    print("")
    print("  # Install cron jobs")
    print("  crontab -e")
    print("  # Add: 0 6,18 * * * /path/to/scripts/daily_scrape.sh")
    print("")
    print("  # Install systemd service")
    print("  sudo cp systemd/roofing-webhook.service /etc/systemd/system/")
    print("  sudo systemctl enable roofing-webhook")
    print("  sudo systemctl start roofing-webhook")

if __name__ == "__main__":
    main()