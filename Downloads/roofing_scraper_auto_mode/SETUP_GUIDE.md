# 🏠 Roofing Lead Generation System - Complete Setup Guide

## 🎯 Overview

This is an enterprise-grade roofing lead generation system with multi-source scraping, intelligent routing, real-time webhooks, and comprehensive analytics.

### Key Features
- **Multi-Source Scraping**: Zillow, Redfin, CAD, Permits, Storm Data
- **Real-time Database**: Supabase integration with automatic insertion
- **Smart Lead Routing**: AI-powered prioritization and assignment
- **Webhook Integration**: GoHighLevel, Zapier, Make.com automation
- **Live Dashboard**: Streamlit analytics and monitoring
- **Export & Reporting**: Daily exports to CSV/Google Sheets/Email
- **High Performance**: Multi-threaded scraping with ScraperAPI

## 🚀 Quick Start (5 Minutes)

### 1. Clone & Install
`bash
git clone <your-repo>
cd roofing_scraper_auto_mode
pip install -r requirements.txt
`

### 2. Configure Environment
`bash
cp .env.template .env
# Edit .env with your credentials (see Configuration section below)
`

### 3. Deploy Database Schema
`bash
python deploy_supabase.py full
`

### 4. Test the System
`bash
# Test Supabase connection
python -c "from supabase_config import SupabaseConnection; print('✅ Connected\!' if SupabaseConnection().supabase else '❌ Failed')"

# Run scrapers
python master_threaded_scraper.py

# Route leads
python lead_router.py

# Launch dashboard
streamlit run lead_dashboard.py
`

## 📋 Detailed Setup

### Prerequisites
- Python 3.9+
- Supabase account
- ScraperAPI account (optional but recommended)
- Email account for notifications

### Required Environment Variables

Create `.env` file with the following:

`env
# Supabase (Required)
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your_anon_key_here
SUPABASE_PROJECT_ID=your_project_id

# ScraperAPI (Recommended)
SCRAPER_API_KEY=your_scraper_api_key

# Email Notifications (Optional)
EMAIL_USER=your-email@gmail.com
EMAIL_PASSWORD=your_app_password
EMAIL_RECIPIENTS=recipient1@example.com,recipient2@example.com

# Webhooks (Optional)
GHL_WEBHOOK_URL=https://hooks.zapier.com/your-ghl-webhook
ZAPIER_WEBHOOK_URL=https://hooks.zapier.com/your-zapier-webhook
MAKE_WEBHOOK_URL=https://hook.integromat.com/your-make-webhook
`

## 🛠️ Component Guide

### 1. Data Collection

#### Master Threaded Scraper
`bash
# Run all scrapers in parallel
python master_threaded_scraper.py

# Individual scrapers
python scrapers/threaded_cad_scraper.py
python scrapers/threaded_redfin_scraper.py
python scrapers/threaded_permit_scraper.py
`

### 2. Lead Intelligence

#### Smart Lead Routing
`bash
# Route new leads (run after scraping)
python lead_router.py

# Route specific source
python lead_router.py --source permit

# Show routing summary
python lead_router.py --summary
`

**Lead Scoring Factors:**
- Property value (20% weight)
- Property age (15% weight)  
- Location desirability (15% weight)
- Source quality (15% weight)
- Storm damage (10% weight)
- Permit activity (10% weight)
- Urgency/recency (5% weight)

### 3. Export & Reporting

#### Daily Exports
`bash
# Export today's leads
python lead_export.py

# Export last 7 days from specific source
python lead_export.py --source permit --days 7

# Export without email notification
python lead_export.py --no-email
`

### 4. Webhook Integration

#### Real-time Lead Distribution
`bash
# Send new leads to webhooks
python webhook_integration.py

# Test webhook configurations  
python webhook_integration.py --test

# Continuous monitoring
python webhook_integration.py --monitor --interval 60
`

### 5. Analytics Dashboard

#### Live Dashboard
`bash
streamlit run lead_dashboard.py
`

Access at: `http://localhost:8501`

## 🔄 Automation Options

### GitHub Actions (Recommended)
`bash
# Generate automation files
python automation_scripts.py

# Commit to GitHub (triggers Actions)
git add .
git commit -m "Setup automation"
git push
`

**GitHub Secrets to Configure:**
- `SUPABASE_URL`
- `SUPABASE_KEY`  
- `SCRAPER_API_KEY`
- `EMAIL_USER`
- `EMAIL_PASSWORD`
- `EMAIL_RECIPIENTS`
- `GHL_WEBHOOK_URL`
- `ZAPIER_WEBHOOK_URL`

### Cron Jobs
`bash
# Daily scraping at 6 AM and 6 PM
0 6,18 * * * /path/to/scripts/daily_scrape.sh

# Weekly maintenance on Sundays at 2 AM
0 2 * * 0 /path/to/scripts/weekly_maintenance.sh

# Health checks every 15 minutes
*/15 * * * * /path/to/scripts/health_check.sh
`

## 📊 Performance Optimization

### ScraperAPI Integration
- Handles IP rotation automatically
- Bypasses anti-bot measures
- Includes CAPTCHA solving
- 99.9% success rate

### Threading & Concurrency
- **Master scraper**: 3 parallel scrapers
- **Individual scrapers**: 5 threads each
- **Webhook processing**: Batch sending
- **Database**: Connection pooling

## 🎯 You're Ready to Scale\!

Your roofing lead generation empire is now fully deployed with:

✅ **Automated Lead Collection** - Multi-source scraping  
✅ **Intelligent Routing** - AI-powered prioritization  
✅ **Real-time Integration** - Webhook automation  
✅ **Live Analytics** - Performance dashboard  
✅ **Enterprise Automation** - GitHub Actions, Docker, Cron  

**Start generating leads immediately:**
`bash
python master_threaded_scraper.py && python lead_router.py
`

**Monitor performance:**
`bash
streamlit run lead_dashboard.py
`

**Scale with automation:**
`bash
python automation_scripts.py && git push
`

Your roofing business is now powered by enterprise-grade lead generation technology\! 🚀