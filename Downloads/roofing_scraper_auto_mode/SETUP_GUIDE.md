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
```bash
git clone <your-repo>
cd roofing_scraper_auto_mode
pip install -r requirements.txt
```

### 2. Configure Environment
```bash
cp .env.template .env
# Edit .env with your credentials (see Configuration section below)
```

### 3. Deploy Database Schema
```bash
python deploy_supabase.py full
```

### 4. Test the System
```bash
# Test Supabase connection
python -c "from supabase_config import SupabaseConnection; print('✅ Connected!' if SupabaseConnection().supabase else '❌ Failed')"

# Run scrapers
python master_threaded_scraper.py

# Route leads
python lead_router.py

# Launch dashboard
streamlit run lead_dashboard.py
```

## 📋 Detailed Setup

### Prerequisites
- Python 3.9+
- Supabase account
- ScraperAPI account (optional but recommended)
- Email account for notifications

### Required Environment Variables

Create `.env` file with the following:

```env
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
```

### Database Setup

#### Option 1: Automated Setup
```bash
python deploy_supabase.py full
```

#### Option 2: Manual Setup
1. Go to your Supabase SQL editor
2. Copy and paste `supabase_tables.sql`
3. Execute the SQL

#### Database Tables Created
- `zillow_leads` - Zillow property data
- `redfin_leads` - Redfin property data  
- `cad_leads` - County appraisal district data
- `permit_leads` - Building permit data
- `storm_events` - Storm and hail event data

Each table includes lead routing fields:
- `lead_status` - hot, warm, cold, follow_up, etc.
- `priority` - urgent, high, medium, low
- `routing_tags` - Comma-separated tags
- `next_action` - Recommended next step
- `assigned_to` - Team member assignment

## 🛠️ Component Guide

### 1. Data Collection

#### Master Threaded Scraper
```bash
# Run all scrapers in parallel
python master_threaded_scraper.py

# Individual scrapers
python scrapers/threaded_cad_scraper.py
python scrapers/threaded_redfin_scraper.py
python scrapers/threaded_permit_scraper.py
```

#### Legacy Scrapers (Also Available)
```bash
python permit_scraper.py
python redfin_scraper.py
python texas_cad_scraper.py
python storm_integration.py
```

### 2. Lead Intelligence

#### Smart Lead Routing
```bash
# Route new leads (run after scraping)
python lead_router.py

# Route specific source
python lead_router.py --source permit

# Show routing summary
python lead_router.py --summary
```

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
```bash
# Export today's leads
python lead_export.py

# Export last 7 days from specific source
python lead_export.py --source permit --days 7

# Export without email notification
python lead_export.py --no-email
```

**Export Formats:**
- CSV files (timestamped)
- Google Sheets (with summary)
- Email reports (with attachments)

### 4. Webhook Integration

#### Real-time Lead Distribution
```bash
# Send new leads to webhooks
python webhook_integration.py

# Test webhook configurations  
python webhook_integration.py --test

# Continuous monitoring
python webhook_integration.py --monitor --interval 60
```

**Supported Integrations:**
- GoHighLevel CRM
- Zapier automations
- Make.com workflows
- Custom webhook endpoints

### 5. Analytics Dashboard

#### Live Dashboard
```bash
streamlit run lead_dashboard.py
```

**Dashboard Features:**
- Real-time lead metrics
- Source performance analysis
- Geographic heat maps
- Lead quality scoring
- Conversion tracking
- Export functionality

Access at: `http://localhost:8501`

## 🔄 Automation Options

### GitHub Actions (Recommended)
```bash
# Generate automation files
python automation_scripts.py

# Commit to GitHub (triggers Actions)
git add .
git commit -m "Setup automation"
git push
```

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
```bash
# Install cron scripts
python automation_scripts.py

# Add to crontab
crontab -e

# Daily scraping at 6 AM and 6 PM
0 6,18 * * * /path/to/scripts/daily_scrape.sh

# Weekly maintenance on Sundays at 2 AM
0 2 * * 0 /path/to/scripts/weekly_maintenance.sh

# Health checks every 15 minutes
*/15 * * * * /path/to/scripts/health_check.sh
```

### Docker Deployment
```bash
# Generate Docker files
python automation_scripts.py

# Deploy with Docker Compose
docker-compose up -d

# View logs
docker-compose logs -f
```

### Systemd Service (Linux)
```bash
# Install webhook service
sudo cp systemd/roofing-webhook.service /etc/systemd/system/
sudo systemctl enable roofing-webhook
sudo systemctl start roofing-webhook

# Check status
sudo systemctl status roofing-webhook
```

## 📊 Performance Optimization

### ScraperAPI Integration
- Handles IP rotation automatically
- Bypasses anti-bot measures
- Includes CAPTCHA solving
- 99.9% success rate

**Configuration:**
```python
# In .env
SCRAPER_API_KEY=your_key_here

# Automatic usage in threaded scrapers
threaded_scrape(urls, parse_function, "table_name", threads=5)
```

### Threading & Concurrency
- **Master scraper**: 3 parallel scrapers
- **Individual scrapers**: 5 threads each
- **Webhook processing**: Batch sending
- **Database**: Connection pooling

### Caching & Rate Limiting
- **Dashboard**: 5-minute cache on data queries
- **API calls**: Exponential backoff on failures
- **Database**: Automatic duplicate prevention
- **Exports**: Incremental processing

## 🔒 Security Best Practices

### Environment Security
- Use `.env` files (never commit credentials)
- Rotate API keys regularly
- Use app passwords for email
- Enable 2FA on all accounts

### Database Security
- Row Level Security (RLS) enabled
- Service role for scrapers
- Authenticated role for dashboard
- Regular backups automated

### Webhook Security
- HTTPS endpoints only
- API key authentication
- Request signing (where supported)
- Rate limiting protection

## 🐛 Troubleshooting

### Common Issues

#### "Supabase connection failed"
```bash
# Check credentials
python -c "import os; print(os.getenv('SUPABASE_URL', 'NOT SET'))"

# Test connection
python -c "from supabase_config import SupabaseConnection; SupabaseConnection()"
```

#### "No leads found"
```bash
# Check scraper logs
python master_threaded_scraper.py

# Verify target URLs are accessible
curl -I "https://www.redfin.com"

# Check ScraperAPI balance
curl "http://api.scraperapi.com/account?api_key=YOUR_KEY"
```

#### "Dashboard not loading"
```bash
# Install Streamlit
pip install streamlit

# Check port availability
lsof -i :8501

# Run with debug
streamlit run lead_dashboard.py --logger.level=debug
```

#### "Webhooks not firing"
```bash
# Test webhook URLs
python webhook_integration.py --test

# Check webhook logs
python webhook_integration.py --monitor

# Verify URL accessibility
curl -X POST -d '{"test": true}' YOUR_WEBHOOK_URL
```

### Logs & Monitoring

#### Log Locations
- `logs/scraper.log` - Scraping activity
- `logs/router.log` - Lead routing decisions  
- `logs/webhook.log` - Webhook deliveries
- `logs/export.log` - Export operations
- `logs/health.log` - System health checks

#### Monitoring Commands
```bash
# Real-time scraping logs
tail -f logs/scraper.log

# Database connection health
python -c "from supabase_config import SupabaseConnection; print('OK' if SupabaseConnection().supabase else 'FAIL')"

# Lead processing stats
python lead_router.py --summary

# Webhook test
python webhook_integration.py --test
```

## 📈 Scaling & Optimization

### Horizontal Scaling
- Deploy multiple scraper instances
- Use different ScraperAPI accounts
- Load balance webhook processing
- Regional Supabase deployments

### Performance Tuning
- Increase thread counts for high-volume
- Optimize database indexes
- Cache frequently accessed data
- Use CDN for static assets

### Data Management
- Implement data retention policies
- Archive old leads automatically
- Compress export files
- Monitor storage usage

## 🔗 Integration Examples

### GoHighLevel Integration
```javascript
// Zapier webhook handler
const leadData = inputData;

// Map to GHL contact format
const contact = {
  firstName: leadData.owner_name?.split(' ')[0] || 'Unknown',
  lastName: leadData.owner_name?.split(' ')[1] || 'Lead',
  email: `${leadData.zip_code}@leads.yourcompany.com`,
  phone: '555-ROOFING',
  address1: leadData.address_text,
  city: leadData.city,
  state: leadData.state,
  postalCode: leadData.zip_code,
  tags: leadData.routing_tags?.split(',') || [],
  customFields: [
    { id: 'lead_score', value: leadData.lead_score },
    { id: 'property_value', value: leadData.price },
    { id: 'lead_source', value: leadData.source }
  ]
};

// Send to GHL API
return await ghl.contacts.create(contact);
```

### Custom CRM Integration
```python
# webhook_integration.py extension
def send_to_custom_crm(lead_data):
    crm_payload = {
        'contact': {
            'name': lead_data.get('owner_name', 'Unknown'),
            'address': lead_data.get('address_text'),
            'score': lead_data.get('lead_score'),
            'source': lead_data.get('source'),
            'tags': lead_data.get('routing_tags', '').split(',')
        },
        'property': {
            'value': lead_data.get('price'),
            'year_built': lead_data.get('year_built'),
            'type': lead_data.get('property_type')
        }
    }
    
    response = requests.post(
        'https://your-crm.com/api/leads',
        json=crm_payload,
        headers={'Authorization': f'Bearer {CRM_API_KEY}'}
    )
    
    return response.status_code == 200
```

## 📞 Support & Maintenance

### Regular Maintenance Tasks
- **Daily**: Monitor scraper performance
- **Weekly**: Review lead quality scores
- **Monthly**: Update routing rules
- **Quarterly**: Review and optimize performance

### System Updates
```bash
# Update dependencies
pip install -r requirements.txt --upgrade

# Deploy schema changes
python deploy_supabase.py deploy

# Restart services
docker-compose restart
```

### Backup & Recovery
```bash
# Create database backup
python deploy_supabase.py backup

# Export configuration
cp .env .env.backup

# Test disaster recovery
python -c "from supabase_config import SupabaseConnection; print('✅' if SupabaseConnection().supabase else '❌')"
```

## 🎉 Success Metrics

### Key Performance Indicators
- **Lead Volume**: Target 100+ leads/day
- **Lead Quality**: Average score > 6.5
- **Response Time**: < 2 hours for urgent leads
- **Conversion Rate**: Track through CRM integration
- **System Uptime**: > 99.5%

### Optimization Opportunities
- A/B testing on lead scoring weights
- Geographic expansion to new markets
- Additional data sources integration
- Machine learning lead prediction
- Automated follow-up sequences

---

## 🎯 You're Ready to Scale!

Your roofing lead generation empire is now fully deployed with:

✅ **Automated Lead Collection** - Multi-source scraping  
✅ **Intelligent Routing** - AI-powered prioritization  
✅ **Real-time Integration** - Webhook automation  
✅ **Live Analytics** - Performance dashboard  
✅ **Enterprise Automation** - GitHub Actions, Docker, Cron  

**Start generating leads immediately:**
```bash
python master_threaded_scraper.py && python lead_router.py
```

**Monitor performance:**
```bash
streamlit run lead_dashboard.py
```

**Scale with automation:**
```bash
python automation_scripts.py && git push
```

Your roofing business is now powered by enterprise-grade lead generation technology! 🚀