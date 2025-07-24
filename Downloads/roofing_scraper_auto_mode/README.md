# 🏠 Enterprise Roofing Lead Generation System

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Supabase](https://img.shields.io/badge/Database-Supabase-green.svg)](https://supabase.com)
[![Streamlit](https://img.shields.io/badge/Dashboard-Streamlit-red.svg)](https://streamlit.io)
[![ScraperAPI](https://img.shields.io/badge/Scraping-ScraperAPI-orange.svg)](https://scraperapi.com)

> **Automated multi-source lead generation system for roofing contractors with AI-powered scoring, real-time monitoring, and enterprise integrations.**

## 🎯 Key Features

- **🔄 Multi-Source Scraping**: Zillow, Redfin, County Appraisal Districts, Building Permits
- **🧠 AI Lead Scoring**: 7-factor algorithm with property value, age, location analysis
- **⚡ Real-time Processing**: Multi-threaded architecture with live database insertion
- **📊 Advanced Analytics**: Comprehensive dashboards and weekly PDF reports
- **🔗 Enterprise Integrations**: Webhooks for CRM systems (GoHighLevel, Zapier, Make.com)
- **📧 Automated Reporting**: Daily email summaries and system health alerts
- **🛡️ Production Ready**: Error handling, retry logic, and comprehensive monitoring

## 🚀 Quick Start

### 1. System Requirements
```bash
# Python 3.8+ with required packages
pip install --break-system-packages requests beautifulsoup4 supabase python-dotenv pandas streamlit plotly
```

### 2. Configuration
1. **Copy environment template**:
   ```bash
   cp .env.example ~/.Desktop/.env
   ```

2. **Configure Supabase** (Required):
   - Sign up at [supabase.com](https://supabase.com)
   - Create new project
   - Update `~/Desktop/.env` with your credentials:
   ```env
   SUPABASE_URL=https://your-project-id.supabase.co
   SUPABASE_KEY=your-service-role-key
   ```

3. **Deploy database schema**:
   ```bash
   python3 sql_runner.py full
   ```

### 3. One-Click Execution
```bash
# Test system components
./Desktop/test_system.command

# Run system health check
python3 ~/Desktop/system_check.py

# Start daily pipeline
./Desktop/run_daily.command

# Launch monitoring dashboard
streamlit run ~/Desktop/system_dashboard.py
```

## 📋 System Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Data Sources  │───▶│  Lead Processing │───▶│   Distribution  │
├─────────────────┤    ├──────────────────┤    ├─────────────────┤
│ • Zillow        │    │ • AI Scoring     │    │ • Supabase DB   │
│ • Redfin        │    │ • Deduplication  │    │ • Email Reports │
│ • CAD Records   │    │ • Enrichment     │    │ • CRM Webhooks  │
│ • Permits       │    │ • Routing Logic  │    │ • CSV Export    │
│ • Storm Data    │    │ • Quality Filter │    │ • Google Sheets │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                │
                       ┌──────────────────┐
                       │   Monitoring     │
                       ├──────────────────┤
                       │ • Health Checks  │
                       │ • Error Alerts   │
                       │ • Performance    │
                       │ • Weekly Reports │
                       └──────────────────┘
```

## 🧠 AI Lead Scoring Algorithm

Our proprietary 7-factor scoring system rates leads 1-10:

| Factor | Weight | Description |
|--------|--------|-------------|
| **Property Value** | 20% | Higher value = better lead |
| **Property Age** | 15% | 10-30 years optimal for roof replacement |
| **Location Score** | 15% | Target zip codes and neighborhoods |
| **Data Source** | 15% | Zillow > Redfin > CAD > Permits |
| **Storm Impact** | 10% | Recent hail/wind damage areas |
| **Permit Activity** | 10% | Recent building permits nearby |
| **Urgency Signals** | 5% | Days on market, price changes |

**High Priority Leads (8-10)**: Immediate follow-up recommended  
**Medium Priority (6-7)**: Follow-up within 48 hours  
**Low Priority (1-5)**: Nurture campaign appropriate  

## 📊 Monitoring & Analytics

### Real-time Dashboard
```bash
streamlit run ~/Desktop/system_dashboard.py
# Access at: http://localhost:8501
```

**Features:**
- Live system health monitoring
- Lead volume trends and source breakdown
- Error rate tracking and log analysis
- Performance metrics and alerts
- One-click system operations

### Weekly Health Reports
```bash
python3 ~/Desktop/weekly_report.py
```

**Generates:**
- Executive summary with key insights
- Professional PDF with charts and graphs
- Database performance metrics
- System reliability analysis
- Actionable recommendations

### System Health Checks
```bash
python3 ~/Desktop/system_check.py
```

**Validates:**
- Internet connectivity and DNS resolution
- Database connection and table integrity
- Python dependencies and system files
- Disk space and system resources
- API endpoints and webhook configurations

## 🔗 Enterprise Integrations

### CRM Webhooks
Configure in `~/Desktop/.env`:
```env
# GoHighLevel
GHL_WEBHOOK_URL=your_gohighlevel_webhook_url
GHL_API_KEY=your_ghl_api_key

# Zapier Integration
ZAPIER_WEBHOOK_URL=your_zapier_webhook_url

# Make.com Integration  
MAKE_WEBHOOK_URL=your_make_webhook_url

# Custom Webhook
CUSTOM_WEBHOOK_URL=your_custom_webhook_url
```

### Email Automation
```env
EMAIL_USER=your_email@gmail.com
EMAIL_PASSWORD=your_app_password
EMAIL_RECIPIENTS=you@domain.com,team@domain.com
ENABLE_EMAIL_ALERTS=true
```

### Google Sheets Export
```env
GOOGLE_SHEETS_CREDENTIALS=service_account.json
```

## 📁 Project Structure

```
roofing_scraper_auto_mode/
├── 📄 Core Scrapers
│   ├── master_threaded_scraper.py    # Multi-threaded orchestrator
│   ├── scrapers/
│   │   ├── threaded_zillow_scraper.py
│   │   ├── threaded_redfin_scraper.py
│   │   ├── threaded_cad_scraper.py
│   │   └── threaded_permit_scraper.py
│   
├── 🧠 Processing & Routing
│   ├── lead_router.py                # AI scoring and routing
│   ├── lead_export.py               # Multi-format export
│   └── webhook_integration.py       # CRM distribution
│   
├── 🗄️ Database & Config
│   ├── supabase_config.py           # Database connection
│   ├── supabase_tables.sql          # Schema definition
│   └── sql_runner.py                # Database deployment
│   
├── 📊 Monitoring & Reports
│   ├── system_check.py              # Health monitoring
│   ├── weekly_report.py             # PDF reports
│   └── system_dashboard.py          # Streamlit dashboard
│   
└── 🚀 Automation
    ├── run_daily.command            # One-click daily pipeline
    ├── test_system.command          # System validation
    └── automation_scripts.py       # Scheduled tasks
```

## 🛠️ Configuration Files

### Desktop Control Center
All control files are saved to `~/Desktop/` for easy access:

- **`.env`** - Environment configuration
- **`system_check.py`** - Health monitoring
- **`weekly_report.py`** - PDF report generator  
- **`system_dashboard.py`** - Streamlit dashboard
- **`run_daily.command`** - Daily pipeline launcher
- **`test_system.command`** - Quick system test

### Environment Variables
Complete configuration template in `~/Desktop/.env`:

```env
# Supabase Database (Required)
SUPABASE_URL=https://your-project-id.supabase.co
SUPABASE_KEY=your-service-role-key
SUPABASE_PROJECT_ID=your-project-id

# ScraperAPI (Recommended)
SCRAPER_API_KEY=your-scraperapi-key

# Email Notifications
EMAIL_USER=your_email@gmail.com
EMAIL_PASSWORD=your_app_password
EMAIL_RECIPIENTS=you@domain.com
ENABLE_EMAIL_ALERTS=true

# System Configuration
LOG_LEVEL=INFO
MAX_THREADS=5
BATCH_SIZE=100
RETRY_ATTEMPTS=3
```

## 🔧 Troubleshooting

### Common Issues

**❌ DNS Resolution Errors**
```bash
# Check internet connectivity
python3 ~/Desktop/system_check.py

# Verify Supabase credentials
grep SUPABASE_URL ~/Desktop/.env
```

**❌ Missing Dependencies**
```bash
# Install all required packages
pip install --break-system-packages requests beautifulsoup4 supabase python-dotenv pandas streamlit plotly

# Test imports
python3 -c "import requests, beautifulsoup4, supabase, dotenv"
```

**❌ Database Connection Failed**
```bash
# Verify Supabase configuration
python3 -c "from supabase_config import SupabaseConnection; conn = SupabaseConnection()"

# Deploy database schema
python3 sql_runner.py full
```

### System Recovery
```bash
# Complete system reset and fix
./Desktop/quick_fix.command

# Verify system health
python3 ~/Desktop/system_check.py

# Test with minimal configuration
./Desktop/test_system.command
```

## 📈 Performance Optimization

### Scaling Configuration
```env
# Increase for higher volume
MAX_THREADS=10
BATCH_SIZE=200

# ScraperAPI concurrency limits
CONCURRENT_REQUESTS=5
```

### Database Optimization
```sql
-- Add custom indexes for your queries
CREATE INDEX idx_leads_custom ON zillow_leads(zip_code, lead_score, created_at);

-- Partition large tables by date
-- (Contact support for enterprise partitioning)
```

## 🔒 Security & Compliance

- **Row Level Security (RLS)** enabled on all Supabase tables
- **Environment variable isolation** with `.env` files
- **API key rotation** supported for all integrations
- **Webhook signature verification** for secure endpoints
- **Rate limiting** and retry logic to prevent abuse
- **Data encryption** in transit and at rest via Supabase

## 📞 Support & Maintenance

### Health Monitoring
- **Automated daily health checks** with email alerts
- **Weekly system reports** with performance analysis
- **Real-time dashboard** for immediate issue detection
- **Comprehensive logging** for debugging and auditing

### Updates & Maintenance
```bash
# Check for system updates
git pull origin main

# Update dependencies
pip install --break-system-packages --upgrade requests beautifulsoup4 supabase python-dotenv

# Database migrations
python3 sql_runner.py migrate

# System health validation
python3 ~/Desktop/system_check.py
```

## 📋 API Reference

### Core Functions
```python
# Lead collection
from master_threaded_scraper import RoofingLeadScraper
scraper = RoofingLeadScraper()
scraper.run_all_scrapers()

# Lead scoring
from lead_router import LeadRouter
router = LeadRouter()
scored_leads = router.score_and_route_leads()

# Export and distribution
from lead_export import LeadExporter
exporter = LeadExporter()
exporter.run_daily_export()
```

### Database Operations
```python
from supabase_config import SupabaseConnection
conn = SupabaseConnection()

# Insert lead
conn.safe_insert('zillow_leads', lead_data)

# Query leads
leads = conn.supabase.table('zillow_leads').select('*').gte('lead_score', 8).execute()
```

## 🏆 Enterprise Features

- **✅ Multi-source data aggregation** from 4+ lead sources
- **✅ AI-powered lead scoring** with 7-factor algorithm  
- **✅ Real-time processing** with multi-threaded architecture
- **✅ Enterprise database** with Supabase PostgreSQL
- **✅ Advanced monitoring** with health checks and alerts
- **✅ Professional reporting** with PDF generation
- **✅ CRM integrations** via webhooks and APIs
- **✅ Automated operations** with one-click daily pipeline
- **✅ Comprehensive analytics** with Streamlit dashboard
- **✅ Production monitoring** with error handling and recovery

---

## 📄 License

MIT License - See [LICENSE](LICENSE) file for details.

## 🤝 Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open Pull Request

---

**🏠 Built for roofing contractors who demand enterprise-grade lead generation solutions.**