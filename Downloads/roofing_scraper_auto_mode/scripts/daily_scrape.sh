#!/bin/bash
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