# Roofing Lead Scrapers

This directory contains all roofing lead scraping tools with Supabase integration.

## Scrapers Overview

### Property Scrapers
- **`dfw_zillow_scraper.py`** - Zillow DFW area scraper
- **`playwright_zillow_scraper.py`** - Playwright-based Zillow scraper
- **`redfin_scraper.py`** - Redfin property scraper
- **`texas_cad_scraper.py`** - Texas CAD (County Appraisal District) scraper

### Permit & Storm Data
- **`permit_scraper.py`** - Building permit scraper
- **`storm_integration.py`** - Storm/hail damage integration

## Supabase Integration

All scrapers include Supabase integration that:
- Inserts leads into dedicated tables (zillow_leads, redfin_leads, cad_leads, permit_leads, storm_events)
- Uses shared configuration from `../supabase_config.py`
- Loads credentials from `../.env`
- Includes error handling and logging

## Database Tables

Run `../supabase_tables.sql` in your Supabase SQL editor to create:
- `zillow_leads` - Zillow property data
- `redfin_leads` - Redfin property data  
- `cad_leads` - Texas CAD property data
- `permit_leads` - Building permit data
- `storm_events` - Storm and hail events

## Setup Requirements

1. Install dependencies:
   ```bash
   pip install supabase python-dotenv requests beautifulsoup4 playwright
   ```

2. Configure environment variables in `../.env`:
   ```
   SUPABASE_URL=your_supabase_url
   SUPABASE_KEY=your_supabase_key
   ```

3. Run SQL schema: Execute `../supabase_tables.sql` in Supabase

4. Test individual scrapers:
   ```bash
   python permit_scraper.py
   python redfin_scraper.py  
   python texas_cad_scraper.py
   ```

## Lead Scoring

All scrapers include intelligent lead scoring (1-10):
- **High Priority (8-10)**: Recent construction, high-value properties, storm-affected areas
- **Medium Priority (6-7)**: Good value properties, moderate age
- **Low Priority (1-5)**: Basic properties, newer construction

## Usage with Control Panel

Use `../unified_control_panel.py` to run all scrapers together with centralized management.