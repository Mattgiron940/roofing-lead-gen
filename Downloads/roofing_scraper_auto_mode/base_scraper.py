import os
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from supabase_client import supabase
from config import SCRAPER_API_KEY

def get_scraperapi_url(target_url):
    return f"http://api.scraperapi.com?api_key={SCRAPER_API_KEY}&url={target_url}"

def threaded_scrape(urls, parse_func, table_name, threads=5):
    print(f"🚀 Scraping {len(urls)} URLs into {table_name} with {threads} threads.")

    def scrape_and_insert(url):
        try:
            res = requests.get(get_scraperapi_url(url), timeout=30)
            res.raise_for_status()
            data = parse_func(res.text, url)
            if data:
                supabase.table(table_name).insert(data).execute()
                print(f"✅ Inserted from {url}")
            else:
                print(f"⚠️ No data for {url}")
        except Exception as e:
            print(f"❌ Error on {url}: {e}")

    with ThreadPoolExecutor(max_workers=threads) as executor:
        futures = [executor.submit(scrape_and_insert, u) for u in urls]
        for i, future in enumerate(as_completed(futures)):
            future.result()
            print(f"📦 Done {i+1}/{len(urls)}")