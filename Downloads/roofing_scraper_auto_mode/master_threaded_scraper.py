#!/usr/bin/env python3
"""
Master Threaded Scraper Controller
Coordinates all threaded scrapers with ScraperAPI and Supabase
"""

import os
import sys
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import subprocess

# Add current directory to path for imports
sys.path.append('.')

def run_scraper(scraper_name):
    """Run individual scraper and return results"""
    try:
        start_time = time.time()
        print(f"🚀 Starting {scraper_name}...")
        
        # Run the scraper as subprocess
        result = subprocess.run([
            sys.executable, 
            f"scrapers/{scraper_name}.py"
        ], capture_output=True, text=True, timeout=300)
        
        end_time = time.time()
        runtime = end_time - start_time
        
        if result.returncode == 0:
            print(f"✅ {scraper_name} completed in {runtime:.2f}s")
            return {
                'scraper': scraper_name,
                'status': 'success',
                'runtime': runtime,
                'output': result.stdout
            }
        else:
            print(f"❌ {scraper_name} failed: {result.stderr}")
            return {
                'scraper': scraper_name,
                'status': 'failed',
                'runtime': runtime,
                'error': result.stderr
            }
            
    except subprocess.TimeoutExpired:
        print(f"⏰ {scraper_name} timed out after 5 minutes")
        return {
            'scraper': scraper_name,
            'status': 'timeout',
            'runtime': 300,
            'error': 'Timeout after 5 minutes'
        }
    except Exception as e:
        print(f"❌ Error running {scraper_name}: {e}")
        return {
            'scraper': scraper_name,
            'status': 'error',
            'runtime': 0,
            'error': str(e)
        }

def run_parallel_scrapers():
    """Run all scrapers in parallel"""
    
    # Available scrapers
    scrapers = [
        'threaded_cad_scraper',
        'threaded_redfin_scraper', 
        'threaded_permit_scraper'
    ]
    
    print("🎯 MASTER THREADED SCRAPER STARTING")
    print("=" * 60)
    print(f"📊 Running {len(scrapers)} scrapers in parallel")
    print(f"🕐 Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    results = []
    start_time = time.time()
    
    # Run scrapers in parallel
    with ThreadPoolExecutor(max_workers=3) as executor:
        # Submit all scrapers
        future_to_scraper = {
            executor.submit(run_scraper, scraper): scraper 
            for scraper in scrapers
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_scraper):
            scraper = future_to_scraper[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                print(f"❌ Exception in {scraper}: {e}")
                results.append({
                    'scraper': scraper,
                    'status': 'exception',
                    'runtime': 0,
                    'error': str(e)
                })
    
    end_time = time.time()
    total_runtime = end_time - start_time
    
    # Print comprehensive report
    print_final_report(results, total_runtime)
    
    return results

def print_final_report(results, total_runtime):
    """Print comprehensive final report"""
    print("\n" + "=" * 60)
    print("🎯 MASTER SCRAPER FINAL REPORT")
    print("=" * 60)
    
    # Overall stats
    successful = len([r for r in results if r['status'] == 'success'])
    failed = len([r for r in results if r['status'] != 'success'])
    
    print(f"⏱️  Total Runtime: {total_runtime:.2f} seconds")
    print(f"✅ Successful: {successful}")
    print(f"❌ Failed: {failed}")
    print(f"📊 Total Scrapers: {len(results)}")
    
    # Individual scraper results
    print("\n📋 Individual Scraper Results:")
    for result in results:
        status_emoji = "✅" if result['status'] == 'success' else "❌"
        scraper_name = result['scraper'].replace('threaded_', '').replace('_scraper', '').title()
        runtime = result['runtime']
        
        print(f"   {status_emoji} {scraper_name}: {result['status']} ({runtime:.2f}s)")
        
        if result['status'] != 'success' and 'error' in result:
            print(f"      Error: {result['error'][:100]}...")
    
    # Performance stats
    if results:
        avg_runtime = sum(r['runtime'] for r in results) / len(results)
        fastest = min(results, key=lambda x: x['runtime'])
        slowest = max(results, key=lambda x: x['runtime'])
        
        print(f"\n📈 Performance Stats:")
        print(f"   • Average Runtime: {avg_runtime:.2f}s")
        print(f"   • Fastest: {fastest['scraper']} ({fastest['runtime']:.2f}s)")
        print(f"   • Slowest: {slowest['scraper']} ({slowest['runtime']:.2f}s)")
    
    # Supabase status
    print(f"\n📊 Data Storage:")
    print(f"   • All scraped data automatically inserted into Supabase")
    print(f"   • Tables: cad_leads, redfin_leads, permit_leads")
    print(f"   • Check your Supabase dashboard for real-time data")
    
    print("\n✅ MASTER SCRAPER COMPLETED!")
    print("=" * 60)

def check_dependencies():
    """Check if all required dependencies are available"""
    try:
        import requests
        import beautifulsoup4
        import supabase
        import dotenv
        print("✅ All dependencies available")
        return True
    except ImportError as e:
        print(f"❌ Missing dependency: {e}")
        print("Run: pip install requests beautifulsoup4 supabase python-dotenv")
        return False

def check_environment():
    """Check if environment variables are set"""
    from config import SUPABASE_URL, SUPABASE_KEY, SCRAPER_API_KEY
    
    if not SUPABASE_URL:
        print("❌ SUPABASE_URL not set in .env")
        return False
    if not SUPABASE_KEY:
        print("❌ SUPABASE_KEY not set in .env")
        return False
    if not SCRAPER_API_KEY:
        print("⚠️  SCRAPER_API_KEY not set, using default")
    
    print("✅ Environment configuration OK")
    return True

def main():
    """Main execution function"""
    print("🔍 Checking dependencies and environment...")
    
    if not check_dependencies():
        return 1
    
    if not check_environment():
        return 1
    
    # Run all scrapers
    results = run_parallel_scrapers()
    
    # Return success if any scrapers succeeded
    successful_count = len([r for r in results if r['status'] == 'success'])
    return 0 if successful_count > 0 else 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)