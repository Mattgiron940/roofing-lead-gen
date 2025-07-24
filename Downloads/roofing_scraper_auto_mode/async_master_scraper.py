#!/usr/bin/env python3
"""
Master Async Scraper Orchestration
Coordinates all async scrapers to achieve 5,000+ requests/hour across
Zillow, Redfin, CAD, and Permits with intelligent load balancing
"""

import asyncio
import json
import time
import logging
from datetime import datetime
from typing import Dict, List, Optional
from pathlib import Path
import os

# Import all async scrapers
from async_zillow_scraper import AsyncZillowScraper
from async_redfin_scraper import AsyncRedfinScraper
from async_cad_scraper import AsyncCADScraper
from async_permit_scraper import AsyncPermitScraper
from async_scraper_framework import ScraperConfig

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/async_master_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class AsyncMasterScraper:
    """Master orchestrator for all async scrapers"""
    
    def __init__(self):
        # Scraper configurations optimized for 5K+ requests/hour total
        self.scraper_configs = {
            'zillow': {
                'max_concurrent': 120,
                'requests_per_hour': 1500,
                'priority': 1  # Highest priority
            },
            'redfin': {
                'max_concurrent': 100,
                'requests_per_hour': 1400,
                'priority': 2
            },
            'cad': {
                'max_concurrent': 80,
                'requests_per_hour': 1200,
                'priority': 3
            },
            'permits': {
                'max_concurrent': 90,
                'requests_per_hour': 1300,
                'priority': 2
            }
        }
        
        # Performance tracking
        self.master_stats = {
            'start_time': datetime.now(),
            'scrapers_launched': 0,
            'total_requests': 0,
            'total_leads_extracted': 0,
            'total_leads_saved': 0,
            'scraper_results': {}
        }
        
        # Ensure logs directory exists
        Path('logs').mkdir(exist_ok=True)
        
        logger.info("🚀 Master Async Scraper initialized")
        logger.info(f"📊 Target: 5,400 requests/hour across 4 scrapers")
        
    async def run_all_scrapers_parallel(self) -> Dict:
        """Run all scrapers in parallel for maximum throughput"""
        
        logger.info("🌟 Starting parallel execution of all async scrapers")
        
        # Create scraper instances
        scrapers = {}
        
        try:
            # Initialize all scrapers
            zillow_config = self.scraper_configs['zillow']
            scrapers['zillow'] = AsyncZillowScraper(
                max_concurrent=zillow_config['max_concurrent'],
                requests_per_hour=zillow_config['requests_per_hour']
            )
            
            redfin_config = self.scraper_configs['redfin']
            scrapers['redfin'] = AsyncRedfinScraper(
                max_concurrent=redfin_config['max_concurrent'],
                requests_per_hour=redfin_config['requests_per_hour']
            )
            
            cad_config = self.scraper_configs['cad']
            scrapers['cad'] = AsyncCADScraper(
                max_concurrent=cad_config['max_concurrent'],
                requests_per_hour=cad_config['requests_per_hour']
            )
            
            permits_config = self.scraper_configs['permits']
            scrapers['permits'] = AsyncPermitScraper(
                max_concurrent=permits_config['max_concurrent'],
                requests_per_hour=permits_config['requests_per_hour']
            )
            
            self.master_stats['scrapers_launched'] = len(scrapers)
            
            # Launch all scrapers concurrently
            logger.info(f"🚀 Launching {len(scrapers)} scrapers concurrently...")
            
            tasks = []
            for scraper_name, scraper_instance in scrapers.items():
                task = asyncio.create_task(
                    self.run_scraper_with_monitoring(scraper_name, scraper_instance),
                    name=f"scraper_{scraper_name}"
                )
                tasks.append(task)
            
            # Wait for all scrapers to complete
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results
            for i, result in enumerate(results):
                scraper_name = list(scrapers.keys())[i]
                
                if isinstance(result, Exception):
                    logger.error(f"❌ {scraper_name} scraper failed: {result}")
                    self.master_stats['scraper_results'][scraper_name] = {
                        'status': 'failed',
                        'error': str(result)
                    }
                else:
                    logger.info(f"✅ {scraper_name} scraper completed successfully")
                    self.master_stats['scraper_results'][scraper_name] = result
            
            # Generate master performance report
            return self.generate_master_report()
            
        except Exception as e:
            logger.error(f"❌ Master scraper execution failed: {e}")
            raise
    
    async def run_scraper_with_monitoring(self, scraper_name: str, scraper_instance) -> Dict:
        """Run individual scraper with monitoring and error handling"""
        
        logger.info(f"🔄 Starting {scraper_name} scraper...")
        start_time = time.time()
        
        try:
            # Run the scraper
            result = await scraper_instance.run_scraper()
            
            # Add timing information
            result['scraper_name'] = scraper_name
            result['execution_time'] = time.time() - start_time
            
            # Update master stats
            self.master_stats['total_requests'] += result.get('total_requests', 0)
            self.master_stats['total_leads_extracted'] += result.get('leads_extracted', 0)
            self.master_stats['total_leads_saved'] += result.get('leads_saved', 0)
            
            logger.info(f"✅ {scraper_name}: {result.get('leads_saved', 0)} leads saved in {result['execution_time']:.1f}s")
            
            return result
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"❌ {scraper_name} failed after {execution_time:.1f}s: {e}")
            
            return {
                'scraper_name': scraper_name,
                'status': 'failed',
                'error': str(e),
                'execution_time': execution_time,
                'total_requests': 0,
                'leads_extracted': 0,
                'leads_saved': 0
            }
    
    async def run_scrapers_sequential(self) -> Dict:
        """Run scrapers sequentially (backup mode)"""
        
        logger.info("🔄 Starting sequential execution of async scrapers")
        
        # Order by priority
        scraper_order = sorted(
            self.scraper_configs.items(),
            key=lambda x: x[1]['priority']
        )
        
        for scraper_name, config in scraper_order:
            try:
                logger.info(f"🚀 Starting {scraper_name} scraper...")
                
                # Create and run scraper
                if scraper_name == 'zillow':
                    scraper = AsyncZillowScraper(
                        max_concurrent=config['max_concurrent'],
                        requests_per_hour=config['requests_per_hour']
                    )
                elif scraper_name == 'redfin':
                    scraper = AsyncRedfinScraper(
                        max_concurrent=config['max_concurrent'],
                        requests_per_hour=config['requests_per_hour']
                    )
                elif scraper_name == 'cad':
                    scraper = AsyncCADScraper(
                        max_concurrent=config['max_concurrent'],
                        requests_per_hour=config['requests_per_hour']
                    )
                elif scraper_name == 'permits':
                    scraper = AsyncPermitScraper(
                        max_concurrent=config['max_concurrent'],
                        requests_per_hour=config['requests_per_hour']
                    )
                
                result = await self.run_scraper_with_monitoring(scraper_name, scraper)
                self.master_stats['scraper_results'][scraper_name] = result
                
                # Brief pause between scrapers
                await asyncio.sleep(2)
                
            except Exception as e:
                logger.error(f"❌ {scraper_name} failed in sequential mode: {e}")
                self.master_stats['scraper_results'][scraper_name] = {
                    'status': 'failed',
                    'error': str(e)
                }
        
        return self.generate_master_report()
    
    def generate_master_report(self) -> Dict:
        """Generate comprehensive master performance report"""
        
        total_runtime = (datetime.now() - self.master_stats['start_time']).total_seconds()
        
        # Calculate aggregate metrics
        successful_scrapers = sum(1 for result in self.master_stats['scraper_results'].values() 
                                 if result.get('status') != 'failed')
        
        total_requests = self.master_stats['total_requests']
        total_leads_extracted = self.master_stats['total_leads_extracted']
        total_leads_saved = self.master_stats['total_leads_saved']
        
        # Calculate performance metrics
        requests_per_second = total_requests / max(total_runtime, 1)
        requests_per_hour = requests_per_second * 3600
        
        # Success rates
        extraction_success_rate = (total_leads_extracted / max(total_requests, 1)) * 100
        save_success_rate = (total_leads_saved / max(total_leads_extracted, 1)) * 100
        
        report = {
            'master_execution': {
                'start_time': self.master_stats['start_time'].isoformat(),
                'end_time': datetime.now().isoformat(),
                'total_runtime_seconds': round(total_runtime, 2),
                'scrapers_launched': self.master_stats['scrapers_launched'],
                'successful_scrapers': successful_scrapers,
                'failed_scrapers': self.master_stats['scrapers_launched'] - successful_scrapers
            },
            'aggregate_performance': {
                'total_requests': total_requests,
                'total_leads_extracted': total_leads_extracted,
                'total_leads_saved': total_leads_saved,
                'requests_per_second': round(requests_per_second, 2),
                'requests_per_hour': round(requests_per_hour, 0),
                'extraction_success_rate_percent': round(extraction_success_rate, 2),
                'save_success_rate_percent': round(save_success_rate, 2)
            },
            'scraper_breakdown': self.master_stats['scraper_results'],
            'target_achievement': {
                'target_requests_per_hour': 5400,
                'achieved_requests_per_hour': round(requests_per_hour, 0),
                'target_achieved': requests_per_hour >= 5000,
                'performance_ratio': round((requests_per_hour / 5400) * 100, 1)
            }
        }
        
        return report
    
    async def save_master_report(self, report: Dict, filename: Optional[str] = None):
        """Save master report to file"""
        
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"logs/master_scraper_report_{timestamp}.json"
        
        try:
            with open(filename, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            
            logger.info(f"📊 Master report saved to {filename}")
            
        except Exception as e:
            logger.error(f"❌ Failed to save master report: {e}")
    
    def print_performance_summary(self, report: Dict):
        """Print formatted performance summary"""
        
        print("\n" + "="*70)
        print("🚀 ASYNC MASTER SCRAPER PERFORMANCE REPORT")
        print("="*70)
        
        # Master execution summary
        master_exec = report['master_execution']
        print(f"📅 Execution Time: {master_exec['total_runtime_seconds']:.1f} seconds")
        print(f"🔧 Scrapers Launched: {master_exec['scrapers_launched']}")
        print(f"✅ Successful: {master_exec['successful_scrapers']}")
        print(f"❌ Failed: {master_exec['failed_scrapers']}")
        
        # Aggregate performance
        perf = report['aggregate_performance']
        print(f"\n📊 AGGREGATE PERFORMANCE:")
        print(f"   • Total Requests: {perf['total_requests']:,}")
        print(f"   • Leads Extracted: {perf['total_leads_extracted']:,}")
        print(f"   • Leads Saved: {perf['total_leads_saved']:,}")
        print(f"   • Requests/Second: {perf['requests_per_second']:.2f}")
        print(f"   • Requests/Hour: {perf['requests_per_hour']:,.0f}")
        print(f"   • Extraction Rate: {perf['extraction_success_rate_percent']:.1f}%")
        print(f"   • Save Rate: {perf['save_success_rate_percent']:.1f}%")
        
        # Target achievement
        target = report['target_achievement']
        print(f"\n🎯 TARGET ACHIEVEMENT:")
        print(f"   • Target: {target['target_requests_per_hour']:,} req/hour")
        print(f"   • Achieved: {target['achieved_requests_per_hour']:,} req/hour")
        print(f"   • Performance: {target['performance_ratio']}% of target")
        if target['target_achieved']:
            print(f"   • Status: ✅ TARGET EXCEEDED!")
        else:
            print(f"   • Status: ⚠️ Target not reached")
        
        # Individual scraper breakdown
        print(f"\n🔍 SCRAPER BREAKDOWN:")
        for scraper_name, result in report['scraper_breakdown'].items():
            if result.get('status') == 'failed':
                print(f"   • {scraper_name.upper()}: ❌ FAILED - {result.get('error', 'Unknown error')}")
            else:
                leads_saved = result.get('leads_saved', 0)
                runtime = result.get('execution_time', 0)
                req_hour = result.get('requests_per_hour', 0)
                print(f"   • {scraper_name.upper()}: ✅ {leads_saved:,} leads, {req_hour:,.0f} req/hr, {runtime:.1f}s")
        
        print("="*70)

async def main():
    """Main function to run the master async scraper"""
    
    logger.info("🌟 Starting Master Async Scraper System")
    
    try:
        # Initialize master scraper
        master = AsyncMasterScraper()
        
        # Get execution mode from environment (default: parallel)
        execution_mode = os.getenv('SCRAPER_EXECUTION_MODE', 'parallel').lower()
        
        if execution_mode == 'sequential':
            logger.info("🔄 Running in sequential mode")
            report = await master.run_scrapers_sequential()
        else:
            logger.info("⚡ Running in parallel mode (default)")
            report = await master.run_all_scrapers_parallel()
        
        # Save and display report
        await master.save_master_report(report)
        master.print_performance_summary(report)
        
        # Check if target was achieved
        if report['target_achievement']['target_achieved']:
            logger.info("🎉 SUCCESS: 5,000+ requests/hour target achieved!")
            exit_code = 0
        else:
            logger.warning("⚠️ Target not fully achieved, but scrapers completed")
            exit_code = 1
        
        return report, exit_code
        
    except Exception as e:
        logger.error(f"❌ Master scraper system failed: {e}")
        raise

if __name__ == "__main__":
    # Run the master async scraper system
    try:
        report, exit_code = asyncio.run(main())
        exit(exit_code)
    except KeyboardInterrupt:
        logger.info("🛑 Master scraper interrupted by user")
        exit(130)
    except Exception as e:
        logger.error(f"💥 Fatal error: {e}")
        exit(1)