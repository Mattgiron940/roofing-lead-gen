#!/usr/bin/env python3
"""
High-Performance Async Scraper Framework
Supports 5,000+ requests/hour with asyncio, aiohttp, ScraperAPI rotation,
intelligent retries, incremental fetching, and direct Supabase integration
"""

import asyncio
import aiohttp
import json
import time
import hashlib
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Any, Tuple
from dataclasses import dataclass, asdict
from abc import ABC, abstractmethod
import os
from itertools import cycle
import random
from pathlib import Path

from supabase_client import supabase
from dfw_geo_filter import dfw_filter
from lead_limit_controller import lead_controller

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class ScraperConfig:
    """Configuration for async scraper instances"""
    name: str
    max_concurrent: int = 100
    requests_per_hour: int = 5000
    scraperapi_keys: List[str] = None
    retry_attempts: int = 3
    retry_backoff: float = 1.0
    timeout: float = 30.0
    rate_limit_delay: float = 0.1
    incremental_enabled: bool = True
    cache_duration_hours: int = 24
    
    def __post_init__(self):
        if self.scraperapi_keys is None:
            # Load from environment
            keys_str = os.getenv('SCRAPERAPI_KEYS', os.getenv('SCRAPERAPI_KEY', ''))
            self.scraperapi_keys = [k.strip() for k in keys_str.split(',') if k.strip()]
        
        if not self.scraperapi_keys:
            raise ValueError("No ScraperAPI keys provided")

@dataclass
class RequestResult:
    """Result from an async request"""
    url: str
    success: bool
    data: Optional[str] = None
    error: Optional[str] = None
    status_code: Optional[int] = None
    attempt: int = 1
    duration: float = 0.0
    scraperapi_key: Optional[str] = None

class AsyncScraperFramework:
    """High-performance async scraper with ScraperAPI integration"""
    
    def __init__(self, config: ScraperConfig):
        self.config = config
        self.scraperapi_keys = cycle(config.scraperapi_keys)
        self.current_key = next(self.scraperapi_keys)
        
        # Rate limiting
        self.semaphore = asyncio.Semaphore(config.max_concurrent)
        self.rate_limiter = asyncio.Semaphore(config.requests_per_hour)
        
        # Statistics
        self.stats = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'cache_hits': 0,
            'scraperapi_rotations': 0,
            'start_time': datetime.now(),
            'key_usage': {key: 0 for key in config.scraperapi_keys}
        }
        
        # Incremental fetching cache
        self.url_cache = {}
        self.cache_file = Path(f"{config.name}_cache.json")
        self.load_cache()
        
        logger.info(f"🚀 Async scraper {config.name} initialized")
        logger.info(f"📊 Config: {config.max_concurrent} concurrent, {config.requests_per_hour}/hour")
        logger.info(f"🔑 ScraperAPI keys: {len(config.scraperapi_keys)} available")
    
    def load_cache(self):
        """Load incremental fetching cache from disk"""
        try:
            if self.cache_file.exists():
                with open(self.cache_file, 'r') as f:
                    cache_data = json.load(f)
                    
                # Filter expired entries
                cutoff_time = time.time() - (self.config.cache_duration_hours * 3600)
                self.url_cache = {
                    url: data for url, data in cache_data.items()
                    if data.get('timestamp', 0) > cutoff_time
                }
                
                logger.info(f"📂 Loaded {len(self.url_cache)} cached entries")
        except Exception as e:
            logger.warning(f"⚠️ Could not load cache: {e}")
            self.url_cache = {}
    
    def save_cache(self):
        """Save incremental fetching cache to disk"""
        try:
            with open(self.cache_file, 'w') as f:
                json.dump(self.url_cache, f)
        except Exception as e:
            logger.error(f"❌ Could not save cache: {e}")
    
    def get_next_scraperapi_key(self) -> str:
        """Get next ScraperAPI key with rotation"""
        self.current_key = next(self.scraperapi_keys)
        self.stats['scraperapi_rotations'] += 1
        return self.current_key
    
    def should_fetch_url(self, url: str, force_refresh: bool = False) -> bool:
        """Determine if URL should be fetched based on incremental logic"""
        if not self.config.incremental_enabled or force_refresh:
            return True
        
        if url not in self.url_cache:
            return True
        
        cache_entry = self.url_cache[url]
        cache_age = time.time() - cache_entry.get('timestamp', 0)
        
        # Refresh if cache is expired
        if cache_age > (self.config.cache_duration_hours * 3600):
            return True
        
        return False
    
    def update_cache(self, url: str, content_hash: str, data: Any = None):
        """Update cache with new content hash and data"""
        self.url_cache[url] = {
            'content_hash': content_hash,
            'timestamp': time.time(),
            'data': data
        }
    
    async def fetch_with_scraperapi(self, session: aiohttp.ClientSession, url: str) -> RequestResult:
        """Fetch URL using ScraperAPI with rotation and retry logic"""
        
        for attempt in range(1, self.config.retry_attempts + 1):
            api_key = self.current_key
            self.stats['key_usage'][api_key] += 1
            
            scraperapi_url = "http://api.scraperapi.com"
            params = {
                'api_key': api_key,
                'url': url,
                'render': 'false'  # Set to 'true' if JavaScript rendering needed
            }
            
            start_time = time.time()
            
            try:
                # Apply rate limiting
                async with self.rate_limiter:
                    async with session.get(scraperapi_url, params=params, timeout=self.config.timeout) as response:
                        duration = time.time() - start_time
                        
                        if response.status == 200:
                            content = await response.text()
                            self.stats['successful_requests'] += 1
                            
                            return RequestResult(
                                url=url,
                                success=True,
                                data=content,
                                status_code=response.status,
                                attempt=attempt,
                                duration=duration,
                                scraperapi_key=api_key
                            )
                        else:
                            logger.warning(f"⚠️ {self.config.name} attempt {attempt}: HTTP {response.status} for {url}")
                            
            except asyncio.TimeoutError:
                logger.warning(f"⚠️ {self.config.name} attempt {attempt}: Timeout for {url}")
            except Exception as e:
                logger.warning(f"⚠️ {self.config.name} attempt {attempt}: {type(e).__name__}: {e}")
            
            # Rotate key and apply backoff before retry
            if attempt < self.config.retry_attempts:
                self.get_next_scraperapi_key()
                backoff_delay = self.config.retry_backoff * (2 ** (attempt - 1)) + random.uniform(0, 1)
                await asyncio.sleep(backoff_delay)
        
        # All attempts failed
        duration = time.time() - start_time
        self.stats['failed_requests'] += 1
        
        return RequestResult(
            url=url,
            success=False,
            error=f"Failed after {self.config.retry_attempts} attempts",
            attempt=self.config.retry_attempts,
            duration=duration
        )
    
    async def process_url(self, session: aiohttp.ClientSession, url: str, processor_func) -> Optional[List[Dict]]:
        """Process single URL with incremental fetching and data extraction"""
        
        async with self.semaphore:
            self.stats['total_requests'] += 1
            
            # Check if we should fetch this URL
            if not self.should_fetch_url(url):
                self.stats['cache_hits'] += 1
                logger.debug(f"📂 Cache hit for {url}")
                return self.url_cache[url].get('data', [])
            
            # Fetch URL content
            result = await self.fetch_with_scraperapi(session, url)
            
            if not result.success:
                logger.error(f"❌ Failed to fetch {url}: {result.error}")
                return []
            
            # Process content to extract data
            try:
                extracted_data = await processor_func(result.data, url)
                
                # Update cache with content hash
                content_hash = hashlib.md5(result.data.encode()).hexdigest()
                self.update_cache(url, content_hash, extracted_data)
                
                logger.debug(f"✅ Processed {url}: {len(extracted_data)} items extracted")
                return extracted_data
                
            except Exception as e:
                logger.error(f"❌ Error processing {url}: {e}")
                return []
    
    async def batch_process_urls(self, urls: List[str], processor_func, batch_size: int = None) -> List[Dict]:
        """Process multiple URLs concurrently with batching"""
        
        if batch_size is None:
            batch_size = self.config.max_concurrent
        
        all_results = []
        
        # Create aiohttp session with optimized settings
        connector = aiohttp.TCPConnector(
            limit=self.config.max_concurrent * 2,
            limit_per_host=50,
            ttl_dns_cache=300,
            use_dns_cache=True
        )
        
        timeout = aiohttp.ClientTimeout(total=self.config.timeout)
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            # Process URLs in batches to manage memory
            for i in range(0, len(urls), batch_size):
                batch_urls = urls[i:i + batch_size]
                
                logger.info(f"🔄 Processing batch {i//batch_size + 1}: {len(batch_urls)} URLs")
                
                # Create tasks for concurrent processing
                tasks = [
                    self.process_url(session, url, processor_func)
                    for url in batch_urls
                ]
                
                # Execute batch concurrently
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Collect successful results
                for result in batch_results:
                    if isinstance(result, list):
                        all_results.extend(result)
                    elif isinstance(result, Exception):
                        logger.error(f"❌ Batch processing error: {result}")
                
                # Rate limiting between batches
                if i + batch_size < len(urls):
                    await asyncio.sleep(self.config.rate_limit_delay)
        
        # Save cache after processing
        self.save_cache()
        
        return all_results
    
    async def save_leads_to_supabase(self, leads: List[Dict], table_name: str) -> int:
        """Save leads to Supabase with DFW filtering and deduplication"""
        
        if not leads:
            return 0
        
        saved_count = 0
        
        for lead in leads:
            try:
                # Check daily lead limit
                if not lead_controller.can_process_lead(self.config.name):
                    logger.warning(f"⚠️ Daily lead limit reached for {self.config.name}")
                    break
                
                # Apply DFW filtering
                is_dfw = dfw_filter.is_dfw_lead(lead)
                lead['dfw'] = is_dfw
                
                # Save to Supabase with deduplication
                if supabase.insert_lead_with_deduplication(table_name, lead):
                    saved_count += 1
                    
                    # Update lead controller
                    lead_controller.increment_lead_count(self.config.name, is_dfw)
                    
                    if saved_count % 100 == 0:
                        logger.info(f"📊 Saved {saved_count} leads to {table_name}")
                
            except Exception as e:
                logger.error(f"❌ Error saving lead to Supabase: {e}")
        
        logger.info(f"✅ Saved {saved_count}/{len(leads)} leads to {table_name}")
        return saved_count
    
    def get_performance_stats(self) -> Dict:
        """Get comprehensive performance statistics"""
        
        elapsed_time = (datetime.now() - self.stats['start_time']).total_seconds()
        requests_per_second = self.stats['total_requests'] / max(elapsed_time, 1)
        success_rate = (self.stats['successful_requests'] / max(self.stats['total_requests'], 1)) * 100
        
        return {
            'scraper_name': self.config.name,
            'runtime_seconds': elapsed_time,
            'total_requests': self.stats['total_requests'],
            'successful_requests': self.stats['successful_requests'],
            'failed_requests': self.stats['failed_requests'],
            'cache_hits': self.stats['cache_hits'],
            'success_rate_percent': round(success_rate, 2),
            'requests_per_second': round(requests_per_second, 2),
            'requests_per_hour': round(requests_per_second * 3600, 0),
            'scraperapi_rotations': self.stats['scraperapi_rotations'],
            'key_usage_distribution': self.stats['key_usage'],
            'cache_entries': len(self.url_cache),
            'configuration': {
                'max_concurrent': self.config.max_concurrent,
                'target_requests_per_hour': self.config.requests_per_hour,
                'scraperapi_keys_count': len(self.config.scraperapi_keys),
                'incremental_enabled': self.config.incremental_enabled,
                'retry_attempts': self.config.retry_attempts
            }
        }

class BaseAsyncScraper(ABC):
    """Abstract base class for async scrapers"""
    
    def __init__(self, config: ScraperConfig):
        self.framework = AsyncScraperFramework(config)
        self.config = config
    
    @abstractmethod
    async def generate_urls(self) -> List[str]:
        """Generate list of URLs to scrape"""
        pass
    
    @abstractmethod
    async def extract_data(self, content: str, url: str) -> List[Dict]:
        """Extract structured data from page content"""
        pass
    
    @abstractmethod
    def get_supabase_table(self) -> str:
        """Get Supabase table name for this scraper"""
        pass
    
    async def run_scraper(self) -> Dict:
        """Run the complete scraping process"""
        
        logger.info(f"🚀 Starting {self.config.name} async scraper")
        start_time = time.time()
        
        try:
            # Generate URLs to scrape
            urls = await self.generate_urls()
            logger.info(f"📍 Generated {len(urls)} URLs for {self.config.name}")
            
            if not urls:
                logger.warning(f"⚠️ No URLs generated for {self.config.name}")
                return self.framework.get_performance_stats()
            
            # Process URLs and extract data
            extracted_data = await self.framework.batch_process_urls(urls, self.extract_data)
            logger.info(f"📊 Extracted {len(extracted_data)} records from {self.config.name}")
            
            # Save to Supabase
            table_name = self.get_supabase_table()
            saved_count = await self.framework.save_leads_to_supabase(extracted_data, table_name)
            
            # Generate performance report
            stats = self.framework.get_performance_stats()
            stats['leads_extracted'] = len(extracted_data)
            stats['leads_saved'] = saved_count
            stats['total_runtime'] = time.time() - start_time
            
            logger.info(f"✅ {self.config.name} completed: {saved_count} leads saved in {stats['total_runtime']:.1f}s")
            
            return stats
            
        except Exception as e:
            logger.error(f"❌ {self.config.name} scraper failed: {e}")
            raise

# Utility functions for common scraping tasks
async def create_scraperapi_session(max_concurrent: int = 100) -> aiohttp.ClientSession:
    """Create optimized aiohttp session for ScraperAPI requests"""
    
    connector = aiohttp.TCPConnector(
        limit=max_concurrent * 2,
        limit_per_host=50,
        ttl_dns_cache=300,
        use_dns_cache=True,
        enable_cleanup_closed=True
    )
    
    timeout = aiohttp.ClientTimeout(total=30.0, connect=10.0)
    
    return aiohttp.ClientSession(
        connector=connector,
        timeout=timeout,
        headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
    )

def calculate_optimal_batch_size(total_urls: int, max_concurrent: int, target_requests_per_hour: int) -> int:
    """Calculate optimal batch size for given constraints"""
    
    # Aim for batches that complete in 5-10 minutes
    target_batch_time = 300  # 5 minutes
    estimated_time_per_request = 3.0  # seconds
    
    optimal_batch = min(
        max_concurrent * 2,  # Don't exceed 2x concurrency
        target_requests_per_hour // 12,  # 12 batches per hour max
        total_urls // 4,  # At least 4 batches total
        max(50, total_urls // 20)  # Minimum 50, or 1/20th of total
    )
    
    return max(10, optimal_batch)  # Minimum batch size of 10

if __name__ == "__main__":
    # Test the async framework
    async def test_framework():
        config = ScraperConfig(
            name="test_scraper",
            max_concurrent=50,
            requests_per_hour=1000,
            scraperapi_keys=[os.getenv('SCRAPERAPI_KEY', 'test_key')]
        )
        
        framework = AsyncScraperFramework(config)
        
        async def dummy_processor(content: str, url: str) -> List[Dict]:
            return [{'url': url, 'content_length': len(content), 'timestamp': datetime.now().isoformat()}]
        
        # Test with a few URLs
        test_urls = [
            'https://httpbin.org/delay/1',
            'https://httpbin.org/user-agent',
            'https://httpbin.org/headers'
        ]
        
        results = await framework.batch_process_urls(test_urls, dummy_processor)
        stats = framework.get_performance_stats()
        
        print(f"✅ Test completed: {len(results)} results")
        print(f"📊 Performance: {stats['requests_per_second']:.2f} req/sec")
        print(f"🎯 Success rate: {stats['success_rate_percent']:.1f}%")
    
    # Run test
    asyncio.run(test_framework())