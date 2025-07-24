#!/usr/bin/env python3
"""
DFW Comprehensive Logging and Export System
Provides unified logging, performance tracking, and result export functionality
for all DFW scrapers with thread-safe operations
"""

import os
import json
import csv
import logging
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union
from pathlib import Path
import gzip
import shutil

class DFWLoggingSystem:
    """Comprehensive logging system for DFW scraping operations"""
    
    def __init__(self, base_log_dir: str = "logs"):
        self.base_log_dir = Path(base_log_dir)
        self.base_log_dir.mkdir(exist_ok=True)
        
        # Thread safety
        self.lock = threading.Lock()
        
        # Performance tracking
        self.performance_metrics = {}
        self.scraper_stats = {}
        
        # Log rotation settings
        self.max_log_size_mb = 50
        self.max_log_files = 10
        
        # Initialize logging configuration
        self.setup_logging()
        
        self.logger = logging.getLogger(__name__)
        self.logger.info("🔧 DFW Logging System initialized")
        
    def setup_logging(self):
        """Setup comprehensive logging configuration"""
        
        # Create log directories
        (self.base_log_dir / "scrapers").mkdir(exist_ok=True)
        (self.base_log_dir / "performance").mkdir(exist_ok=True)
        (self.base_log_dir / "errors").mkdir(exist_ok=True)
        (self.base_log_dir / "archived").mkdir(exist_ok=True)
        
        # Configure root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO)
        
        # Clear existing handlers
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
        
        # Create formatters
        detailed_formatter = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(name)-20s | %(threadName)-10s | %(message)s'
        )
        
        simple_formatter = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(message)s'
        )
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(simple_formatter)
        root_logger.addHandler(console_handler)
        
        # Main log file handler
        main_log_file = self.base_log_dir / f"dfw_system_{datetime.now().strftime('%Y%m%d')}.log"
        file_handler = logging.FileHandler(main_log_file)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(detailed_formatter)
        root_logger.addHandler(file_handler)
        
        # Error log file handler
        error_log_file = self.base_log_dir / "errors" / f"errors_{datetime.now().strftime('%Y%m%d')}.log"
        error_handler = logging.FileHandler(error_log_file)
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(detailed_formatter)
        root_logger.addHandler(error_handler)
        
    def create_scraper_logger(self, scraper_name: str) -> logging.Logger:
        """Create dedicated logger for individual scrapers"""
        logger_name = f"dfw_scraper.{scraper_name}"
        logger = logging.getLogger(logger_name)
        
        # Create scraper-specific log file
        log_file = self.base_log_dir / "scrapers" / f"{scraper_name}_{datetime.now().strftime('%Y%m%d')}.log"
        
        # Avoid duplicate handlers
        if not logger.handlers:
            handler = logging.FileHandler(log_file)
            handler.setLevel(logging.DEBUG)
            
            formatter = logging.Formatter(
                '%(asctime)s | %(levelname)-8s | %(threadName)-10s | %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.DEBUG)
        
        return logger
    
    def log_scraper_start(self, scraper_name: str, config: Dict):
        """Log scraper startup with configuration"""
        with self.lock:
            start_time = datetime.now()
            
            self.scraper_stats[scraper_name] = {
                'start_time': start_time,
                'config': config.copy(),
                'leads_processed': 0,
                'dfw_leads': 0,
                'errors': 0,
                'performance_metrics': {}
            }
            
            logger = self.create_scraper_logger(scraper_name)
            logger.info(f"🚀 {scraper_name} STARTED")
            logger.info("=" * 60)
            logger.info(f"📊 Configuration: {json.dumps(config, indent=2)}")
            logger.info(f"⏰ Start Time: {start_time.isoformat()}")
            logger.info(f"🧵 Thread Count: {config.get('max_workers', 'Unknown')}")
            logger.info(f"🎯 Daily Limit: {config.get('daily_limit', 'Unknown')}")
            logger.info("=" * 60)
    
    def log_scraper_progress(self, scraper_name: str, leads_processed: int, dfw_leads: int, 
                           current_batch: int = 0, total_batches: int = 0):
        """Log scraper progress updates"""
        with self.lock:
            if scraper_name in self.scraper_stats:
                self.scraper_stats[scraper_name]['leads_processed'] = leads_processed
                self.scraper_stats[scraper_name]['dfw_leads'] = dfw_leads
                
                elapsed = datetime.now() - self.scraper_stats[scraper_name]['start_time']
                leads_per_minute = leads_processed / max(elapsed.total_seconds() / 60, 1)
                
                logger = self.create_scraper_logger(scraper_name)
                
                if total_batches > 0:
                    progress_pct = (current_batch / total_batches) * 100
                    logger.info(f"📈 Progress: {current_batch}/{total_batches} batches ({progress_pct:.1f}%)")
                
                logger.info(f"📊 Processed: {leads_processed} total, {dfw_leads} DFW ({leads_per_minute:.1f}/min)")
    
    def log_scraper_error(self, scraper_name: str, error: Exception, context: str = ""):
        """Log scraper errors with context"""
        with self.lock:
            if scraper_name in self.scraper_stats:
                self.scraper_stats[scraper_name]['errors'] += 1
            
            logger = self.create_scraper_logger(scraper_name)
            logger.error(f"❌ ERROR in {context}: {type(error).__name__}: {error}")
            
            # Also log to main error log
            main_logger = logging.getLogger(__name__)
            main_logger.error(f"❌ {scraper_name} error in {context}: {error}")
    
    def log_scraper_completion(self, scraper_name: str, final_stats: Dict):
        """Log scraper completion with final statistics"""
        with self.lock:
            if scraper_name not in self.scraper_stats:
                return
            
            end_time = datetime.now()
            start_time = self.scraper_stats[scraper_name]['start_time']
            total_runtime = end_time - start_time
            
            # Update final stats
            self.scraper_stats[scraper_name].update({
                'end_time': end_time,
                'total_runtime': total_runtime.total_seconds(),
                'final_stats': final_stats
            })
            
            logger = self.create_scraper_logger(scraper_name)
            logger.info("🏁 SCRAPER COMPLETION SUMMARY")
            logger.info("=" * 60)
            logger.info(f"⏰ Runtime: {total_runtime}")
            logger.info(f"📊 Total Leads: {final_stats.get('total_leads', 0)}")
            logger.info(f"🗺️ DFW Leads: {final_stats.get('dfw_leads', 0)}")
            logger.info(f"❌ Errors: {self.scraper_stats[scraper_name]['errors']}")
            logger.info(f"⚡ Throughput: {final_stats.get('leads_per_second', 0):.2f} leads/sec")
            logger.info("=" * 60)
            logger.info("✅ SCRAPER COMPLETED SUCCESSFULLY")
    
    def export_scraper_results(self, scraper_name: str, all_results: List[Dict], 
                             dfw_results: List[Dict], format: str = "both") -> Dict[str, str]:
        """Export scraper results to JSON and CSV files"""
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        export_dir = self.base_log_dir / "exports" / scraper_name
        export_dir.mkdir(parents=True, exist_ok=True)
        
        exported_files = {}
        
        try:
            # Prepare comprehensive export data
            export_data = {
                'scraper_name': scraper_name,
                'export_timestamp': datetime.now().isoformat(),
                'total_results': len(all_results),
                'dfw_results': len(dfw_results),
                'dfw_percentage': round(len(dfw_results) / len(all_results) * 100, 2) if all_results else 0,
                'stats': self.scraper_stats.get(scraper_name, {}),
                'all_results': all_results,
                'dfw_only_results': dfw_results
            }
            
            # Export to JSON
            if format in ["json", "both"]:
                json_file = export_dir / f"{scraper_name}_results_{timestamp}.json"
                with open(json_file, 'w', encoding='utf-8') as f:
                    json.dump(export_data, f, indent=2, default=str)
                exported_files['json'] = str(json_file)
                
            # Export DFW results to CSV
            if format in ["csv", "both"] and dfw_results:
                csv_file = export_dir / f"{scraper_name}_dfw_leads_{timestamp}.csv"
                
                # Get all possible fieldnames
                all_fields = set()
                for result in dfw_results:
                    all_fields.update(result.keys())
                
                with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=sorted(all_fields))
                    writer.writeheader()
                    writer.writerows(dfw_results)
                
                exported_files['csv'] = str(csv_file)
            
            # Create summary report
            summary_file = export_dir / f"{scraper_name}_summary_{timestamp}.txt"
            self._create_summary_report(scraper_name, export_data, summary_file)
            exported_files['summary'] = str(summary_file)
            
            logger = self.create_scraper_logger(scraper_name)
            logger.info(f"📂 Results exported: {len(exported_files)} files created")
            for file_type, file_path in exported_files.items():
                logger.info(f"   • {file_type.upper()}: {file_path}")
                
        except Exception as e:
            logger = self.create_scraper_logger(scraper_name)
            logger.error(f"❌ Export failed: {e}")
            
        return exported_files
    
    def _create_summary_report(self, scraper_name: str, export_data: Dict, file_path: Path):
        """Create human-readable summary report"""
        stats = export_data['stats']
        
        with open(file_path, 'w') as f:
            f.write(f"DFW {scraper_name.upper()} SCRAPER - EXECUTION SUMMARY\n")
            f.write("=" * 80 + "\n\n")
            
            f.write(f"📅 Execution Date: {export_data['export_timestamp'][:10]}\n")
            f.write(f"⏰ Start Time: {stats.get('start_time', 'Unknown')}\n")
            f.write(f"⏰ End Time: {stats.get('end_time', 'Unknown')}\n")
            f.write(f"⏱️ Total Runtime: {stats.get('total_runtime', 0):.2f} seconds\n\n")
            
            f.write("📊 PERFORMANCE METRICS\n")
            f.write("-" * 40 + "\n")
            f.write(f"Total Leads Processed: {export_data['total_results']}\n")
            f.write(f"DFW Leads: {export_data['dfw_results']}\n")
            f.write(f"DFW Percentage: {export_data['dfw_percentage']:.2f}%\n")
            f.write(f"Processing Errors: {stats.get('errors', 0)}\n")
            f.write(f"Thread Count: {stats.get('config', {}).get('max_workers', 'Unknown')}\n\n")
            
            if 'final_stats' in stats:
                f.write("🎯 FINAL STATISTICS\n")
                f.write("-" * 40 + "\n")
                final_stats = stats['final_stats']
                for key, value in final_stats.items():
                    f.write(f"{key.replace('_', '').title()}: {value}\n")
                f.write("\n")
            
            f.write("📂 EXPORTED FILES\n")
            f.write("-" * 40 + "\n")
            f.write(f"JSON Export: {export_data['total_results']} records\n")
            f.write(f"CSV Export: {export_data['dfw_results']} DFW records\n")
            f.write(f"Summary Report: This file\n\n")
            
            f.write("✅ SCRAPER EXECUTION COMPLETED SUCCESSFULLY\n")
    
    def get_system_performance_report(self) -> Dict:
        """Generate comprehensive system performance report"""
        
        total_leads = sum(stats.get('leads_processed', 0) for stats in self.scraper_stats.values())
        total_dfw = sum(stats.get('dfw_leads', 0) for stats in self.scraper_stats.values())
        total_errors = sum(stats.get('errors', 0) for stats in self.scraper_stats.values())
        
        # Calculate system-wide metrics
        active_scrapers = len([s for s in self.scraper_stats.values() if 'start_time' in s])
        completed_scrapers = len([s for s in self.scraper_stats.values() if 'end_time' in s])
        
        system_uptime = max([
            (datetime.now() - stats['start_time']).total_seconds()
            for stats in self.scraper_stats.values() 
            if 'start_time' in stats
        ], default=0)
        
        return {
            'report_timestamp': datetime.now().isoformat(),
            'system_metrics': {
                'total_leads_processed': total_leads,
                'total_dfw_leads': total_dfw,
                'total_errors': total_errors,
                'dfw_percentage': round(total_dfw / total_leads * 100, 2) if total_leads > 0 else 0,
                'system_uptime_seconds': system_uptime,
                'active_scrapers': active_scrapers,
                'completed_scrapers': completed_scrapers
            },
            'scraper_breakdown': {
                name: {
                    'leads_processed': stats.get('leads_processed', 0),
                    'dfw_leads': stats.get('dfw_leads', 0),
                    'errors': stats.get('errors', 0),
                    'runtime_seconds': stats.get('total_runtime', 0),
                    'status': 'completed' if 'end_time' in stats else 'running'
                }
                for name, stats in self.scraper_stats.items()
            },
            'log_files': {
                'main_log': self._get_current_log_files('main'),
                'error_log': self._get_current_log_files('error'),
                'scraper_logs': self._get_current_log_files('scrapers')
            }
        }
    
    def _get_current_log_files(self, log_type: str) -> List[str]:
        """Get list of current log files by type"""
        try:
            if log_type == 'main':
                pattern = f"dfw_system_{datetime.now().strftime('%Y%m%d')}*.log"
                return [str(f) for f in self.base_log_dir.glob(pattern)]
            elif log_type == 'error':
                pattern = f"errors_{datetime.now().strftime('%Y%m%d')}*.log"
                return [str(f) for f in (self.base_log_dir / "errors").glob(pattern)]
            elif log_type == 'scrapers':
                pattern = f"*_{datetime.now().strftime('%Y%m%d')}*.log"
                return [str(f) for f in (self.base_log_dir / "scrapers").glob(pattern)]
        except Exception:
            return []
    
    def export_system_report(self) -> str:
        """Export comprehensive system performance report"""
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_file = self.base_log_dir / f"system_performance_report_{timestamp}.json"
        
        try:
            report = self.get_system_performance_report()
            
            with open(report_file, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            
            self.logger.info(f"📋 System performance report exported: {report_file}")
            return str(report_file)
            
        except Exception as e:
            self.logger.error(f"❌ Failed to export system report: {e}")
            return ""
    
    def rotate_logs(self):
        """Rotate old log files to prevent disk space issues"""
        try:
            current_date = datetime.now().strftime('%Y%m%d')
            
            # Find log files older than 7 days
            cutoff_date = datetime.now() - timedelta(days=7)
            
            for log_dir in [self.base_log_dir, self.base_log_dir / "scrapers", self.base_log_dir / "errors"]:
                for log_file in log_dir.glob("*.log"):
                    file_date = datetime.fromtimestamp(log_file.stat().st_mtime)
                    
                    if file_date < cutoff_date:
                        # Compress and move to archived directory
                        archived_file = self.base_log_dir / "archived" / f"{log_file.name}.gz"
                        
                        with open(log_file, 'rb') as f_in:
                            with gzip.open(archived_file, 'wb') as f_out:
                                shutil.copyfileobj(f_in, f_out)
                        
                        log_file.unlink()  # Remove original
                        self.logger.info(f"📦 Archived log file: {log_file.name}")
            
        except Exception as e:
            self.logger.error(f"❌ Log rotation failed: {e}")

# Global logging system instance
dfw_logging = DFWLoggingSystem()

# Convenience functions
def get_scraper_logger(scraper_name: str) -> logging.Logger:
    """Get dedicated logger for a scraper"""
    return dfw_logging.create_scraper_logger(scraper_name)

def log_scraper_start(scraper_name: str, config: Dict):
    """Log scraper startup"""
    dfw_logging.log_scraper_start(scraper_name, config)

def log_scraper_progress(scraper_name: str, leads_processed: int, dfw_leads: int, 
                       current_batch: int = 0, total_batches: int = 0):
    """Log scraper progress"""
    dfw_logging.log_scraper_progress(scraper_name, leads_processed, dfw_leads, current_batch, total_batches)

def log_scraper_error(scraper_name: str, error: Exception, context: str = ""):
    """Log scraper error"""
    dfw_logging.log_scraper_error(scraper_name, error, context)

def log_scraper_completion(scraper_name: str, final_stats: Dict):
    """Log scraper completion"""
    dfw_logging.log_scraper_completion(scraper_name, final_stats)

def export_scraper_results(scraper_name: str, all_results: List[Dict], 
                         dfw_results: List[Dict], format: str = "both") -> Dict[str, str]:
    """Export scraper results"""
    return dfw_logging.export_scraper_results(scraper_name, all_results, dfw_results, format)

def export_system_report() -> str:
    """Export system performance report"""
    return dfw_logging.export_system_report()

if __name__ == "__main__":
    # Test the logging system
    logging_system = DFWLoggingSystem()
    
    # Simulate scraper operations
    test_config = {
        'max_workers': 5,
        'daily_limit': 3000,
        'scraper_api_key': 'test_key'
    }
    
    # Test logging workflow
    logging_system.log_scraper_start('test_scraper', test_config)
    logging_system.log_scraper_progress('test_scraper', 50, 35, 1, 5)
    logging_system.log_scraper_progress('test_scraper', 150, 105, 3, 5)
    
    test_stats = {
        'total_leads': 200,
        'dfw_leads': 140,
        'leads_per_second': 2.5,
        'success_rate': 95.0
    }
    
    logging_system.log_scraper_completion('test_scraper', test_stats)
    
    # Export system report
    report_file = logging_system.export_system_report()
    print(f"System report exported: {report_file}")