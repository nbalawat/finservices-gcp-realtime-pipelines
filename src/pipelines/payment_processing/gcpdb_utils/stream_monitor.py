"""Monitor BigQuery streaming inserts in real-time."""

from google.cloud import bigquery
import logging
from datetime import datetime, timedelta
import time
from typing import Dict, List
from collections import defaultdict
import json
from colorama import init, Fore, Style

# Initialize colorama for colored output
init()

class StreamMonitor:
    def __init__(self, project_id='agentic-experiments-446019', 
                 dataset_id='pipeline_data_test',
                 table_id='payments'):
        self.client = bigquery.Client(project=project_id)
        self.table_id = f"{project_id}.{dataset_id}.{table_id}"
        self.last_count = 0
        self.start_time = datetime.now()
        
        # Initialize counters
        self.total_records = 0
        self.last_timestamp = None

    def get_detailed_stats(self) -> Dict[str, any]:
        """Get detailed statistics for recent records"""
        query = f"""
        WITH recent_data AS (
            SELECT *,
                JSON_EXTRACT_SCALAR(metadata, '$.channel') as channel,
                JSON_EXTRACT_SCALAR(metadata, '$.region') as region,
                CAST(JSON_EXTRACT_SCALAR(metadata, '$.processing_time') AS FLOAT64) as processing_time,
                CAST(JSON_EXTRACT_SCALAR(metadata, '$.retry_count') AS INT64) as retry_count
            FROM `{self.table_id}`
            WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 MINUTE)
        )
        SELECT
            COUNT(*) as total_records,
            COALESCE(AVG(amount), 0) as avg_amount,
            COALESCE(MIN(amount), 0) as min_amount,
            COALESCE(MAX(amount), 0) as max_amount,
            COUNT(DISTINCT payment_type) as unique_payment_types,
            COUNT(DISTINCT customer_id) as unique_customers,
            COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END) as completed_count,
            COUNT(CASE WHEN status = 'PENDING' THEN 1 END) as pending_count,
            COUNT(CASE WHEN status = 'FAILED' THEN 1 END) as failed_count,
            COALESCE(AVG(processing_time), 0) as avg_processing_time,
            COALESCE(AVG(retry_count), 0) as avg_retry_count,
            STRING_AGG(DISTINCT payment_type) as payment_types,
            STRING_AGG(DISTINCT currency) as currencies,
            STRING_AGG(DISTINCT channel) as channels,
            STRING_AGG(DISTINCT region) as regions
        FROM recent_data
        """
        
        try:
            query_job = self.client.query(query)
            results = list(query_job.result())
            stats = dict(results[0])
            
            # Convert None values to appropriate defaults
            stats = {k: (v if v is not None else '') for k, v in stats.items()}
            stats['total_records'] = stats.get('total_records', 0)
            stats['avg_amount'] = float(stats.get('avg_amount', 0))
            stats['min_amount'] = float(stats.get('min_amount', 0))
            stats['max_amount'] = float(stats.get('max_amount', 0))
            stats['completed_count'] = int(stats.get('completed_count', 0))
            stats['pending_count'] = int(stats.get('pending_count', 0))
            stats['failed_count'] = int(stats.get('failed_count', 0))
            stats['avg_processing_time'] = float(stats.get('avg_processing_time', 0))
            stats['avg_retry_count'] = float(stats.get('avg_retry_count', 0))
            
            return stats
        except Exception as e:
            logging.error(f"Query failed: {str(e)}")
            return {
                'total_records': 0,
                'avg_amount': 0.0,
                'min_amount': 0.0,
                'max_amount': 0.0,
                'unique_payment_types': 0,
                'unique_customers': 0,
                'completed_count': 0,
                'pending_count': 0,
                'failed_count': 0,
                'avg_processing_time': 0.0,
                'avg_retry_count': 0.0,
                'payment_types': '',
                'currencies': '',
                'channels': '',
                'regions': ''
            }

    def format_stat(self, name: str, value: any, color: str = Fore.WHITE) -> str:
        """Format a statistic with color"""
        return f"{color}{name}: {Style.BRIGHT}{value}{Style.RESET_ALL}"

    def monitor(self, interval: int = 5):
        """Monitor streaming inserts"""
        print(f"\n{Fore.CYAN}=== BigQuery Stream Monitor ==={Style.RESET_ALL}")
        print(f"Monitoring table: {Fore.YELLOW}{self.table_id}{Style.RESET_ALL}")
        print(f"Update interval: {interval} seconds")
        print(f"{Fore.RED}Press Ctrl+C to stop{Style.RESET_ALL}\n")
        
        try:
            while True:
                stats = self.get_detailed_stats()
                current_time = datetime.now()
                elapsed = (current_time - self.start_time).total_seconds()
                
                # Calculate rates
                records_per_minute = stats['total_records']
                records_per_second = records_per_minute / 60
                
                # Update total records
                if self.last_count == 0:
                    self.last_count = stats['total_records']
                new_records = stats['total_records'] - self.last_count
                self.last_count = stats['total_records']
                
                # Clear screen (comment out if you want to keep history)
                print("\033[2J\033[H")
                
                print(f"\n{Fore.CYAN}=== Stream Statistics at {current_time.strftime('%H:%M:%S')} ==={Style.RESET_ALL}")
                print(f"Elapsed time: {elapsed:.0f} seconds")
                
                print(f"\n{Fore.GREEN}Traffic Statistics:{Style.RESET_ALL}")
                print(self.format_stat("Records in last minute", stats['total_records'], Fore.WHITE))
                print(self.format_stat("Rate", f"{records_per_second:.2f} records/second", Fore.WHITE))
                print(self.format_stat("New records", f"+{new_records}", Fore.GREEN))
                
                print(f"\n{Fore.GREEN}Payment Statistics:{Style.RESET_ALL}")
                print(self.format_stat("Average amount", f"${stats['avg_amount']:.2f}", Fore.WHITE))
                print(self.format_stat("Min amount", f"${stats['min_amount']:.2f}", Fore.WHITE))
                print(self.format_stat("Max amount", f"${stats['max_amount']:.2f}", Fore.WHITE))
                
                print(f"\n{Fore.GREEN}Status Breakdown:{Style.RESET_ALL}")
                total = stats['completed_count'] + stats['pending_count'] + stats['failed_count']
                if total > 0:
                    completed_pct = (stats['completed_count'] / total) * 100
                    pending_pct = (stats['pending_count'] / total) * 100
                    failed_pct = (stats['failed_count'] / total) * 100
                    print(self.format_stat("Completed", f"{stats['completed_count']} ({completed_pct:.1f}%)", Fore.GREEN))
                    print(self.format_stat("Pending", f"{stats['pending_count']} ({pending_pct:.1f}%)", Fore.YELLOW))
                    print(self.format_stat("Failed", f"{stats['failed_count']} ({failed_pct:.1f}%)", Fore.RED))
                
                print(f"\n{Fore.GREEN}Performance:{Style.RESET_ALL}")
                print(self.format_stat("Avg processing time", f"{stats['avg_processing_time']:.2f}s", Fore.WHITE))
                print(self.format_stat("Avg retry count", f"{stats['avg_retry_count']:.2f}", Fore.WHITE))
                
                print(f"\n{Fore.GREEN}Diversity:{Style.RESET_ALL}")
                print(self.format_stat("Unique customers", stats['unique_customers'], Fore.WHITE))
                if stats['payment_types']: print(self.format_stat("Payment types", stats['payment_types'], Fore.WHITE))
                if stats['currencies']: print(self.format_stat("Currencies", stats['currencies'], Fore.WHITE))
                if stats['channels']: print(self.format_stat("Channels", stats['channels'], Fore.WHITE))
                if stats['regions']: print(self.format_stat("Regions", stats['regions'], Fore.WHITE))
                
                time.sleep(interval)
                
        except KeyboardInterrupt:
            print(f"\n{Fore.YELLOW}Monitoring stopped by user{Style.RESET_ALL}")
        except Exception as e:
            print(f"\n{Fore.RED}Monitoring error: {str(e)}{Style.RESET_ALL}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    monitor = StreamMonitor()
    monitor.monitor(interval=5)
