import apache_beam as beam
import numpy as np
import pandas as pd
from typing import Dict, List, Iterator
from dataclasses import dataclass

@dataclass
class QueryMetrics:
    """Container for query performance metrics"""
    query_type: str
    total_queries: int
    successful_queries: int
    failed_queries: int
    avg_execution_time: float
    median_execution_time: float
    p95_execution_time: float
    p99_execution_time: float
    min_execution_time: float
    max_execution_time: float

class MetricsCalculatorDoFn(beam.DoFn):
    """Calculate performance metrics for each query type"""
    
    def process(self, element: Tuple[str, List[Dict]]) -> Iterator[QueryMetrics]:
        """
        Process metrics for a group of query results
        
        Args:
            element: Tuple of (query_type, list of results)
            
        Yields:
            QueryMetrics object with calculated statistics
        """
        query_type, results = element
        
        # Extract execution times for successful queries
        execution_times = [
            r['execution_time_ms'] 
            for r in results 
            if r['status'] == 'success'
        ]
        
        if not execution_times:
            logging.warning(f"No successful queries for type {query_type}")
            return
        
        metrics = QueryMetrics(
            query_type=query_type,
            total_queries=len(results),
            successful_queries=len(execution_times),
            failed_queries=len(results) - len(execution_times),
            avg_execution_time=np.mean(execution_times),
            median_execution_time=np.median(execution_times),
            p95_execution_time=np.percentile(execution_times, 95),
            p99_execution_time=np.percentile(execution_times, 99),
            min_execution_time=np.min(execution_times),
            max_execution_time=np.max(execution_times)
        )
        
        yield metrics

class ResultsProcessor:
    """Process and analyze query execution results"""
    
    def __init__(self):
        self.results_df = None
    
    def process_metrics(self, metrics_list: List[QueryMetrics]) -> pd.DataFrame:
        """
        Convert metrics into a DataFrame for analysis
        
        Args:
            metrics_list: List of QueryMetrics objects
            
        Returns:
            pandas.DataFrame: Processed metrics
        """
        self.results_df = pd.DataFrame([
            {
                'Query Type': m.query_type,
                'Total Queries': m.total_queries,
                'Success Rate': m.successful_queries / m.total_queries * 100,
                'Avg Time (ms)': m.avg_execution_time,
                'Median Time (ms)': m.median_execution_time,
                'P95 Time (ms)': m.p95_execution_time,
                'P99 Time (ms)': m.p99_execution_time,
                'Min Time (ms)': m.min_execution_time,
                'Max Time (ms)': m.max_execution_time
            }
            for m in metrics_list
        ])
        
        return self.results_df
    
    def get_summary_report(self) -> str:
        """
        Generate a text summary of the performance results
        
        Returns:
            str: Formatted summary report
        """
        if self.results_df is None:
            return "No results available"
        
        summary = []
        summary.append("Performance Test Summary")
        summary.append("=" * 50)
        
        for _, row in self.results_df.iterrows():
            summary.append(f"\nQuery Type: {row['Query Type']}")
            summary.append(f"Success Rate: {row['Success Rate']:.2f}%")
            summary.append(f"Latency Statistics (ms):")
            summary.append(f"  Average: {row['Avg Time (ms)']:.2f}")
            summary.append(f"  Median:  {row['Median Time (ms)']:.2f}")
            summary.append(f"  P95:     {row['P95 Time (ms)']:.2f}")
            summary.append(f"  P99:     {row['P99 Time (ms)']:.2f}")
            summary.append(f"  Range:   {row['Min Time (ms)']:.2f} - {row['Max Time (ms)']:.2f}")
        
        return "\n".join(summary)

def create_visualizations(results_df: pd.DataFrame) -> Dict[str, Any]:
    """
    Create visualization specifications for the results
    
    Args:
        results_df: DataFrame containing performance metrics
        
    Returns:
        Dictionary containing visualization specifications
    """
    # We'll implement visualizations later using a visualization library
    pass