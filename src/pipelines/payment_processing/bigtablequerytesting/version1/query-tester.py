import asyncio
from typing import Dict, List, Any, Callable
import time
from dataclasses import dataclass
import pandas as pd
import logging

@dataclass
class QueryTestResult:
    table_name: str
    query_type: str
    execution_time_ms: float
    row_count: int
    success: bool
    error: Optional[str] = None

class QueryTester:
    """Framework for testing queries across different table structures"""
    
    def __init__(self, tables: Dict[str, Table]):
        self.tables = tables
        self.customer_queries = CustomerQueries()
        self.generic_queries = GenericQueries()
        self.analytics_queries = AnalyticsQueries()
        self.results: List[QueryTestResult] = []

    async def test_query(self, query_func: Callable, 
                        params: Dict[str, Any]) -> List[QueryTestResult]:
        """Test a single query implementation across all tables"""
        implementations = query_func()
        results = []

        for table_name, implementation in implementations.items():
            table = self.tables[table_name]
            
            try:
                start_time = time.time()
                rows = await implementation(table, **params)
                execution_time = (time.time() - start_time) * 1000
                
                result = QueryTestResult(
                    table_name=table_name,
                    query_type=query_func.__name__,
                    execution_time_ms=execution_time,
                    row_count=len(rows) if isinstance(rows, list) else 0,
                    success=True
                )
                
            except Exception as e:
                result = QueryTestResult(
                    table_name=table_name,
                    query_type=query_func.__name__,
                    execution_time_ms=0,
                    row_count=0,
                    success=False,
                    error=str(e)
                )
            
            results.append(result)
            self.results.append(result)
        
        return results

    async def run_comprehensive_test(self, iterations: int = 5) -> pd.DataFrame:
        """Run all query tests multiple times"""
        test_params = {
            # Customer queries
            self.customer_queries.customer_daily_type_transactions: {
                "customer_id": "CUST123",
                "transaction_date": "2024-03-15",
                "payment_type": "ACH"
            },
            self.customer_queries.customer_date_range_transactions: {
                "customer_id": "CUST123",
                "start_date": "2024-03-01",
                "end_date": "2024-03-31"
            },
            # Generic queries
            self.generic_queries.transaction_batch_lookup: {
                "transaction_ids": ["TX001", "TX002", "TX003"]
            },
            self.generic_queries.daily_customer_activity: {
                "transaction_date": "2024-03-15"
            },
            # Analytics queries
            self.analytics_queries.date_range_customer_activity: {
                "start_date": "2024-03-01",
                "end_date": "2024-03-31"
            },
            self.analytics_queries.payment_type_analysis: {
                "start_date": "2024-03-01",
                "end_date": "2024-03-31",
                "payment_type": "ACH"
            }
        }

        for _ in range(iterations):
            for query_func, params in test_params.items():
                await self.test_query(query_func, params)

        return self.get_summary_stats()

    def get_summary_stats(self) -> pd.DataFrame:
        """Generate summary statistics for test results"""
        summaries = []
        
        for query_type in set(r.query_type for r in self.results):
            query_results = [r for r in self.results if r.query_type == query_type]
            
            for table in set(r.table_name for r in query_results):
                table_results = [r for r in query_results if r.table_name == table]
                successful_results = [r for r in table_results if r.success]
                
                if successful_results:
                    execution_times = [r.execution_time_ms for r in successful_results]
                    
                    summaries.append({
                        'Query Type': query_type,
                        'Table': table,
                        'Success Rate': len(successful_results) / len(table_results) * 100,
                        'Avg Time (ms)': sum(execution_times) / len(execution_times),
                        'Min Time (ms)': min(execution_times),
                        'Max Time (ms)': max(execution_times),
                        'Avg Row Count': sum(r.row_count for r in successful_results) / len(successful_results)
                    })
                else:
                    summaries.append({
                        'Query Type': query_type,
                        'Table': table,
                        'Success Rate': 0,
                        'Avg Time (ms)': None,
                        'Min Time (ms)': None,
                        'Max Time (ms)': None,
                        'Avg Row Count': 0
                    })
        
        return pd.DataFrame(summaries)

async def main():
    """Main function to run query tests"""
    # Initialize BigTable client and tables
    instance = await create_bigtable_client()
    tables = {
        'payments_by_customer': instance.table('payments_by_customer'),
        'payments_by_date': instance.table('payments_by_date'),
        'payments_by_transaction': instance.table('payments_by_transaction'),
        'payments_by_id': instance.table('payments_by_id'),
        'payments_by_id_date': instance.table('payments_by_id_date')
    }
    
    # Create tester and run tests
    tester = QueryTester(tables)
    results_df = await tester.run_comprehensive_test()
    
    # Print results
    print("\nQuery Performance Results:")
    print("=" * 80)
    print(results_df.to_string())
    
    # Save results
    results_df.to_csv('query_performance_results.csv', index=False)
    return results_df

if __name__ == "__main__":
    asyncio.run(main())