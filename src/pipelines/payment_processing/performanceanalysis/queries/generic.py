from typing import List, Set
from datetime import datetime, timedelta
from google.cloud.bigtable.row_set import RowSet
from google.cloud.bigtable.row_data import PartialRowData

class TransactionBatchLookupQuery(BaseQuery):
    """Query for looking up multiple transactions by their IDs"""
    
    def build_row_key(self, transaction_id: str) -> str:
        """
        Build row key for transaction lookup
        
        Args:
            transaction_id: Transaction identifier
            
        Returns:
            str: Constructed row key
        """
        return transaction_id
    
    def execute(self, transaction_ids: List[str]) -> QueryResult:
        """
        Execute batch lookup of transactions
        
        Args:
            transaction_ids: List of transaction IDs to look up
            
        Returns:
            QueryResult: Query execution results
        """
        try:
            row_set = RowSet()
            for tx_id in transaction_ids:
                row_set.add_row_key(self.build_row_key(tx_id))
            
            def query_func():
                table = self.client.instance().table(self.table_name)
                rows = table.read_rows(row_set=row_set)
                results = [row for row in rows]
                return results
            
            results, execution_time = self._measure_execution_time(query_func)
            
            return self._create_result(
                execution_time=execution_time,
                row_count=len(results) if results else 0
            )
        
        except Exception as e:
            return self._create_result(
                execution_time=0,
                row_count=0,
                status="error",
                error=str(e)
            )

class DailyCustomerActivityQuery(BaseQuery):
    """Query for finding all customers who transacted on a given day"""
    
    def build_row_key(self, transaction_date: str) -> str:
        """Build prefix for daily activity query"""
        return f"{transaction_date}"
    
    def execute(self, transaction_date: str) -> QueryResult:
        """
        Execute query for daily customer activity
        
        Args:
            transaction_date: Date in YYYY-MM-DD format
            
        Returns:
            QueryResult: Query execution results with unique customer count
        """
        try:
            row_key_prefix = self.build_row_key(transaction_date)
            row_set = RowSet()
            row_set.add_row_range_from_prefix(row_key_prefix)
            
            def query_func():
                table = self.client.instance().table(self.table_name)
                rows = table.read_rows(row_set=row_set)
                # Extract unique customer IDs from row keys
                customer_ids: Set[str] = set()
                for row in rows:
                    # Extract customer ID from row key based on table schema
                    key_parts = row.row_key.decode('utf-8').split('#')
                    if len(key_parts) >= 3:
                        customer_ids.add(key_parts[2])  # Adjust index based on key structure
                return customer_ids
            
            customer_ids, execution_time = self._measure_execution_time(query_func)
            
            return self._create_result(
                execution_time=execution_time,
                row_count=len(customer_ids) if customer_ids else 0
            )
            
        except Exception as e:
            return self._create_result(
                execution_time=0,
                row_count=0,
                status="error",
                error=str(e)
            )

class PaymentTypeAnalysisQuery(BaseQuery):
    """Query for analyzing transaction volumes by payment type over a period"""
    
    def build_row_key(self, transaction_date: str, payment_type: str) -> str:
        """Build row key prefix for payment type analysis"""
        return f"{transaction_date}#{payment_type}"
    
    def execute(self, start_date: str, end_date: str, 
                payment_type: str) -> QueryResult:
        """
        Execute query for payment type analysis over a date range
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            payment_type: Type of payment to analyze
            
        Returns:
            QueryResult: Query execution results
        """
        try:
            start_key = self.build_row_key(start_date, payment_type)
            end_key = self.build_row_key(end_date + "\uff\uff", payment_type)
            
            row_set = RowSet()
            row_set.add_row_range(start_key=start_key, end_key=end_key)
            
            def query_func():
                table = self.client.instance().table(self.table_name)
                rows = table.read_rows(row_set=row_set)
                results = [row for row in rows]
                return results
            
            results, execution_time = self._measure_execution_time(query_func)
            
            return self._create_result(
                execution_time=execution_time,
                row_count=len(results) if results else 0
            )
            
        except Exception as e:
            return self._create_result(
                execution_time=0,
                row_count=0,
                status="error",
                error=str(e)
            )