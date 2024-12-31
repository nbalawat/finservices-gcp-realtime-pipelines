from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from google.cloud.bigtable.row_set import RowSet
from google.cloud.bigtable.row_data import PartialRowData
import time
import json

class CustomerDailyTransactionsQuery(BaseQuery):
    """Query for finding customer transactions of a particular payment type on a given day"""
    
    def build_row_key(self, customer_id: str, transaction_date: str, 
                     transaction_type: str) -> str:
        """
        Build row key for customer daily transactions query
        
        Args:
            customer_id: Customer identifier
            transaction_date: Date in YYYY-MM-DD format
            transaction_type: Type of transaction
            
        Returns:
            str: Constructed row key
        """
        return f"{customer_id}#{transaction_date}#{transaction_type}"
    
    def execute(self, customer_id: str, transaction_date: str,
                transaction_type: str) -> QueryResult:
        """
        Execute query for customer's transactions of specific type on given day
        
        Args:
            customer_id: Customer identifier
            transaction_date: Date in YYYY-MM-DD format
            transaction_type: Type of transaction
            
        Returns:
            QueryResult: Query execution results
        """
        try:
            # Create the row key
            row_key = self.build_row_key(customer_id, transaction_date, transaction_type)
            
            # Create a row set for prefix scanning
            row_set = RowSet()
            row_set.add_row_range_from_prefix(row_key)
            
            def query_func():
                table = self.client.instance().table(self.table_name)
                rows = table.read_rows(row_set=row_set)
                results = []
                for row in rows:
                    cells = row.cells["cf1"]  # Assuming column family is cf1
                    transaction_data = {
                        column: cells[column][0].value.decode('utf-8')
                        for column in cells
                    }
                    results.append(transaction_data)
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

class CustomerDateRangeQuery(BaseQuery):
    """Query for finding all customer transactions over a date range"""
    
    def build_row_key(self, customer_id: str, start_date: str) -> str:
        """Build prefix for customer date range query"""
        return f"{customer_id}#{start_date}"
    
    def execute(self, customer_id: str, start_date: str, 
                end_date: str) -> QueryResult:
        """
        Execute query for customer's transactions within date range
        
        Args:
            customer_id: Customer identifier
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            QueryResult: Query execution results
        """
        try:
            # Create row range for date range scan
            start_key = f"{customer_id}#{start_date}"
            end_key = f"{customer_id}#{end_date}\uff\uff"  # \uff\uff ensures we get all entries for end_date
            
            row_set = RowSet()
            row_set.add_row_range(start_key=start_key, end_key=end_key)
            
            def query_func():
                table = self.client.instance().table(self.table_name)
                rows = table.read_rows(row_set=row_set)
                results = []
                for row in rows:
                    cells = row.cells["cf1"]
                    transaction_data = {
                        column: cells[column][0].value.decode('utf-8')
                        for column in cells
                    }
                    results.append(transaction_data)
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

class CustomerTypeTransactionsQuery(BaseQuery):
    """Query for finding all customer transactions of a particular type over a date range"""
    
    def build_row_key(self, customer_id: str, transaction_type: str, 
                     date: str) -> str:
        """Build row key for type-specific transaction query"""
        return f"{customer_id}#{date}#{transaction_type}"
    
    def execute(self, customer_id: str, transaction_type: str,
                start_date: str, end_date: str) -> QueryResult:
        """
        Execute query for customer's transactions of specific type within date range
        
        Args:
            customer_id: Customer identifier
            transaction_type: Type of transaction
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            QueryResult: Query execution results
        """
        try:
            # Create row range for filtered scan
            start_key = self.build_row_key(customer_id, transaction_type, start_date)
            end_key = self.build_row_key(customer_id, transaction_type, end_date + "\uff\uff")
            
            row_set = RowSet()
            row_set.add_row_range(start_key=start_key, end_key=end_key)
            
            def query_func():
                table = self.client.instance().table(self.table_name)
                rows = table.read_rows(row_set=row_set)
                results = []
                for row in rows:
                    cells = row.cells["cf1"]
                    transaction_data = {
                        column: cells[column][0].value.decode('utf-8')
                        for column in cells
                    }
                    # Add parsed data for better analysis
                    key_parts = row.row_key.decode('utf-8').split('#')
                    transaction_data.update({
                        'customer_id': key_parts[0],
                        'date': key_parts[1],
                        'type': key_parts[2]
                    })
                    results.append(transaction_data)
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

class CustomerAllTransactionsQuery(BaseQuery):
    """Query for finding all transactions for a given customer"""
    
    def build_row_key(self, customer_id: str) -> str:
        """Build prefix for all customer transactions query"""
        return customer_id
    
    def execute(self, customer_id: str) -> QueryResult:
        """
        Execute query for all customer transactions
        
        Args:
            customer_id: Customer identifier
            
        Returns:
            QueryResult: Query execution results
        """
        try:
            # Create row set for prefix scan
            row_key_prefix = self.build_row_key(customer_id)
            row_set = RowSet()
            row_set.add_row_range_from_prefix(row_key_prefix)
            
            def query_func():
                table = self.client.instance().table(self.table_name)
                rows = table.read_rows(row_set=row_set)
                results = []
                
                for row in rows:
                    cells = row.cells["cf1"]
                    
                    # Parse the base payment JSON from the cell
                    base_payment_str = cells.get("base_payment", [None])[0]
                    if base_payment_str:
                        base_payment = json.loads(base_payment_str.value.decode('utf-8'))
                    else:
                        base_payment = {}
                    
                    # Parse any type-specific data
                    type_specific_str = cells.get("type_specific_data", [None])[0]
                    if type_specific_str:
                        type_specific_data = json.loads(type_specific_str.value.decode('utf-8'))
                    else:
                        type_specific_data = {}
                    
                    # Combine all data
                    transaction_data = {
                        **base_payment,
                        'type_specific_data': type_specific_data
                    }
                    
                    # Add key information
                    key_parts = row.row_key.decode('utf-8').split('#')
                    transaction_data.update({
                        'customer_id': key_parts[0],
                        'date': key_parts[1] if len(key_parts) > 1 else None,
                        'type': key_parts[2] if len(key_parts) > 2 else None
                    })
                    
                    results.append(transaction_data)
                
                return results
            
            results, execution_time = self._measure_execution_time(query_func)
            
            return self._create_result(
                execution_time=execution_time,
                row_count=len(results) if results else 0,
                status="success"
            )
            
        except Exception as e:
            return self._create_result(
                execution_time=0,
                row_count=0,
                status="error",
                error=str(e)
            )