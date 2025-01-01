from typing import List, Optional, Dict, Any
from google.cloud.bigtable.row_set import RowSet
import json

class CustomerQueries:
    """Implementations of customer-centric queries for each table structure"""

    @staticmethod
    async def customer_daily_type_transactions():
        """For a given customer find transactions of a particular payment type on a given day"""
        
        # Implementation for payments_by_customer (customerId#transaction_date#transaction_type)
        async def using_customer_table(table, customer_id: str, transaction_date: str, 
                                     payment_type: str) -> List[dict]:
            row_key = f"{customer_id}#{transaction_date}#{payment_type}"
            row_set = RowSet()
            row_set.add_row_range_from_prefix(row_key)
            
            results = []
            async for row in table.read_rows(row_set=row_set):
                results.append(row)
            return results

        # Implementation for payments_by_date (transaction_date#transaction_type#customerId)
        async def using_date_table(table, customer_id: str, transaction_date: str,
                                 payment_type: str) -> List[dict]:
            row_key = f"{transaction_date}#{payment_type}#{customer_id}"
            row_set = RowSet()
            row_set.add_row_key(row_key)
            
            results = []
            async for row in table.read_rows(row_set=row_set):
                results.append(row)
            return results

        # Implementation for payments_by_transaction (transaction_id#customerId#transaction_date)
        async def using_transaction_table(table, customer_id: str, transaction_date: str,
                                        payment_type: str) -> List[dict]:
            # Need to scan and filter since key structure doesn't optimize this query
            row_set = RowSet()
            results = []
            async for row in table.read_rows():
                key_parts = row.row_key.decode('utf-8').split('#')
                if (len(key_parts) >= 3 and 
                    key_parts[1] == customer_id and 
                    key_parts[2] == transaction_date):
                    # Additional filtering for payment type
                    cell_value = row.cells["cf1"]["payment_type"][0].value
                    if cell_value.decode('utf-8') == payment_type:
                        results.append(row)
            return results

        # Implementation for payments_by_id (transaction_id)
        async def using_id_table(table, customer_id: str, transaction_date: str,
                               payment_type: str) -> List[dict]:
            # Full scan required as key structure doesn't support this query pattern
            row_set = RowSet()
            results = []
            async for row in table.read_rows():
                cell_values = row.cells["cf1"]
                if (cell_values["customer_id"][0].value.decode('utf-8') == customer_id and
                    cell_values["transaction_date"][0].value.decode('utf-8') == transaction_date and
                    cell_values["payment_type"][0].value.decode('utf-8') == payment_type):
                    results.append(row)
            return results

        # Implementation for payments_by_id_date (transaction_id#transaction_date)
        async def using_id_date_table(table, customer_id: str, transaction_date: str,
                                    payment_type: str) -> List[dict]:
            # Scan by date and filter
            prefix = f"#{transaction_date}"
            row_set = RowSet()
            results = []
            async for row in table.read_rows():
                if row.row_key.decode('utf-8').endswith(prefix):
                    cell_values = row.cells["cf1"]
                    if (cell_values["customer_id"][0].value.decode('utf-8') == customer_id and
                        cell_values["payment_type"][0].value.decode('utf-8') == payment_type):
                        results.append(row)
            return results

        return {
            "payments_by_customer": using_customer_table,
            "payments_by_date": using_date_table,
            "payments_by_transaction": using_transaction_table,
            "payments_by_id": using_id_table,
            "payments_by_id_date": using_id_date_table
        }

    @staticmethod
    async def customer_date_range_transactions():
        """For a given customer find all transactions over a given date range"""
        
        # Implementation for payments_by_customer (customerId#transaction_date#transaction_type)
        async def using_customer_table(table, customer_id: str, start_date: str,
                                     end_date: str) -> List[dict]:
            start_key = f"{customer_id}#{start_date}"
            end_key = f"{customer_id}#{end_date}\uff\uff"  # \uff\uff ensures we get all entries for end_date
            
            row_set = RowSet()
            row_set.add_row_range(start_key=start_key, end_key=end_key)
            
            results = []
            async for row in table.read_rows(row_set=row_set):
                results.append(row)
            return results

        # Implementation for payments_by_date (transaction_date#transaction_type#customerId)
        async def using_date_table(table, customer_id: str, start_date: str,
                                 end_date: str) -> List[dict]:
            # Need to scan date range and filter by customer
            start_key = start_date
            end_key = f"{end_date}\uff\uff"
            
            row_set = RowSet()
            row_set.add_row_range(start_key=start_key, end_key=end_key)
            
            results = []
            async for row in table.read_rows(row_set=row_set):
                key_parts = row.row_key.decode('utf-8').split('#')
                if len(key_parts) > 2 and key_parts[2] == customer_id:
                    results.append(row)
            return results

        # Continue with other table implementations...
        return {
            "payments_by_customer": using_customer_table,
            "payments_by_date": using_date_table,
            # Add other implementations
        }
