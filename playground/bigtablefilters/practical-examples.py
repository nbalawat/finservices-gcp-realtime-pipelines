from google.cloud.bigtable import row_filters
from typing import Dict, List
import datetime

class PracticalFilterExamples:
    """Real-world examples of filter usage for each table structure"""

    @staticmethod
    async def payments_by_customer_examples(table) -> Dict[str, List[Dict]]:
        """
        Practical filter examples for payments_by_customer table
        Structure: customerId#transaction_date#transaction_type
        """
        examples = {
            "High value customer transactions": {
                "description": "Find all high-value (>$10000) transactions for a specific customer in last 30 days",
                "filter": row_filters.RowFilterChain(filters=[
                    row_filters.RowKeyRegexFilter(b"CUST123#.*"),  # Specific customer
                    row_filters.ValueRangeFilter(  # High value
                        start_value=b"10000.00",
                        end_value=None,
                        inclusive_start=True
                    ),
                    row_filters.TimestampRangeFilter(  # Last 30 days
                        start_timestamp=int((datetime.datetime.now() - datetime.timedelta(days=30)).timestamp() * 1000000)
                    )
                ])
            },
            "Failed ACH transactions": {
                "description": "Find all failed ACH transactions for any customer",
                "filter": row_filters.RowFilterChain(filters=[
                    row_filters.RowKeyRegexFilter(b".*#ACH$"),  # ACH transactions
                    row_filters.ValueRegexFilter(b"FAILED")  # Failed status
                ])
            },
            "Customer transaction history": {
                "description": "Get complete transaction history for a customer with latest status only",
                "filter": row_filters.RowFilterChain(filters=[
                    row_filters.RowKeyRegexFilter(b"CUST123#.*"),  # Specific customer
                    row_filters.CellsPerRowLimitFilter(1)  # Latest version only
                ])
            }
        }

        results = {}
        for name, example in examples.items():
            rows = []
            async for row in table.read_rows(filter_=example["filter"]):
                rows.append({
                    "row_key": row.row_key.decode('utf-8'),
                    "data": {
                        col_family: {
                            col: val[0].value.decode('utf-8')
                            for col, val in columns.items()
                        }
                        for col_family, columns in row.cells.items()
                    }
                })
            results[name] = {
                "description": example["description"],
                "matched_rows": len(rows),
                "sample_data": rows[:3]
            }
        
        return results

    @staticmethod
    async def payments_by_date_examples(table) -> Dict[str, List[Dict]]:
        """
        Practical filter examples for payments_by_date table
        Structure: transaction_date#transaction_type#customerId
        """
        examples = {
            "Daily transaction summary": {
                "description": "Get all transactions for a specific day with amount totals",
                "filter": row_filters.RowFilterChain(filters=[
                    row_filters.RowKeyRegexFilter(b"2024-03-15#.*"),  # Specific date
                    row_filters.ColumnQualifierRegexFilter(b"amount|type")  # Only amount and type columns
                ])
            },
            "Payment type analysis": {
                "description": "Analyze all WIRE transfers in date range",
                "filter": row_filters.RowFilterChain(filters=[
                    row_filters.RowKeyRegexFilter(b"2024-03-.*#WIRE#.*"),  # March WIRE transfers
                    row_filters.ColumnQualifierRegexFilter(b"amount|status")  # Only amount and status
                ])
            },
            "Customer activity pattern": {
                "description": "Find customers with multiple transactions on same day",
                "filter": row_filters.RowFilterChain(filters=[
                    row_filters.RowKeyRegexFilter(b"2024-03-15#.*"),  # Specific date
                    row_filters.ColumnQualifierRegexFilter(b"customer_id")  # Only customer ID
                ])
            }
        }

        results = {}
        for name, example in examples.items():
            rows = []
            async for row in table.read_rows(filter_=example["filter"]):
                rows.append({
                    "row_key": row.row_key.decode('utf-8'),
                    "data": {
                        col_family: {
                            col: val[0].value.decode('utf-8')
                            for col, val in columns.items()
                        }
                        for col_family, columns in row.cells.items()
                    }
                })
            results[name] = {
                "description": example["description"],
                "matched_rows": len(rows),
                "sample_data": rows[:3]
            }
        
        return results

    @staticmethod
    async def payments_by_transaction_examples(table) -> Dict[str, List[Dict]]:
        """
        Practical filter examples for payments_by_transaction table
        Structure: transaction_id#customerId#transaction_date
        """
        examples = {
            "Transaction batch validation": {
                "description": "Validate status of multiple transactions",
                "filter": row_filters.RowFilterChain(filters=[
                    row_filters.RowKeyRegexFilter(b"(TX001|TX002|TX003).*"),  # Specific transactions
                    row_filters.ColumnQualifierRegexFilter(b"status|amount")  # Only status and amount
                ])
            },
            "Customer transaction lookup": {
                "description": "Find all transactions for a customer with amounts",
                "filter": row_filters.RowFilterChain(filters=[
                    row_filters.RowKeyRegexFilter(b".*#CUST123#.*"),  # Specific customer
                    row_filters.ColumnQualifierRegexFilter(b"amount|type|status")  # Relevant fields
                ])
            },
            "Recent large transactions": {
                "description": "Find recent transactions over $50000",
                "filter": row_filters.RowFilterChain(filters=[
                    row_filters.TimestampRangeFilter(  # Last hour
                        start_timestamp=int((datetime.datetime.now() - datetime.timedelta(hours=1)).timestamp() * 1000000)
                    ),
                    row_filters.ValueRangeFilter(  # Over $50000
                        start_value=b"50000.00",
                        end_value=None,
                        inclusive_start=True
                    )
                ])
            }
        }

        results = {}
        for name, example in examples.items():
            rows = []
            async for row in table.read_rows(filter_=example["filter"]):
                rows.append({
                    "row_key": row.row_key.decode('utf-8'),
                    "data": {
                        col_family: {
                            col: val[0].value.decode('utf-8')
                            for col, val in columns.items()
                        }
                        for col_family, columns in row.cells.items()
                    }
                })
            results[name] = {
                "description": example["description"],
                "matched_rows": len(rows),
                "sample_data": rows[:3]
            }
        
        return results

    @staticmethod
    async def payments_by_id_examples(table) -> Dict[str, List[Dict]]:
        """
        Practical filter examples for payments_by_id table
        Structure: transaction_id
        """
        examples = {
            "Transaction details": {
                "description": "Get full details of specific transactions",
                "filter": row_filters.RowFilterChain(filters=[
                    row_filters.RowKeyRegexFilter(b"TX001|TX002|TX003"),  # Specific transactions
                    row_filters.PassAllFilter(True)  # All columns
                ])
            },
            "Transaction status check": {
                "description": "Quick status check of transactions",
                "filter": row_filters.RowFilterChain(filters=[
                    row_filters.RowKeyRegexFilter(b"TX.*"),  # All transactions
                    row_filters.ColumnQualifierRegexFilter(b"status")  # Only status
                ])
            },
            "Amount verification": {
                "description": "Verify amounts for specific transactions",
                "filter": row_filters.RowFilterChain(filters=[
                    row_filters.RowKeyRegexFilter(b"TX.*"),  # All transactions
                    row_filters.ColumnQualifierRegexFilter(b"amount|currency")  # Amount and currency
                ])
            }
        }

        results = {}
        for name, example in examples.items():
            rows = []
            async for row in table.read_rows(filter_=example["filter"]):
                rows.append({
                    "row_key": row.row_key.decode('utf-8'),
                    "data": {
                        col_family: {
                            col: val[0].value.decode('utf-8')
                            for col, val in columns.items()
                        }
                        for col_family, columns in row.cells.items()
                    }
                })
            results[name] = {
                "description": example["description"],
                "matched_rows": len(rows),
                "sample_data": rows[:3]
            }
        
        return results