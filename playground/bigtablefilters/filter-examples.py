from google.cloud.bigtable import row_filters
from typing import List, Dict, Any
import asyncio
import datetime
import re

class BigtableFilterExplorer:
    """Explore and test different Bigtable filters"""
    
    def __init__(self, table):
        self.table = table

    async def test_row_key_regex_filter(self) -> List[Dict]:
        """
        Test RowKeyRegexFilter with different patterns
        
        Examples using payments_by_customer (customerId#transaction_date#transaction_type):
        - Find all transactions for customer starting with 'CUST123'
        - Find all ACH transactions
        - Find all transactions from March 2024
        """
        test_cases = [
            {
                "description": "Find customer CUST123 transactions",
                "filter": row_filters.RowKeyRegexFilter(b"CUST123#.*"),
            },
            {
                "description": "Find all ACH transactions",
                "filter": row_filters.RowKeyRegexFilter(b".*#ACH$"),
            },
            {
                "description": "Find March 2024 transactions",
                "filter": row_filters.RowKeyRegexFilter(b".*#2024-03-.*"),
            }
        ]

        results = []
        for case in test_cases:
            try:
                rows = []
                async for row in self.table.read_rows(filter_=case["filter"]):
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
                
                results.append({
                    "test_case": case["description"],
                    "matched_rows": len(rows),
                    "sample_rows": rows[:3]  # First 3 rows as sample
                })
            except Exception as e:
                results.append({
                    "test_case": case["description"],
                    "error": str(e)
                })
        
        return results

    async def test_value_range_filter(self) -> List[Dict]:
        """
        Test ValueRangeFilter for amount ranges
        
        Examples:
        - Find high-value transactions (>$10000)
        - Find micro-transactions (<$10)
        """
        test_cases = [
            {
                "description": "High-value transactions",
                "filter": row_filters.ValueRangeFilter(
                    start_value=b"10000.00",
                    end_value=None,
                    inclusive_start=True,
                    inclusive_end=True
                )
            },
            {
                "description": "Micro-transactions",
                "filter": row_filters.ValueRangeFilter(
                    start_value=b"0.00",
                    end_value=b"10.00",
                    inclusive_start=True,
                    inclusive_end=True
                )
            }
        ]

        results = []
        for case in test_cases:
            try:
                rows = []
                async for row in self.table.read_rows(filter_=case["filter"]):
                    rows.append({
                        "row_key": row.row_key.decode('utf-8'),
                        "amount": float(row.cells["cf1"]["amount"][0].value.decode('utf-8'))
                    })
                
                results.append({
                    "test_case": case["description"],
                    "matched_rows": len(rows),
                    "sample_rows": rows[:3]
                })
            except Exception as e:
                results.append({
                    "test_case": case["description"],
                    "error": str(e)
                })
        
        return results

    async def test_column_filters(self) -> List[Dict]:
        """
        Test column-based filters
        
        Examples:
        - Get only specific payment fields
        - Get ranges of columns
        """
        test_cases = [
            {
                "description": "Only payment status fields",
                "filter": row_filters.ColumnQualifierRegexFilter(b"status.*")
            },
            {
                "description": "Only amount-related fields",
                "filter": row_filters.ColumnQualifierRegexFilter(b"amount.*")
            },
            {
                "description": "Range of timestamp columns",
                "filter": row_filters.ColumnRangeFilter(
                    start_column=b"created_at",
                    end_column=b"updated_at",
                    inclusive_start=True,
                    inclusive_end=True
                )
            }
        ]

        results = []
        for case in test_cases:
            try:
                rows = []
                async for row in self.table.read_rows(filter_=case["filter"]):
                    rows.append({
                        "row_key": row.row_key.decode('utf-8'),
                        "columns": list(row.cells["cf1"].keys())
                    })
                
                results.append({
                    "test_case": case["description"],
                    "matched_rows": len(rows),
                    "sample_rows": rows[:3]
                })
            except Exception as e:
                results.append({
                    "test_case": case["description"],
                    "error": str(e)
                })
        
        return results

    async def test_composite_filters(self) -> List[Dict]:
        """
        Test composite filters combining multiple conditions
        
        Examples:
        - High-value ACH transactions
        - Recent failed transactions
        """
        high_value_filter = row_filters.ValueRangeFilter(
            start_value=b"10000.00",
            end_value=None,
            inclusive_start=True,
            inclusive_end=True
        )
        
        ach_filter = row_filters.RowKeyRegexFilter(b".*#ACH$")
        
        status_failed_filter = row_filters.ValueRangeFilter(
            start_value=b"FAILED",
            end_value=b"FAILED",
            inclusive_start=True,
            inclusive_end=True
        )

        test_cases = [
            {
                "description": "High-value ACH transactions",
                "filter": row_filters.RowFilterChain(
                    filters=[high_value_filter, ach_filter]
                )
            },
            {
                "description": "Either high-value or failed transactions",
                "filter": row_filters.RowFilterUnion(
                    filters=[high_value_filter, status_failed_filter]
                )
            },
            {
                "description": "Conditional: More details for high-value transactions",
                "filter": row_filters.ConditionalRowFilter(
                    base_filter=high_value_filter,
                    true_filter=row_filters.PassAllFilter(True),  # Return all columns
                    false_filter=row_filters.ColumnQualifierRegexFilter(b"^(amount|status)$")  # Only basic info
                )
            }
        ]

        results = []
        for case in test_cases:
            try:
                rows = []
                async for row in self.table.read_rows(filter_=case["filter"]):
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
                
                results.append({
                    "test_case": case["description"],
                    "matched_rows": len(rows),
                    "sample_rows": rows[:3]
                })
            except Exception as e:
                results.append({
                    "test_case": case["description"],
                    "error": str(e)
                })
        
        return results

    async def test_timestamp_filters(self) -> List[Dict]:
        """
        Test timestamp-based filters
        
        Examples:
        - Recent transactions
        - Transactions in specific time window
        """
        now = datetime.datetime.now()
        one_hour_ago = now - datetime.timedelta(hours=1)
        one_day_ago = now - datetime.timedelta(days=1)
        
        test_cases = [
            {
                "description": "Last hour transactions",
                "filter": row_filters.TimestampRangeFilter(
                    start_timestamp=int(one_hour_ago.timestamp() * 1000000),
                    end_timestamp=int(now.timestamp() * 1000000)
                )
            },
            {
                "description": "Last 24 hours transactions",
                "filter": row_filters.TimestampRangeFilter(
                    start_timestamp=int(one_day_ago.timestamp() * 1000000),
                    end_timestamp=int(now.timestamp() * 1000000)
                )
            }
        ]

        results = []
        for case in test_cases:
            try:
                rows = []
                async for row in self.table.read_rows(filter_=case["filter"]):
                    rows.append({
                        "row_key": row.row_key.decode('utf-8'),
                        "timestamp": row.cells["cf1"]["created_at"][0].timestamp
                    })
                
                results.append({
                    "test_case": case["description"],
                    "matched_rows": len(rows),
                    "sample_rows": rows[:3]
                })
            except Exception as e:
                results.append({
                    "test_case": case["description"],
                    "error": str(e)
                })
        
        return results

    async def test_transformation_filters(self) -> List[Dict]:
        """
        Test transformation filters
        
        Examples:
        - Latest version of each cell
        - Skip first N versions
        - Strip values for existence check
        """
        test_cases = [
            {
                "description": "Latest version only",
                "filter": row_filters.CellsPerRowLimitFilter(1)
            },
            {
                "description": "Skip first version",
                "filter": row_filters.CellsPerRowOffsetFilter(1)
            },
            {
                "description": "Check existence only",
                "filter": row_filters.StripValueTransformer(True)
            }
        ]

        results = []
        for case in test_cases:
            try:
                rows = []
                async for row in self.table.read_rows(filter_=case["filter"]):
                    rows.append({
                        "row_key": row.row_key.decode('utf-8'),
                        "cell_count": sum(len(vals) for cols in row.cells.values() for vals in cols.values())
                    })
                
                results.append({
                    "test_case": case["description"],
                    "matched_rows": len(rows),
                    "sample_rows": rows[:3]
                })
            except Exception as e:
                results.append({
                    "test_case": case["description"],
                    "error": str(e)
                })
        
        return results