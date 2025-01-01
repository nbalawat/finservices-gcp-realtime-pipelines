from google.cloud.bigtable import row_filters
from google.cloud.bigtable.row_set import RowSet
from typing import List, Dict, Any

class ComplexFilterExamples:
    """Examples of complex filter patterns using RowFilterUnion and ConditionalRowFilter"""

    @staticmethod
    async def union_filter_examples(table):
        """Examples of RowFilterUnion (OR logic) for different scenarios"""
        
        # Example 1: High-value OR flagged transactions
        high_value_or_flagged = {
            "description": "Find transactions that are either high-value (>$50000) or flagged for review",
            "filter": row_filters.RowFilterUnion(filters=[
                # High value filter
                row_filters.ValueRangeFilter(
                    start_value=b"50000.00",
                    end_value=None,
                    inclusive_start=True
                ),
                # Flagged transactions filter
                row_filters.ValueRegexFilter(b"FLAGGED_FOR_REVIEW")
            ])
        }

        # Example 2: Multiple payment types
        specific_payment_types = {
            "description": "Find all WIRE or ACH transactions",
            "filter": row_filters.RowFilterUnion(filters=[
                row_filters.RowKeyRegexFilter(b".*#WIRE$"),
                row_filters.RowKeyRegexFilter(b".*#ACH$")
            ])
        }

        # Example 3: Problem transactions
        problem_transactions = {
            "description": "Find transactions that are either failed, pending for >24h, or flagged for fraud",
            "filter": row_filters.RowFilterUnion(filters=[
                row_filters.ValueRegexFilter(b"FAILED"),
                row_filters.ValueRegexFilter(b"PENDING_LONG"),
                row_filters.ValueRegexFilter(b"FRAUD_SUSPECT")
            ])
        }

        # Run the examples
        results = {}
        for name, example in {
            "high_value_or_flagged": high_value_or_flagged,
            "specific_payment_types": specific_payment_types,
            "problem_transactions": problem_transactions
        }.items():
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
    async def conditional_filter_examples(table):
        """Examples of ConditionalRowFilter for different scenarios"""
        
        # Example 1: Enhanced data for high-value transactions
        high_value_enhanced = {
            "description": "Get full details for high-value transactions, basic details for others",
            "filter": row_filters.ConditionalRowFilter(
                base_filter=row_filters.ValueRangeFilter(  # Condition: amount > 50000
                    start_value=b"50000.00",
                    end_value=None,
                    inclusive_start=True
                ),
                true_filter=row_filters.PassAllFilter(True),  # All columns for high-value
                false_filter=row_filters.ColumnQualifierRegexFilter(  # Basic info only
                    b"^(amount|status|type)$"
                )
            )
        }

        # Example 2: Different handling for various transaction states
        status_based_details = {
            "description": "Get different columns based on transaction status",
            "filter": row_filters.ConditionalRowFilter(
                base_filter=row_filters.ValueRegexFilter(b"FAILED"),  # Condition: failed transactions
                true_filter=row_filters.ColumnQualifierRegexFilter(  # Error details for failed
                    b"(error.*|status_history|amount)"
                ),
                false_filter=row_filters.ColumnQualifierRegexFilter(  # Standard info for others
                    b"(status|amount|type)"
                )
            )
        }

        # Example 3: Customer type based filtering
        customer_type_based = {
            "description": "Different detail levels based on customer type",
            "filter": row_filters.ConditionalRowFilter(
                base_filter=row_filters.ValueRegexFilter(b"PREMIUM_CUSTOMER"),  # Condition: premium customer
                true_filter=row_filters.ColumnQualifierRegexFilter(  # Full details for premium
                    b".*"
                ),
                false_filter=row_filters.ColumnQualifierRegexFilter(  # Basic info for regular
                    b"^(amount|status|type|date)$"
                )
            )
        }

        # Run the examples
        results = {}
        for name, example in {
            "high_value_enhanced": high_value_enhanced,
            "status_based_details": status_based_details,
            "customer_type_based": customer_type_based
        }.items():
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

async def run_examples():
    """Run and display complex filter examples"""
    
    # Initialize BigTable client and table
    instance = await create_bigtable_client()
    table = instance.table('payments_by_customer')  # Using customer table for examples
    
    # Run union filter examples
    print("\nRow Filter Union Examples (OR logic):")
    print("=" * 80)
    union_results = await ComplexFilterExamples.union_filter_examples(table)
    
    for name, result in union_results.items():
        print(f"\n{name}:")
        print(f"Description: {result['description']}")
        print(f"Matched rows: {result['matched_rows']}")
        if result['matched_rows'] > 0:
            print("Sample rows:")
            for row in result['sample_data']:
                print(f"  - {row['row_key']}")
                for col_family, columns in row['data'].items():
                    for col, val in columns.items():
                        print(f"    {col}: {val}")
    
    # Run conditional filter examples
    print("\nConditional Row Filter Examples:")
    print("=" * 80)
    conditional_results = await ComplexFilterExamples.conditional_filter_examples(table)
    
    for name, result in conditional_results.items():
        print(f"\n{name}:")
        print(f"Description: {result['description']}")
        print(f"Matched rows: {result['matched_rows']}")
        if result['matched_rows'] > 0:
            print("Sample rows:")
            for row in result['sample_data']:
                print(f"  - {row['row_key']}")
                for col_family, columns in row['data'].items():
                    for col, val in columns.items():
                        print(f"    {col}: {val}")

if __name__ == "__main__":
    asyncio.run(run_examples())