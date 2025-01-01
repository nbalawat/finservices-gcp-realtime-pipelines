from google.cloud.bigtable import row_filters
from google.cloud.bigtable.row_set import RowSet
from typing import List, Dict

async def get_customer_transactions_by_types(
    table,
    customer_id: str,
    transaction_types: List[str],
    start_date: str,
    end_date: str
) -> List[Dict]:
    """
    Get customer transactions for multiple transaction types within a date range
    
    Args:
        table: Bigtable table instance
        customer_id: Customer ID
        transaction_types: List of transaction types (e.g., ['WIRE', 'ACH', 'SWIFT'])
        start_date: Start date (format: YYYY-MM-DD or YYYY-MM-DDTHH:mm:ss)
        end_date: End date (format: YYYY-MM-DD or YYYY-MM-DDTHH:mm:ss)
        
    Returns:
        List of matching transactions
    """
    # Create row set for the date range
    row_set = RowSet()
    
    # Add range from start to end date for the customer
    start_key = f"{customer_id}#{start_date}"
    end_key = f"{customer_id}#{end_date}\xff"
    
    row_set.add_row_range(
        start_key=start_key.encode('utf-8'),
        end_key=end_key.encode('utf-8')
    )
    
    # Create filters for each transaction type
    type_filters = [
        row_filters.RowKeyRegexFilter(f".*#{tx_type}$".encode('utf-8'))
        for tx_type in transaction_types
    ]
    
    # Combine type filters with OR logic using RowFilterUnion
    type_filter = row_filters.RowFilterUnion(filters=type_filters)
    
    results = []
    async for row in table.read_rows(row_set=row_set, filter_=type_filter):
        row_key = row.row_key.decode('utf-8')
        customer_id, timestamp, tx_type = row_key.split('#')
        
        data = {
            col_family: {
                col: val[0].value.decode('utf-8')
                for col, val in columns.items()
            }
            for col_family, columns in row.cells.items()
        }
        
        results.append({
            'row_key': row_key,
            'customer_id': customer_id,
            'timestamp': timestamp,
            'transaction_type': tx_type,
            'data': data
        })
    
    return results

async def example_usage():
    """Example usage of multiple transaction types query"""
    
    # Initialize BigTable client
    instance = await create_bigtable_client()
    table = instance.table('payments_by_customer')
    
    # Example parameters
    customer_id = "CUST123"
    transaction_types = ["WIRE", "ACH", "SWIFT"]
    start_date = "2024-03-01"
    end_date = "2024-03-31"
    
    results = await get_customer_transactions_by_types(
        table,
        customer_id=customer_id,
        transaction_types=transaction_types,
        start_date=start_date,
        end_date=end_date
    )
    
    # Process and display results
    print(f"\nResults for customer {customer_id}:")
    print(f"Date range: {start_date} to {end_date}")
    print(f"Transaction types: {', '.join(transaction_types)}")
    print(f"Total transactions found: {len(results)}")
    
    # Group by transaction type
    by_type = {}
    for tx in results:
        tx_type = tx['transaction_type']
        by_type.setdefault(tx_type, []).append(tx)
    
    # Print summary by type
    print("\nBreakdown by type:")
    for tx_type, transactions in by_type.items():
        print(f"{tx_type}: {len(transactions)} transactions")
        
        # Show sample transaction for each type
        if transactions:
            sample = transactions[0]
            print(f"  Sample transaction:")
            print(f"  Timestamp: {sample['timestamp']}")
            print(f"  Amount: {sample['data'].get('cf1', {}).get('amount', 'N/A')}")

async def main():
    """Main function with additional example scenarios"""
    
    instance = await create_bigtable_client()
    table = instance.table('payments_by_customer')
    customer_id = "CUST123"
    
    # Scenario 1: All electronic payments
    electronic_types = ["WIRE", "ACH", "SWIFT", "SEPA"]
    results1 = await get_customer_transactions_by_types(
        table,
        customer_id=customer_id,
        transaction_types=electronic_types,
        start_date="2024-03-01",
        end_date="2024-03-31"
    )
    print(f"\nElectronic payments in March: {len(results1)}")
    
    # Scenario 2: International payments
    international_types = ["WIRE", "SWIFT"]
    results2 = await get_customer_transactions_by_types(
        table,
        customer_id=customer_id,
        transaction_types=international_types,
        start_date="2024-03-01T00:00:00",
        end_date="2024-03-31T23:59:59"
    )
    print(f"\nInternational payments in March: {len(results2)}")
    
    # Calculate some statistics
    if results2:
        amounts = [
            float(tx['data'].get('cf1', {}).get('amount', 0))
            for tx in results2
        ]
        total_amount = sum(amounts)
        avg_amount = total_amount / len(amounts)
        print(f"Total amount: ${total_amount:,.2f}")
        print(f"Average amount: ${avg_amount:,.2f}")

if __name__ == "__main__":
    asyncio.run(main())