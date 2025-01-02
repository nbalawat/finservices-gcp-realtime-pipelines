import asyncio
from google.cloud.bigtable.data import TableAsync, BigtableDataClientAsync
from google.cloud.bigtable.data import row_filters,RowRange as QueryRowRange, ReadRowsQuery
from typing import List, Dict
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def create_bigtable_client(*, project_id: str, instance_id: str) -> TableAsync:
    """Create and return async Bigtable table instance
    
    Args:
        project_id: Google Cloud project ID
        instance_id: Bigtable instance ID
    
    Returns:
        Async Bigtable table instance
    """
    logger.info(f"Connecting to BigTable - Project: {project_id}, Instance: {instance_id}")
    client = BigtableDataClientAsync()
    table = TableAsync(client, instance_id=instance_id, table_id='payments_by_customer')
    async with table:
        return table

async def get_customer_transactions_by_types(
    table: TableAsync,
    customer_id: str,
    transaction_types: List[str],
    start_date: str,
    end_date: str
) -> List[Dict]:
    """
    Get customer transactions for multiple transaction types within a date range
    
    Args:
        table: Async Bigtable table instance
        customer_id: Customer ID
        transaction_types: List of transaction types (e.g., ['WIRE', 'ACH'])
        start_date: Start date (format: YYYY-MM-DDTHH:mm:ss)
        end_date: End date (format: YYYY-MM-DDTHH:mm:ss)
        
    Returns:
        List of matching transactions
    """
    # Create start and end keys
    start_key = f"{customer_id}#{start_date}"
    end_key = f"{customer_id}#{end_date}"

    print(f"Start key: {start_key}")
    print(f"End key: {end_key}")
    
    # Create row range
    row_range = QueryRowRange(
        start_key=start_key.encode('utf-8'),
        end_key=end_key.encode('utf-8'),
        start_is_inclusive=True,
        end_is_inclusive=True
    )

    # Create a filter chain for the row key pattern and latest cell
    filter = None 
    filters = []
    query = None
    
    # Validate inputs
    if not transaction_types:
        # If no transaction types specified, match any transaction type
        pattern = f"{customer_id}#[^#]*#[^#]*$"
        filter = row_filters.RowKeyRegexFilter(pattern.encode('utf-8'))
        query = ReadRowsQuery(
            row_ranges=row_range,
            row_filter=filter
        )
    elif len(transaction_types) == 1:
        # If only one transaction type, use a simple regex filter
        pattern = f"{customer_id}#[^#]*#{transaction_types[0]}$"
        filter = row_filters.RowKeyRegexFilter(pattern.encode('utf-8'))
        query = ReadRowsQuery(
            row_ranges=row_range,
            row_filter=filter
        )
    else:
        # For multiple types, create a union of filters
        type_filters = []
        for tx_type in transaction_types:
            pattern = f"{customer_id}#[^#]*#{tx_type}$"
            type_filters.append(row_filters.RowKeyRegexFilter(pattern.encode('utf-8')))

        # Use RowFilterUnion for OR logic between transaction types
        filter_union = row_filters.RowFilterUnion(filters=type_filters)
        
        # Create read query
        query = ReadRowsQuery(
            row_ranges=row_range,
            row_filter=filter_union
        )
    
    results = []
    try:
        rows = await table.read_rows(query)
        for row in rows:
            for cell in row.get_cells():
                results.append({
                    'row_key': row.row_key.decode('utf-8'),
                    'column_family': cell.family,
                    'qualifier': cell.qualifier.decode('utf-8'),
                    'value': cell.value.decode('utf-8'),
                    'timestamp': cell.timestamp_micros
                })

    except Exception as e:
        logger.error(f"Error reading rows: {e}")
        raise
    
    return results

async def main():
    """Main function with additional example scenarios"""
    
    # Initialize BigTable client
    table = await create_bigtable_client(
        project_id="agentic-experiments-446019",
        instance_id="payment-processing-dev"
    )
    
    # Scenario 1: All electronic payments
    customer_id = "CUST000001"
    electronic_types = ["WIRE", "ACH"]
    results1 = await get_customer_transactions_by_types(
        table,
        customer_id=customer_id,
        transaction_types=electronic_types,
        start_date="2024-03-01T00:00:00",
        end_date="2024-03-01T23:59:59"
    )
    print(f"\nElectronic payments in March: {len(results1)}")
    print(f"\nSample Record: {results1[0]}")

    
if __name__ == "__main__":
    asyncio.run(main())