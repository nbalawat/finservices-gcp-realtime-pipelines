from google.cloud.bigtable import row_filters
from google.cloud.bigtable.row_set import RowSet
from datetime import datetime, timedelta

class DateRangeExamples:
    """Examples of date range filtering for different table structures"""
    
    @staticmethod
    async def filter_date_range_payments_by_date(table, start_date: str, end_date: str):
        """
        Filter date range for payments_by_date table
        Row key structure: transaction_date#transaction_type#customerId
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
        """
        # Create row set for date range
        row_set = RowSet()
        
        # Add range from start_date to end_date
        # Note: \xff is used to ensure we get all entries for the end date
        row_set.add_row_range(
            start_key=start_date.encode('utf-8'),
            end_key=(end_date + '\xff').encode('utf-8')
        )
        
        results = []
        async for row in table.read_rows(row_set=row_set):
            results.append({
                'row_key': row.row_key.decode('utf-8'),
                'data': {
                    col_family: {
                        col: val[0].value.decode('utf-8')
                        for col, val in columns.items()
                    }
                    for col_family, columns in row.cells.items()
                }
            })
            
        return results

    @staticmethod
    async def filter_date_range_payments_by_customer(table, customer_id: str, 
                                                   start_date: str, end_date: str):
        """
        Filter date range for payments_by_customer table
        Row key structure: customerId#transaction_date#transaction_type
        
        Args:
            customer_id: Customer ID to filter
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
        """
        # Create row set for customer's date range
        row_set = RowSet()
        
        # Add range from customer's start_date to end_date
        row_set.add_row_range(
            start_key=f"{customer_id}#{start_date}".encode('utf-8'),
            end_key=f"{customer_id}#{end_date}\xff".encode('utf-8')
        )
        
        results = []
        async for row in table.read_rows(row_set=row_set):
            results.append({
                'row_key': row.row_key.decode('utf-8'),
                'data': {
                    col_family: {
                        col: val[0].value.decode('utf-8')
                        for col, val in columns.items()
                    }
                    for col_family, columns in row.cells.items()
                }
            })
            
        return results

    @staticmethod
    async def filter_date_range_payments_by_id_date(table, start_date: str, end_date: str):
        """
        Filter date range for payments_by_id_date table
        Row key structure: transaction_id#transaction_date
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
        """
        # For this table structure, we need to scan and filter
        row_set = RowSet()
        results = []
        
        async for row in table.read_rows():
            row_key = row.row_key.decode('utf-8')
            # Extract date from row key
            if '#' in row_key:
                _, date_part = row_key.split('#')
                if start_date <= date_part <= end_date:
                    results.append({
                        'row_key': row_key,
                        'data': {
                            col_family: {
                                col: val[0].value.decode('utf-8')
                                for col, val in columns.items()
                            }
                            for col_family, columns in row.cells.items()
                        }
                    })
                    
        return results

async def example_usage():
    # Initialize BigTable client
    instance = await create_bigtable_client()
    
    # Example date range
    start_date = "2024-03-01"
    end_date = "2024-03-31"
    customer_id = "CUST123"
    
    # Test on payments_by_date table
    date_table = instance.table('payments_by_date')
    date_results = await DateRangeExamples.filter_date_range_payments_by_date(
        date_table, start_date, end_date
    )
    print(f"Found {len(date_results)} transactions in date range")
    
    # Test on payments_by_customer table for specific customer
    customer_table = instance.table('payments_by_customer')
    customer_results = await DateRangeExamples.filter_date_range_payments_by_customer(
        customer_table, customer_id, start_date, end_date
    )
    print(f"Found {len(customer_results)} transactions for customer in date range")
    
    # Additional examples:
    
    # 1. Last 7 days
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    
    last_week_results = await DateRangeExamples.filter_date_range_payments_by_date(
        date_table, start_date, end_date
    )
    print(f"Found {len(last_week_results)} transactions in last 7 days")
    
    # 2. Current month to date
    start_date = datetime.now().strftime('%Y-%m-01')
    end_date = datetime.now().strftime('%Y-%m-%d')
    
    month_to_date_results = await DateRangeExamples.filter_date_range_payments_by_date(
        date_table, start_date, end_date
    )
    print(f"Found {len(month_to_date_results)} transactions this month")