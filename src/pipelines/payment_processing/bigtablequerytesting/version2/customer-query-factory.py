from enum import Enum
from customer_queries_complete import CustomerTableExecutor, DateTableExecutor, TransactionTableExecutor, IdTableExecutor, IdDateTableExecutor

class TableType(Enum):
    CUSTOMER = "payments_by_customer"
    DATE = "payments_by_date"
    TRANSACTION = "payments_by_transaction"
    ID = "payments_by_id"
    ID_DATE = "payments_by_id_date"

class CustomerQueryFactory:
    """Factory for creating customer query executors"""
    
    @staticmethod
    def create_executor(table_type: TableType, table) -> CustomerQueryExecutor:
        """
        Create appropriate query executor based on table type
        
        Args:
            table_type: Type of table
            table: Bigtable table instance
        """
        executors = {
            TableType.CUSTOMER: CustomerTableExecutor,
            TableType.DATE: DateTableExecutor,
            TableType.TRANSACTION: TransactionTableExecutor,
            TableType.ID: IdTableExecutor,
            TableType.ID_DATE: IdDateTableExecutor
        }
        
        executor_class = executors.get(table_type)
        if not executor_class:
            raise ValueError(f"No executor implemented for table type: {table_type}")
        
        return executor_class(table)

async def example_usage():
    """Example usage demonstrating all customer query patterns"""
    
    instance = await create_bigtable_client()
    
    # Test parameters
    customer_id = "CUST123"
    payment_type = "WIRE"
    date = "2024-03-15"
    start_date = "2024-03-01"
    end_date = "2024-03-31"
    
    # Create executors for each table type
    executors = {}
    for table_type in TableType:
        table = instance.table(table_type.value)
        executors[table_type] = CustomerQueryFactory.create_executor(table_type, table)
    
    # Test each query pattern on each table type
    query_patterns = [
        {
            "name": "Daily Type Transactions",
            "func": lambda e: e.get_daily_type_transactions(
                customer_id=customer_id,
                transaction_date=date,
                payment_type=payment_type
            )
        },
        {
            "name": "Date Range Transactions",
            "func": lambda e: e.get_date_range_transactions(
                customer_id=customer_id,
                start_date=start_date,
                end_date=end_date
            )
        },
        {
            "name": "Type Date Range Transactions",
            "func": lambda e: e.get_type_date_range_transactions(
                customer_id=customer_id,
                payment_type=payment_type,
                start_date=start_date,
                end_date=end_date
            )
        },
        {
            "name": "All Transactions",
            "func": lambda e: e.get_all_transactions(
                customer_id=customer_id
            )
        }
    ]
    
    # Run all patterns on all table types
    for pattern in query_patterns:
        print(f"\nTesting pattern: {pattern['name']}")
        print("=" * 50)
        
        for table_type, executor in executors.items():
            try:
                results = await pattern["func"](executor)
                print(f"\n{table_type.value}:")
                print(f"Found {len(results)} matching transactions")
                
                if results:
                    print("Sample transaction:")
                    print(json.dumps(results[0], indent=2))
                    
            except Exception as e:
                print(f"Error with {table_type.value}: {str(e)}")

async def performance_test():
    """Test performance characteristics of each implementation"""
    
    instance = await create_bigtable_client()
    executors = {}
    
    # Create executors
    for table_type in TableType:
        table = instance.table(table_type.value)
        executors[table_type] = CustomerQueryFactory.create_executor(table_type, table)
    
    # Test parameters
    test_cases = [
        {
            "name": "Single Day Query",
            "params": {
                "customer_id": "CUST123",
                "transaction_date": "2024-03-15",
                "payment_type": "WIRE"
            },
            "func": lambda e, p: e.get_daily_type_transactions(**p)
        },
        {
            "name": "Date Range Query",
            "params": {
                "customer_id": "CUST123",
                "start_date": "2024-03-01",
                "end_date": "2024-03-31"
            },
            "func": lambda e, p: e.get_date_range_transactions(**p)
        }
    ]
    
    # Run tests
    results = []
    for case in test_cases:
        print(f"\nTesting: {case['name']}")
        print("=" * 50)
        
        for table_type, executor in executors.items():
            try:
                start_time = time.time()
                results = await case["func"](executor, case["params"])
                execution_time = time.time() - start_time
                
                print(f"\n{table_type.value}:")
                print(f"Execution time: {execution_time:.3f} seconds")
                print(f"Results found: {len(results)}")
                
            except Exception as e:
                print(f"Error with {table_type.value}: {str(e)}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(example_usage())
    # asyncio.run(performance_test())  # Uncomment to run performance tests