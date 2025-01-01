from google.cloud.bigtable import row_filters
from google.cloud.bigtable.row_set import RowSet
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import logging

class TableType(Enum):
    CUSTOMER = "payments_by_customer"        # customerId#transaction_date#transaction_type
    DATE = "payments_by_date"               # transaction_date#transaction_type#customerId
    TRANSACTION = "payments_by_transaction"  # transaction_id#customerId#transaction_date
    ID = "payments_by_id"                   # transaction_id
    ID_DATE = "payments_by_id_date"         # transaction_id#transaction_date

@dataclass
class QueryParameters:
    """Container for query parameters"""
    customer_id: Optional[str] = None
    transaction_types: Optional[List[str]] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None

class TransactionQuery:
    """Query handler for all table types"""
    
    def __init__(self, table, table_type: TableType):
        self.table = table
        self.table_type = table_type
        self.logger = logging.getLogger(__name__)

    def _build_row_set_and_filters(self, params: QueryParameters) -> Tuple[Optional[RowSet], Optional[row_filters.RowFilter]]:
        """
        Build appropriate row set and filters based on table type and parameters
        """
        row_set = RowSet()
        filters = []

        if self.table_type == TableType.CUSTOMER:
            # customerId#transaction_date#transaction_type
            if params.customer_id:
                if params.start_date and params.end_date:
                    start_key = f"{params.customer_id}#{params.start_date}"
                    end_key = f"{params.customer_id}#{params.end_date}\xff"
                    row_set.add_row_range(
                        start_key=start_key.encode('utf-8'),
                        end_key=end_key.encode('utf-8')
                    )
                else:
                    row_set.add_row_range_from_prefix(params.customer_id.encode('utf-8'))

        elif self.table_type == TableType.DATE:
            # transaction_date#transaction_type#customerId
            if params.start_date and params.end_date:
                start_key = params.start_date
                end_key = f"{params.end_date}\xff"
                row_set.add_row_range(
                    start_key=start_key.encode('utf-8'),
                    end_key=end_key.encode('utf-8')
                )
            
            if params.customer_id:
                filters.append(row_filters.RowKeyRegexFilter(
                    f".*#{params.customer_id}$".encode('utf-8')
                ))

        elif self.table_type == TableType.TRANSACTION:
            # transaction_id#customerId#transaction_date
            if params.customer_id:
                filters.append(row_filters.RowKeyRegexFilter(
                    f".*#{params.customer_id}#.*".encode('utf-8')
                ))
            
            if params.start_date and params.end_date:
                date_filter = row_filters.RowKeyRegexFilter(
                    f".*#[{params.start_date}-{params.end_date}]".encode('utf-8')
                )
                filters.append(date_filter)

        elif self.table_type == TableType.ID_DATE:
            # transaction_id#transaction_date
            if params.start_date and params.end_date:
                start_key = f"#[{params.start_date}"
                end_key = f"#{params.end_date}\xff"
                row_set.add_row_range(
                    start_key=start_key.encode('utf-8'),
                    end_key=end_key.encode('utf-8')
                )

        # Add transaction type filters if specified
        if params.transaction_types:
            type_filters = []
            for tx_type in params.transaction_types:
                if self.table_type in [TableType.CUSTOMER, TableType.DATE]:
                    # For tables with transaction_type in row key
                    type_filters.append(
                        row_filters.RowKeyRegexFilter(f".*#{tx_type}".encode('utf-8'))
                    )
                else:
                    # For tables storing transaction_type in column
                    type_filters.append(
                        row_filters.ValueRegexFilter(tx_type.encode('utf-8'))
                    )
            
            if type_filters:
                filters.append(row_filters.RowFilterUnion(filters=type_filters))

        # Combine all filters with AND logic if multiple exist
        final_filter = None
        if filters:
            if len(filters) == 1:
                final_filter = filters[0]
            else:
                final_filter = row_filters.RowFilterChain(filters=filters)

        return row_set, final_filter

    async def query_transactions(self, params: QueryParameters) -> List[Dict]:
        """
        Query transactions across any table type with given parameters
        
        Args:
            params: QueryParameters object containing search criteria
            
        Returns:
            List of matching transactions
        """
        self.logger.info(f"Querying {self.table_type.value} with parameters: {params}")
        
        row_set, filter_ = self._build_row_set_and_filters(params)
        
        results = []
        try:
            async for row in self.table.read_rows(row_set=row_set, filter_=filter_):
                row_key = row.row_key.decode('utf-8')
                
                # Parse data based on table type
                parsed_data = self._parse_row_data(row)
                results.append(parsed_data)
                
        except Exception as e:
            self.logger.error(f"Error querying {self.table_type.value}: {str(e)}")
            raise
        
        return results

    def _parse_row_data(self, row) -> Dict:
        """Parse row data based on table type"""
        row_key = row.row_key.decode('utf-8')
        data = {
            col_family: {
                col: val[0].value.decode('utf-8')
                for col, val in columns.items()
            }
            for col_family, columns in row.cells.items()
        }
        
        result = {
            'row_key': row_key,
            'data': data
        }
        
        # Parse key components based on table type
        if self.table_type == TableType.CUSTOMER:
            customer_id, timestamp, tx_type = row_key.split('#')
            result.update({
                'customer_id': customer_id,
                'timestamp': timestamp,
                'transaction_type': tx_type
            })
        elif self.table_type == TableType.DATE:
            timestamp, tx_type, customer_id = row_key.split('#')
            result.update({
                'customer_id': customer_id,
                'timestamp': timestamp,
                'transaction_type': tx_type
            })
        elif self.table_type == TableType.TRANSACTION:
            tx_id, customer_id, timestamp = row_key.split('#')
            result.update({
                'transaction_id': tx_id,
                'customer_id': customer_id,
                'timestamp': timestamp
            })
        elif self.table_type == TableType.ID_DATE:
            tx_id, timestamp = row_key.split('#')
            result.update({
                'transaction_id': tx_id,
                'timestamp': timestamp
            })
        elif self.table_type == TableType.ID:
            result.update({
                'transaction_id': row_key
            })
        
        return result

async def example_usage():
    """Example usage across different table types"""
    
    instance = await create_bigtable_client()
    
    # Test parameters
    params = QueryParameters(
        customer_id="CUST123",
        transaction_types=["WIRE", "ACH"],  # Optional
        start_date="2024-03-01",
        end_date="2024-03-31"
    )
    
    # Test on each table type
    for table_type in TableType:
        table = instance.table(table_type.value)
        query = TransactionQuery(table, table_type)
        
        print(f"\nQuerying {table_type.value}:")
        try:
            results = await query.query_transactions(params)
            print(f"Found {len(results)} matching transactions")
            
            # Show sample result
            if results:
                print("Sample transaction:")
                print(results[0])
                
        except Exception as e:
            print(f"Error querying {table_type.value}: {str(e)}")

async def main():
    """Main function demonstrating different query scenarios"""
    
    instance = await create_bigtable_client()
    
    # Scenario 1: Query with all parameters
    params1 = QueryParameters(
        customer_id="CUST123",
        transaction_types=["WIRE", "ACH"],
        start_date="2024-03-01",
        end_date="2024-03-31"
    )
    
    # Scenario 2: Query without transaction types
    params2 = QueryParameters(
        customer_id="CUST123",
        start_date="2024-03-01",
        end_date="2024-03-31"
    )
    
    # Scenario 3: Query with only customer_id
    params3 = QueryParameters(
        customer_id="CUST123"
    )
    
    # Test each scenario on all table types
    scenarios = [
        ("All Parameters", params1),
        ("Without Transaction Types", params2),
        ("Only Customer ID", params3)
    ]
    
    for scenario_name, params in scenarios:
        print(f"\n{scenario_name}:")
        print("=" * 50)
        
        for table_type in TableType:
            table = instance.table(table_type.value)
            query = TransactionQuery(table, table_type)
            
            try:
                results = await query.query_transactions(params)
                print(f"\n{table_type.value}:")
                print(f"Found {len(results)} matching transactions")
            except Exception as e:
                print(f"Error with {table_type.value}: {str(e)}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())