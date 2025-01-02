from typing import List, Dict, Optional
from google.cloud.bigtable.data import row_filters,RowRange as QueryRowRange, ReadRowsQuery
from customer_query_base import CustomerQueryBase, BigtableConfig
import logging

logger = logging.getLogger(__name__)

class PaymentsByDateQuery(CustomerQueryBase):
    """Query implementation for payments_by_date table
    Row key pattern: transaction_date#transaction_type#customerId
    """
    
    def _create_transaction_filter(
        self,
        customer_id: str,
        transaction_types: List[str]
    ) -> row_filters.RowFilter:
        """Create filter for transaction_date#transaction_type#customerId pattern"""
        filters = []
        
        # # Always filter by customer ID since it's the last component
        # customer_filter = row_filters.RowKeyRegexFilter(
        #     f"[^#]*#[^#]*#{customer_id}$".encode('utf-8')
        # )
        # filters.append(customer_filter)
        
        # Add transaction type filter if specified
        if not transaction_types:
            # If no types specified, match any value
            # filters.append(row_filters.PassAllFilter(True))
            return row_filters.PassAllFilter(True)
        elif len(transaction_types) == 1:
            # Single type - use simple pattern
            type_cust_filter = row_filters.RowKeyRegexFilter(
                f"[^#]*#{transaction_types[0]}#{customer_id}".encode('utf-8')
            )
            return type_cust_filter
        else:
            # Multiple types - use union
            type_cust_filters = []
            for tx_type in transaction_types:
                pattern = f"[^#]*#{tx_type}#{customer_id}"
                type_cust_filters.append(
                    row_filters.RowKeyRegexFilter(pattern.encode('utf-8'))
                )
            return row_filters.RowFilterUnion(filters=type_cust_filters)
        
    async def get_customer_transactions_by_types(
        self,
        customer_id: str,
        transaction_types: List[str],
        start_date: str,
        end_date: str
    ) -> List[Dict]:
        """Get transactions by date range and type"""
        table = await self._get_table()
        
        # Create start and end keys with just the date component
        start_key = f"{start_date}#"
        end_key = f"{end_date}#"  # 'z' comes after '#' in ASCII, ensuring we get all entries for end_date
        
        logger.debug(f"Query range - Start: {start_key}, End: {end_key}")
        
        # Create row range for date filtering
        row_range = QueryRowRange(
            start_key=start_key.encode('utf-8'),
            end_key=end_key.encode('utf-8'),
            start_is_inclusive=True,
            end_is_inclusive=True
        )
        
        # Create filter chain for customer and transaction type filtering
        filter = self._create_transaction_filter(customer_id, transaction_types)
        
        # Create query combining row range and filters
        query = ReadRowsQuery(
            row_ranges=row_range,
            row_filter=filter
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

class PaymentsByTransactionQuery(CustomerQueryBase):
    """Query implementation for payments_by_transaction table
    Row key pattern: transaction_id#customerId#transaction_date
    """
    
    def _create_transaction_filter(
        self,
        customer_id: str,
        transaction_types: List[str]
    ) -> row_filters.RowFilter:
        """Create filter for transaction_id#customerId#transaction_date pattern"""
        # Create base filters for column family and qualifier
        filters = [
            # Filter for payment_header column family
            row_filters.FamilyNameRegexFilter("payment_header".encode('utf-8')),
            # Filter for payment_type qualifier
            row_filters.ColumnQualifierRegexFilter("payment_type".encode('utf-8'))
        ]
        
        # Add transaction type value filter if specified
        if not transaction_types:
            # If no types specified, match any value
            filters.append(row_filters.PassAllFilter(True))
        elif len(transaction_types) == 1:
            # Single type - use simple value filter
            filters.append(
                row_filters.ValueRegexFilter(transaction_types[0].encode('utf-8'))
            )
        else:
            # Multiple types - use union of value filters
            type_filters = []
            for tx_type in transaction_types:
                type_filters.append(
                    row_filters.ValueRegexFilter(tx_type.encode('utf-8'))
                )
            filters.append(row_filters.RowFilterUnion(filters=type_filters))
        
        # Combine all filters in a chain
        return row_filters.RowFilterChain(filters=filters)

    async def get_customer_transactions_by_types(
        self,
        customer_id: str,
        transaction_types: List[str],
        start_date: str,
        end_date: str
    ) -> List[Dict]:
        """Get transactions by customer and optionally filter by type"""
        table = await self._get_table()
        
        # Use customer ID in the row key pattern since it's the second component
        # Format: transaction_id#customerId#transaction_date
        start_key = f"#{customer_id}#{start_date}"
        end_key = f"#{customer_id}#{end_date}z"  # 'z' ensures we get all entries for end_date
        
        logger.debug(f"Query range - Start: {start_key}, End: {end_key}")
        
        # Create row range
        row_range = QueryRowRange(
            start_key=start_key.encode('utf-8'),
            end_key=end_key.encode('utf-8'),
            start_is_inclusive=True,
            end_is_inclusive=True
        )
        
        # Create transaction type filter if specified
        filter = self._create_transaction_filter(customer_id, transaction_types)
        
        query = ReadRowsQuery(
            row_ranges=row_range,
            row_filter=filter
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

class PaymentsByIdQuery(CustomerQueryBase):
    """Query implementation for payments_by_id table
    Row key pattern: transaction_id
    """
    
    def _create_transaction_filter(
        self,
        customer_id: str,
        transaction_types: List[str]
    ) -> row_filters.RowFilter:
        """Create filter for transaction_id pattern"""
        filters = []
        
        # Add customer filter on cell values
        filters.append(
            row_filters.ValueRegexFilter(customer_id.encode('utf-8'))
        )
        
        # Add transaction type filters if specified
        if not transaction_types:
            # If no types specified, match any value
            filters.append(row_filters.PassAllFilter(True))
        elif len(transaction_types) == 1:
            # Single type - use simple value filter
            filters.append(
                row_filters.ValueRegexFilter(transaction_types[0].encode('utf-8'))
            )
        else:
            # Multiple types - use union of value filters
            type_filters = []
            for tx_type in transaction_types:
                type_filters.append(
                    row_filters.ValueRegexFilter(tx_type.encode('utf-8'))
                )
            filters.append(row_filters.RowFilterUnion(filters=type_filters))
        
        return row_filters.RowFilterChain(filters=filters)

    async def get_customer_transactions_by_types(
        self,
        customer_id: str,
        transaction_types: List[str],
        start_date: str,
        end_date: str
    ) -> List[Dict]:
        """Get transactions by customer and type, filtering by date in cell values"""
        table = await self._get_table()
        
        # For this table, we scan all rows and filter by cell values
        filter = self._create_transaction_filter(customer_id, transaction_types)
        
        # Add date filter on cell values
        date_filter = row_filters.ValueRegexFilter(
            f"{start_date}.*{end_date}".encode('utf-8')
        )
        
        # Combine filters
        final_filter = row_filters.RowFilterChain(
            filters=[filter, date_filter]
        )
        
        query = ReadRowsQuery(row_filter=final_filter)
        
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

class PaymentsByIdDateQuery(CustomerQueryBase):
    """Query implementation for payments_by_id_date table
    Row key pattern: transaction_id#transaction_date
    """
    
    def _create_transaction_filter(
        self,
        customer_id: str,
        transaction_types: List[str]
    ) -> row_filters.RowFilter:
        """Create filter for transaction_id#transaction_date pattern"""
        filters = []
        
        # Filter by customer in cell values
        filters.append(
            row_filters.ValueRegexFilter(customer_id.encode('utf-8'))
        )
        
        # Add transaction type filters if specified
        if not transaction_types:
            # If no types specified, match any value
            filters.append(row_filters.PassAllFilter(True))
        elif len(transaction_types) == 1:
            # Single type - use simple value filter
            filters.append(
                row_filters.ValueRegexFilter(transaction_types[0].encode('utf-8'))
            )
        else:
            # Multiple types - use union of value filters
            type_filters = []
            for tx_type in transaction_types:
                type_filters.append(
                    row_filters.ValueRegexFilter(tx_type.encode('utf-8'))
                )
            filters.append(row_filters.RowFilterUnion(filters=type_filters))
        
        return row_filters.RowFilterChain(filters=filters)

    async def get_customer_transactions_by_types(
        self,
        customer_id: str,
        transaction_types: List[str],
        start_date: str,
        end_date: str
    ) -> List[Dict]:
        """Get transactions by date range, filtering customer and type in cell values"""
        table = await self._get_table()
        
        # Create date range from row key pattern
        start_key = f".*#{start_date}"
        end_key = f".*#{end_date}~"  # ~ ensures we get all entries up to this date
        
        logger.debug(f"Query range - Start: {start_key}, End: {end_key}")
        
        # Create row range
        row_range = QueryRowRange(
            start_key=start_key.encode('utf-8'),
            end_key=end_key.encode('utf-8'),
            start_is_inclusive=True,
            end_is_inclusive=True
        )
        
        # Create filter for customer and transaction types
        filter = self._create_transaction_filter(customer_id, transaction_types)
        
        query = ReadRowsQuery(
            row_ranges=row_range,
            row_filter=filter
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

class PaymentsByCustomerQuery(CustomerQueryBase):
    """Query implementation for payments_by_customer table
    Row key pattern: customerId#transaction_date#transaction_type
    """
    
    def _create_transaction_filter(
        self,
        customer_id: str,
        transaction_types: List[str]
    ) -> row_filters.RowFilter:
        """Create filter for customerId#transaction_date#transaction_type pattern"""
        if not transaction_types:
            # If no transaction types specified, match any transaction type
            pattern = f"{customer_id}#[^#]*#[^#]*$"
            return row_filters.RowKeyRegexFilter(pattern.encode('utf-8'))
        
        elif len(transaction_types) == 1:
            # If only one transaction type, use a simple regex filter
            pattern = f"{customer_id}#[^#]*#{transaction_types[0]}$"
            return row_filters.RowKeyRegexFilter(pattern.encode('utf-8'))
        
        else:
            # For multiple types, create a union of filters
            type_filters = []
            for tx_type in transaction_types:
                pattern = f"{customer_id}#[^#]*#{tx_type}$"
                type_filters.append(row_filters.RowKeyRegexFilter(pattern.encode('utf-8')))

            # Use RowFilterUnion for OR logic between transaction types
            return row_filters.RowFilterUnion(filters=type_filters)

    async def get_customer_transactions_by_types(
        self,
        customer_id: str,
        transaction_types: List[str],
        start_date: str,
        end_date: str
    ) -> List[Dict]:
        """Get customer transactions for multiple transaction types within a date range"""
        table = await self._get_table()
        
        # Create start and end keys
        start_key = f"{customer_id}#{start_date}"
        end_key = f"{customer_id}#{end_date}"

        logger.debug(f"Query range - Start: {start_key}, End: {end_key}")
        
        # Create row range
        row_range = QueryRowRange(
            start_key=start_key.encode('utf-8'),
            end_key=end_key.encode('utf-8'),
            start_is_inclusive=True,
            end_is_inclusive=True
        )
        
        # Create query based on transaction types
        filter = self._create_transaction_filter(customer_id, transaction_types)
        query = ReadRowsQuery(
            row_ranges=row_range,
            row_filter=filter
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
    """Example usage of different query implementations"""
    # Create configuration
    config = BigtableConfig(
        project_id="agentic-experiments-446019",
        instance_id="payment-processing-dev",
        table_id="payments_by_date"
    )
    
    # Test each implementation
    implementations = [
        ("PaymentsByCustomer", PaymentsByCustomerQuery(
            BigtableConfig(**{**config.__dict__, 'table_id': 'payments_by_customer'})
        )),
        ("PaymentsByDate", PaymentsByDateQuery(config)),
        # ("PaymentsByTransaction", PaymentsByTransactionQuery(
        #     BigtableConfig(**{**config.__dict__, 'table_id': 'payments_by_transaction'})
        # )),
        # # ("PaymentsById", PaymentsByIdQuery(
        # #     BigtableConfig(**{**config.__dict__, 'table_id': 'payments_by_id'})
        # # )),
        # # ("PaymentsByIdDate", PaymentsByIdDateQuery(
        # #     BigtableConfig(**{**config.__dict__, 'table_id': 'payments_by_id_date'})
        # ))
    ] #
    
    customer_id = "CUST000001"
    transaction_types = ["WIRE", "ACH"]
    # transaction_types = []
    start_date = "2024-03-01T00:00:00"
    end_date = "2024-03-01T23:59:59"
    
    print("\nExecuting queries and collecting metrics...")
    print("-" * 80)
    
    metrics_summary = []
    for name, implementation in implementations:
        try:
            results = await implementation.get_customer_transactions_by_types(
                customer_id=customer_id,
                transaction_types=transaction_types,
                start_date=start_date,
                end_date=end_date
            )
            print(f"\n Results for {name}: {len(results)}")
            
            # metrics = implementation.last_query_metrics
            # metrics_summary.append({
            #     'name': name,
            #     'duration_ms': metrics.execution_time_ms,
            #     'row_count': metrics.row_count,
            #     'error': metrics.error
            # })
            
        except Exception as e:
            print(f"Error testing {name}: {e}")
    
    # # Print performance comparison
    # print("\nPerformance Comparison:")
    # print("-" * 80)
    # print(f"{'Query Type':<25} {'Duration (ms)':<15} {'Rows':<10} {'Status'}")
    # print("-" * 80)
    
    # # Sort by execution time
    # metrics_summary.sort(key=lambda x: x['duration_ms'])
    
    # for metric in metrics_summary:
    #     status = "ERROR" if metric['error'] else "OK"
    #     print(
    #         f"{metric['name']:<25} "
    #         f"{metric['duration_ms']:>13.2f}ms "
    #         f"{metric['row_count']:>10} "
    #         f"{status:>8}"
    #     )
    
    # print("-" * 80)
    
    # # Print fastest and slowest
    # if metrics_summary:
    #     fastest = metrics_summary[0]
    #     slowest = metrics_summary[-1]
    #     print(f"\nFastest: {fastest['name']} ({fastest['duration_ms']:.2f}ms)")
    #     print(f"Slowest: {slowest['name']} ({slowest['duration_ms']:.2f}ms)")
    #     print(f"Difference: {slowest['duration_ms'] - fastest['duration_ms']:.2f}ms")

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
