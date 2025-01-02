from typing import List, Dict, Optional
from google.cloud.bigtable.data import row_filters, QueryRowRange, ReadRowsQuery
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
        if not transaction_types:
            # Match any transaction type for the customer
            pattern = f"[^#]*#[^#]*#{customer_id}$"
            return row_filters.RowKeyRegexFilter(pattern.encode('utf-8'))
        
        elif len(transaction_types) == 1:
            # Match specific transaction type
            pattern = f"[^#]*#{transaction_types[0]}#{customer_id}$"
            return row_filters.RowKeyRegexFilter(pattern.encode('utf-8'))
        
        else:
            # Create union of filters for multiple types
            type_filters = []
            for tx_type in transaction_types:
                pattern = f"[^#]*#{tx_type}#{customer_id}$"
                type_filters.append(row_filters.RowKeyRegexFilter(pattern.encode('utf-8')))
            return row_filters.RowFilterUnion(filters=type_filters)

    async def get_customer_transactions_by_types(
        self,
        customer_id: str,
        transaction_types: List[str],
        start_date: str,
        end_date: str
    ) -> List[Dict]:
        """Get transactions by date range and type"""
        table = await self._get_table()
        
        # Create start and end keys based on date pattern
        start_key = f"{start_date}"  # Will match all types and customers from this date
        end_key = f"{end_date}#{'~' * 100}"  # '~' ensures we get all entries up to this date
        
        logger.debug(f"Query range - Start: {start_key}, End: {end_key}")
        
        # Create row range
        row_range = QueryRowRange(
            start_key=start_key.encode('utf-8'),
            end_key=end_key.encode('utf-8'),
            start_is_inclusive=True,
            end_is_inclusive=True
        )
        
        # Create query with customer and type filters
        filter = self._create_transaction_filter(customer_id, transaction_types)
        query = ReadRowsQuery(
            row_ranges=row_range,
            row_filter=filter
        )
        
        results = []
        try:
            rows = await table.read_rows(query)
            async for row in rows:
                results.append(self._parse_row(row))
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
        # For this table, we'll need to filter transaction types from the cell values
        # since they're not in the row key
        type_filters = []
        
        if transaction_types:
            for tx_type in transaction_types:
                type_filters.append(
                    row_filters.ValueRegexFilter(tx_type.encode('utf-8'))
                )
            type_filter = row_filters.RowFilterUnion(filters=type_filters)
        else:
            type_filter = row_filters.PassAllFilter(True)
        
        # Create customer filter
        customer_filter = row_filters.RowKeyRegexFilter(
            f"[^#]*#{customer_id}#".encode('utf-8')
        )
        
        # Combine filters
        return row_filters.RowFilterChain(filters=[customer_filter, type_filter])

    async def get_customer_transactions_by_types(
        self,
        customer_id: str,
        transaction_types: List[str],
        start_date: str,
        end_date: str
    ) -> List[Dict]:
        """Get transactions by customer and optionally filter by type"""
        table = await self._get_table()
        
        # For this table, we'll filter by date using the suffix
        start_key = f"#"  # Start from beginning
        end_key = f"~"    # Go to end
        
        logger.debug(f"Query range - Start: {start_key}, End: {end_key}")
        
        # Create row range
        row_range = QueryRowRange(
            start_key=start_key.encode('utf-8'),
            end_key=end_key.encode('utf-8'),
            start_is_inclusive=True,
            end_is_inclusive=True
        )
        
        # Create combined filter for customer, type, and date
        filter = self._create_transaction_filter(customer_id, transaction_types)
        
        # Add date range filter
        date_filter = row_filters.RowKeyRegexFilter(
            f"[^#]*#{customer_id}#{start_date}.*{end_date}".encode('utf-8')
        )
        
        # Combine with main filter
        final_filter = row_filters.RowFilterChain(filters=[filter, date_filter])
        
        query = ReadRowsQuery(
            row_ranges=row_range,
            row_filter=final_filter
        )
        
        results = []
        try:
            rows = await table.read_rows(query)
            async for row in rows:
                results.append(self._parse_row(row))
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
        if transaction_types:
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
            async for row in rows:
                results.append(self._parse_row(row))
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
        if transaction_types:
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
            async for row in rows:
                results.append(self._parse_row(row))
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
            async for row in rows:
                results.append(self._parse_row(row))
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
        table_id="payments_by_date",  # Change this for different implementations
        read_rows_limit=1000,
        timeout=30.0
    )
    
    # Test each implementation
    implementations = [
        ("PaymentsByCustomer", PaymentsByCustomerQuery(
            BigtableConfig(**{**config.__dict__, 'table_id': 'payments_by_customer'})
        )),
        ("PaymentsByDate", PaymentsByDateQuery(config)),
        ("PaymentsByTransaction", PaymentsByTransactionQuery(
            BigtableConfig(**{**config.__dict__, 'table_id': 'payments_by_transaction'})
        )),
        ("PaymentsById", PaymentsByIdQuery(
            BigtableConfig(**{**config.__dict__, 'table_id': 'payments_by_id'})
        )),
        ("PaymentsByIdDate", PaymentsByIdDateQuery(
            BigtableConfig(**{**config.__dict__, 'table_id': 'payments_by_id_date'})
        ))
    ]
    
    customer_id = "CUST000001"
    transaction_types = ["WIRE", "ACH"]
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
            
            metrics = implementation.last_query_metrics
            metrics_summary.append({
                'name': name,
                'duration_ms': metrics.execution_time_ms,
                'row_count': metrics.row_count,
                'error': metrics.error
            })
            
        except Exception as e:
            print(f"Error testing {name}: {e}")
    
    # Print performance comparison
    print("\nPerformance Comparison:")
    print("-" * 80)
    print(f"{'Query Type':<25} {'Duration (ms)':<15} {'Rows':<10} {'Status'}")
    print("-" * 80)
    
    # Sort by execution time
    metrics_summary.sort(key=lambda x: x['duration_ms'])
    
    for metric in metrics_summary:
        status = "ERROR" if metric['error'] else "OK"
        print(
            f"{metric['name']:<25} "
            f"{metric['duration_ms']:>13.2f}ms "
            f"{metric['row_count']:>10} "
            f"{status:>8}"
        )
    
    print("-" * 80)
    
    # Print fastest and slowest
    if metrics_summary:
        fastest = metrics_summary[0]
        slowest = metrics_summary[-1]
        print(f"\nFastest: {fastest['name']} ({fastest['duration_ms']:.2f}ms)")
        print(f"Slowest: {slowest['name']} ({slowest['duration_ms']:.2f}ms)")
        print(f"Difference: {slowest['duration_ms'] - fastest['duration_ms']:.2f}ms")

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
