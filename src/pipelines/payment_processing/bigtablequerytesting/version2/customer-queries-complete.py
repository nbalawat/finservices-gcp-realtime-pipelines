from abc import ABC, abstractmethod
from typing import List, Dict, Optional, Set
from google.cloud.bigtable import row_filters
from google.cloud.bigtable.row_set import RowSet
import logging

class CustomerQueryExecutor(ABC):
    """Abstract base class for customer-centric query implementations"""
    
    def __init__(self, table):
        self.table = table
        self.logger = logging.getLogger(__name__)

    @abstractmethod
    async def get_daily_type_transactions(
        self,
        customer_id: str,
        transaction_date: str,
        payment_type: str
    ) -> List[Dict]:
        """
        Requirement 1: For a given customer find transactions of a particular payment type on a given day
        """
        pass

    @abstractmethod
    async def get_date_range_transactions(
        self,
        customer_id: str,
        start_date: str,
        end_date: str
    ) -> List[Dict]:
        """
        Requirement 2: For a given customer find all transactions over a given date range
        """
        pass

    @abstractmethod
    async def get_type_date_range_transactions(
        self,
        customer_id: str,
        payment_type: str,
        start_date: str,
        end_date: str
    ) -> List[Dict]:
        """
        Requirement 3: For a given customer find all transactions of a particular type over a given date range
        """
        pass

    @abstractmethod
    async def get_all_transactions(
        self,
        customer_id: str
    ) -> List[Dict]:
        """
        Requirement 4: For a given customer find all transactions
        """
        pass

    def _parse_row(self, row) -> Dict:
        """Base row parsing logic"""
        cells = []
        for cell in row.get_cells():
            cells.append({
                'column_family': cell.family,
                'qualifier': cell.qualifier.decode('utf-8'),
                'value': cell.value.decode('utf-8'),
                'timestamp': cell.timestamp_micros
            })
            
        return {
            'row_key': row.row_key.decode('utf-8'),
            'cells': cells
        }

class CustomerTableExecutor(CustomerQueryExecutor):
    """
    Implementation for payments_by_customer table
    Row key pattern: customerId#transaction_date#transaction_type
    """

    async def get_daily_type_transactions(
        self,
        customer_id: str,
        transaction_date: str,
        payment_type: str
    ) -> List[Dict]:
        """Most efficient implementation - direct key match"""
        row_key = f"{customer_id}#{transaction_date}#{payment_type}"
        row_set = RowSet()
        row_set.add_row_range_from_prefix(row_key.encode('utf-8'))
        
        results = []
        async for row in self.table.read_rows(row_set=row_set):
            results.append(self._parse_row(row))
        return results

    async def get_date_range_transactions(
        self,
        customer_id: str,
        start_date: str,
        end_date: str
    ) -> List[Dict]:
        """Efficient implementation - range scan"""
        start_key = f"{customer_id}#{start_date}"
        end_key = f"{customer_id}#{end_date}\xff"
        
        row_set = RowSet()
        row_set.add_row_range(
            start_key=start_key.encode('utf-8'),
            end_key=end_key.encode('utf-8')
        )
        
        results = []
        async for row in self.table.read_rows(row_set=row_set):
            results.append(self._parse_row(row))
        return results

    async def get_type_date_range_transactions(
        self,
        customer_id: str,
        payment_type: str,
        start_date: str,
        end_date: str
    ) -> List[Dict]:
        """Efficient implementation - range scan with type filter"""
        start_key = f"{customer_id}#{start_date}"
        end_key = f"{customer_id}#{end_date}\xff"
        
        row_set = RowSet()
        row_set.add_row_range(
            start_key=start_key.encode('utf-8'),
            end_key=end_key.encode('utf-8')
        )
        
        type_filter = row_filters.RowKeyRegexFilter(
            f".*#{payment_type}$".encode('utf-8')
        )
        
        results = []
        async for row in self.table.read_rows(row_set=row_set, filter_=type_filter):
            results.append(self._parse_row(row))
        return results

    async def get_all_transactions(
        self,
        customer_id: str
    ) -> List[Dict]:
        """Efficient implementation - prefix scan"""
        row_set = RowSet()
        row_set.add_row_range_from_prefix(customer_id.encode('utf-8'))
        
        results = []
        async for row in self.table.read_rows(row_set=row_set):
            results.append(self._parse_row(row))
        return results

    def _parse_row(self, row) -> Dict:
        """Parse row with customer table structure"""
        base_data = super()._parse_row(row)
        key_parts = row.row_key.decode('utf-8').split('#')
        if len(key_parts) >= 3:
            base_data.update({
                'customer_id': key_parts[0],
                'transaction_date': key_parts[1],
                'transaction_type': key_parts[2]
            })
        return base_data

class DateTableExecutor(CustomerQueryExecutor):
    """
    Implementation for payments_by_date table
    Row key pattern: transaction_date#transaction_type#customerId
    """

    async def get_daily_type_transactions(
        self,
        customer_id: str,
        transaction_date: str,
        payment_type: str
    ) -> List[Dict]:
        """Direct key match - moderately efficient"""
        row_key = f"{transaction_date}#{payment_type}#{customer_id}"
        row_set = RowSet()
        row_set.add_row_key(row_key.encode('utf-8'))
        
        results = []
        async for row in self.table.read_rows(row_set=row_set):
            results.append(self._parse_row(row))
        return results

    async def get_date_range_transactions(
        self,
        customer_id: str,
        start_date: str,
        end_date: str
    ) -> List[Dict]:
        """Range scan with customer filter"""
        row_set = RowSet()
        row_set.add_row_range(
            start_key=start_date.encode('utf-8'),
            end_key=(end_date + '\xff').encode('utf-8')
        )
        
        customer_filter = row_filters.RowKeyRegexFilter(
            f".*#{customer_id}$".encode('utf-8')
        )
        
        results = []
        async for row in self.table.read_rows(row_set=row_set, filter_=customer_filter):
            results.append(self._parse_row(row))
        return results

    async def get_type_date_range_transactions(
        self,
        customer_id: str,
        payment_type: str,
        start_date: str,
        end_date: str
    ) -> List[Dict]:
        """Range scan with type and customer filters"""
        row_set = RowSet()
        row_set.add_row_range(
            start_key=start_date.encode('utf-8'),
            end_key=(end_date + '\xff').encode('utf-8')
        )
        
        filters = [
            row_filters.RowKeyRegexFilter(f".*#{payment_type}#.*".encode('utf-8')),
            row_filters.RowKeyRegexFilter(f".*#{customer_id}$".encode('utf-8'))
        ]
        filter_chain = row_filters.RowFilterChain(filters=filters)
        
        results = []
        async for row in self.table.read_rows(row_set=row_set, filter_=filter_chain):
            results.append(self._parse_row(row))
        return results

    async def get_all_transactions(
        self,
        customer_id: str
    ) -> List[Dict]:
        """Full scan with customer filter - less efficient"""
        customer_filter = row_filters.RowKeyRegexFilter(
            f".*#{customer_id}$".encode('utf-8')
        )
        
        results = []
        async for row in self.table.read_rows(filter_=customer_filter):
            results.append(self._parse_row(row))
        return results

    def _parse_row(self, row) -> Dict:
        """Parse row with date table structure"""
        base_data = super()._parse_row(row)
        key_parts = row.row_key.decode('utf-8').split('#')
        if len(key_parts) >= 3:
            base_data.update({
                'transaction_date': key_parts[0],
                'transaction_type': key_parts[1],
                'customer_id': key_parts[2]
            })
        return base_data

class TransactionTableExecutor(CustomerQueryExecutor):
    """
    Implementation for payments_by_transaction table
    Row key pattern: transaction_id#customerId#transaction_date
    """

    async def get_daily_type_transactions(
        self,
        customer_id: str,
        transaction_date: str,
        payment_type: str
    ) -> List[Dict]:
        """Less efficient - requires scanning and multiple filters"""
        filters = [
            row_filters.RowKeyRegexFilter(f".*#{customer_id}#{transaction_date}".encode('utf-8')),
            row_filters.ValueRegexFilter(payment_type.encode('utf-8'))
        ]
        filter_chain = row_filters.RowFilterChain(filters=filters)
        
        results = []
        async for row in self.table.read_rows(filter_=filter_chain):
            results.append(self._parse_row(row))
        return results

    async def get_date_range_transactions(
        self,
        customer_id: str,
        start_date: str,
        end_date: str
    ) -> List[Dict]:
        """Using customer and date filters"""
        customer_filter = row_filters.RowKeyRegexFilter(
            f".*#{customer_id}#.*".encode('utf-8')
        )
        
        # Create a pattern that matches dates in the range
        date_pattern = f".*#{customer_id}#[{start_date}-{end_date}]"
        date_filter = row_filters.RowKeyRegexFilter(date_pattern.encode('utf-8'))
        
        filter_chain = row_filters.RowFilterChain(filters=[customer_filter, date_filter])
        
        results = []
        async for row in self.table.read_rows(filter_=filter_chain):
            results.append(self._parse_row(row))
        return results

    async def get_type_date_range_transactions(
        self,
        customer_id: str,
        payment_type: str,
        start_date: str,
        end_date: str
    ) -> List[Dict]:
        """Complex filtering required"""
        # Filter for customer ID in row key
        customer_filter = row_filters.RowKeyRegexFilter(
            f".*#{customer_id}#.*".encode('utf-8')
        )
        
        # Filter for date range in row key
        date_pattern = f".*#{customer_id}#[{start_date}-{end_date}]"
        date_filter = row_filters.RowKeyRegexFilter(date_pattern.encode('utf-8'))
        
        # Filter for payment type in column value
        type_filter = row_filters.ValueRegexFilter(payment_type.encode('utf-8'))
        
        filter_chain = row_filters.RowFilterChain(
            filters=[customer_filter, date_filter, type_filter]
        )
        
        results = []
        async for row in self.table.read_rows(filter_=filter_chain):
            results.append(self._parse_row(row))
        return results

    async def get_all_transactions(
        self,
        customer_id: str
    ) -> List[Dict]:
        """Using customer filter"""
        customer_filter = row_filters.RowKeyRegexFilter(
            f".*#{customer_id}#.*".encode('utf-8')
        )
        
        results = []
        async for row in self.table.read_rows(filter_=customer_filter):
            results.append(self._parse_row(row))
        return results

    def _parse_row(self, row) -> Dict:
        """Parse row with transaction table structure"""
        base_data = super()._parse_row(row)
        key_parts = row.row_key.decode('utf-8').split('#')
        if len(key_parts) >= 3:
            base_data.update({
                'transaction_id': key_parts[0],
                'customer_id': key_parts[1],
                'transaction_date': key_parts[2]
            })
            # Add transaction type from column
            if 'cf1' in row.cells and 'payment_type' in row.cells['cf1']:
                base_data['transaction_type'] = row.cells['cf1']['payment_type'][0].value.decode('utf-8')
        return base_data

class IdTableExecutor(CustomerQueryExecutor):
    """
    Implementation for payments_by_id table
    Row key pattern: transaction_id
    """
    
    async def get_date_range_transactions(
        self,
        customer_id: str,
        start_date: str,
        end_date: str
    ) -> List[Dict]:
        """Full scan with customer and date filtering"""
        results = []
        async for row in self.table.read_rows():
            cell_values = row.cells["cf1"]
            row_customer_id = cell_values["customer_id"][0].value.decode('utf-8')
            transaction_date = cell_values["transaction_date"][0].value.decode('utf-8')
            
            if (row_customer_id == customer_id and 
                start_date <= transaction_date <= end_date):
                results.append(self._parse_row(row))
        return results

    async def get_type_date_range_transactions(
        self,
        customer_id: str,
        payment_type: str,
        start_date: str,
        end_date: str
    ) -> List[Dict]:
        """Full scan with customer, type, and date filtering"""
        results = []
        async for row in self.table.read_rows():
            cell_values = row.cells["cf1"]
            row_customer_id = cell_values["customer_id"][0].value.decode('utf-8')
            transaction_date = cell_values["transaction_date"][0].value.decode('utf-8')
            row_payment_type = cell_values["payment_type"][0].value.decode('utf-8')
            
            if (row_customer_id == customer_id and 
                start_date <= transaction_date <= end_date and
                row_payment_type == payment_type):
                results.append(self._parse_row(row))
        return results

    async def get_all_transactions(
        self,
        customer_id: str
    ) -> List[Dict]:
        """Full scan with customer filtering"""
        customer_filter = row_filters.ValueRegexFilter(
            customer_id.encode('utf-8')
        )
        
        results = []
        async for row in self.table.read_rows(filter_=customer_filter):
            results.append(self._parse_row(row))
        return results

    def _parse_row(self, row) -> Dict:
        """Parse row with ID table structure"""
        base_data = super()._parse_row(row)
        base_data['transaction_id'] = row.row_key.decode('utf-8')
        
        # Add data from columns
        if 'cf1' in row.cells:
            cell_values = row.cells['cf1']
            if 'customer_id' in cell_values:
                base_data['customer_id'] = cell_values['customer_id'][0].value.decode('utf-8')
            if 'transaction_date' in cell_values:
                base_data['transaction_date'] = cell_values['transaction_date'][0].value.decode('utf-8')
            if 'payment_type' in cell_values:
                base_data['transaction_type'] = cell_values['payment_type'][0].value.decode('utf-8')
        return base_data

class IdDateTableExecutor(CustomerQueryExecutor):
    """
    Implementation for payments_by_id_date table
    Row key pattern: transaction_id#transaction_date
    """

    async def get_daily_type_transactions(
        self,
        customer_id: str,
        transaction_date: str,
        payment_type: str
    ) -> List[Dict]:
        """Scan by date and filter by customer and type"""
        row_set = RowSet()
        row_set.add_row_range_from_prefix(f"#{transaction_date}".encode('utf-8'))
        
        filters = [
            row_filters.ValueRegexFilter(customer_id.encode('utf-8')),
            row_filters.ValueRegexFilter(payment_type.encode('utf-8'))
        ]
        filter_chain = row_filters.RowFilterChain(filters=filters)
        
        results = []
        async for row in self.table.read_rows(row_set=row_set, filter_=filter_chain):
            results.append(self._parse_row(row))
        return results

    async def get_date_range_transactions(
        self,
        customer_id: str,
        start_date: str,
        end_date: str
    ) -> List[Dict]:
        """Range scan on dates with customer filter"""
        row_set = RowSet()
        start_key = f"#{start_date}"
        end_key = f"#{end_date}\xff"
        
        row_set.add_row_range(
            start_key=start_key.encode('utf-8'),
            end_key=end_key.encode('utf-8')
        )
        
        customer_filter = row_filters.ValueRegexFilter(
            customer_id.encode('utf-8')
        )
        
        results = []
        async for row in self.table.read_rows(row_set=row_set, filter_=customer_filter):
            results.append(self._parse_row(row))
        return results

    async def get_type_date_range_transactions(
        self,
        customer_id: str,
        payment_type: str,
        start_date: str,
        end_date: str
    ) -> List[Dict]:
        """Range scan on dates with customer and type filters"""
        row_set = RowSet()
        start_key = f"#{start_date}"
        end_key = f"#{end_date}\xff"
        
        row_set.add_row_range(
            start_key=start_key.encode('utf-8'),
            end_key=end_key.encode('utf-8')
        )
        
        filters = [
            row_filters.ValueRegexFilter(customer_id.encode('utf-8')),
            row_filters.ValueRegexFilter(payment_type.encode('utf-8'))
        ]
        filter_chain = row_filters.RowFilterChain(filters=filters)
        
        results = []
        async for row in self.table.read_rows(row_set=row_set, filter_=filter_chain):
            results.append(self._parse_row(row))
        return results

    async def get_all_transactions(
        self,
        customer_id: str
    ) -> List[Dict]:
        """Full scan with customer filter"""
        customer_filter = row_filters.ValueRegexFilter(
            customer_id.encode('utf-8')
        )
        
        results = []
        async for row in self.table.read_rows(filter_=customer_filter):
            results.append(self._parse_row(row))
        return results

    def _parse_row(self, row) -> Dict:
        """Parse row with ID-date table structure"""
        base_data = super()._parse_row(row)
        key_parts = row.row_key.decode('utf-8').split('#')
        if len(key_parts) >= 2:
            base_data.update({
                'transaction_id': key_parts[0],
                'transaction_date': key_parts[1]
            })
            # Add data from columns
            if 'cf1' in row.cells:
                cell_values = row.cells['cf1']
                if 'customer_id' in cell_values:
                    base_data['customer_id'] = cell_values['customer_id'][0].value.decode('utf-8')
                if 'payment_type' in cell_values:
                    base_data['transaction_type'] = cell_values['payment_type'][0].value.decode('utf-8')
        return base_data