from abc import ABC, abstractmethod
from typing import List, Dict, Optional, Any, Callable
from dataclasses import dataclass
from google.cloud.bigtable.data import TableAsync, BigtableDataClientAsync
from google.cloud.bigtable.data import row_filters, RowRange as QueryRowRange, ReadRowsQuery
import logging
import time
import functools
import asyncio
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class BigtableConfig:
    """Configuration for Bigtable connection"""
    project_id: str
    instance_id: str
    table_id: str
    app_profile_id: Optional[str] = None
    read_rows_limit: Optional[int] = None
    timeout: Optional[float] = None

@dataclass
class QueryMetrics:
    """Metrics for query execution"""
    start_time: datetime
    end_time: datetime
    execution_time_ms: float
    row_count: int
    query_type: str
    table_id: str
    error: Optional[str] = None

def track_execution_time(func: Callable) -> Callable:
    """Decorator to track execution time of async methods"""
    @functools.wraps(func)
    async def wrapper(self: 'CustomerQueryBase', *args: Any, **kwargs: Any) -> Any:
        start_time = datetime.now()
        start = time.perf_counter()
        error = None
        result = None
        
        try:
            result = await func(self, *args, **kwargs)
            row_count = len(result) if isinstance(result, list) else 0
        except Exception as e:
            error = str(e)
            row_count = 0
            raise
        finally:
            end = time.perf_counter()
            end_time = datetime.now()
            execution_time = (end - start) * 1000  # Convert to milliseconds
            
            # Create metrics
            metrics = QueryMetrics(
                start_time=start_time,
                end_time=end_time,
                execution_time_ms=execution_time,
                row_count=row_count,
                query_type=self.__class__.__name__,
                table_id=self.config.table_id,
                error=error
            )
            
            # Store metrics
            self._last_query_metrics = metrics
            
            # Log metrics
            logger.info(
                f"Query Metrics - Type: {metrics.query_type}, "
                f"Table: {metrics.table_id}, "
                f"Duration: {metrics.execution_time_ms:.2f}ms, "
                f"Rows: {metrics.row_count}"
            )
            if error:
                logger.error(f"Query Error: {error}")
        
        return result
    
    return wrapper

class CustomerQueryBase(ABC):
    """Abstract base class for customer queries"""
    
    def __init__(self, config: BigtableConfig):
        """Initialize with Bigtable configuration
        
        Args:
            config: BigtableConfig instance containing connection details
        """
        self.config = config
        self._table = None
        self._last_query_metrics: Optional[QueryMetrics] = None
    
    @property
    def last_query_metrics(self) -> Optional[QueryMetrics]:
        """Get metrics from the last query execution"""
        return self._last_query_metrics
    
    async def _get_table(self) -> TableAsync:
        """Get or create the table connection
        
        Returns:
            Async Bigtable table instance
        """
        if not self._table:
            logger.info(f"Connecting to BigTable - Project: {self.config.project_id}, "
                       f"Instance: {self.config.instance_id}, "
                       f"Table: {self.config.table_id}")
            client = BigtableDataClientAsync()
            self._table = TableAsync(
                client,
                instance_id=self.config.instance_id,
                table_id=self.config.table_id,
                app_profile_id=self.config.app_profile_id,
                read_rows_limit=self.config.read_rows_limit,
                timeout=self.config.timeout
            )
        return self._table
    
    def _parse_row(self, row) -> Dict:
        """Base row parsing logic
        
        Args:
            row: Bigtable row
            
        Returns:
            Dict containing parsed row data
        """
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
    
    @abstractmethod
    def _create_transaction_filter(
        self,
        customer_id: str,
        transaction_types: List[str]
    ) -> row_filters.RowFilter:
        """Create a row filter for transaction types
        
        Args:
            customer_id: Customer ID to filter by
            transaction_types: List of transaction types to filter
            
        Returns:
            RowFilter for the query
        """
        pass

    @abstractmethod
    @track_execution_time
    async def get_customer_transactions_by_types(
        self,
        customer_id: str,
        transaction_types: List[str],
        start_date: str,
        end_date: str
    ) -> List[Dict]:
        """Get customer transactions for multiple transaction types within a date range
        
        Args:
            customer_id: Customer ID
            transaction_types: List of transaction types (e.g., ['WIRE', 'ACH'])
            start_date: Start date (format: YYYY-MM-DDTHH:mm:ss)
            end_date: End date (format: YYYY-MM-DDTHH:mm:ss)
            
        Returns:
            List of matching transactions
        """
        pass
