from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from datetime import datetime
import time
from dataclasses import dataclass

@dataclass
class QueryResult:
    """Container for query execution results"""
    query_type: str
    execution_time: float
    row_count: int
    status: str
    error: Optional[str] = None

class BaseQuery(ABC):
    """Abstract base class for all queries"""
    
    def __init__(self, table_name: str, client: Any):
        """
        Initialize query with table name and client
        
        Args:
            table_name: Name of the BigTable table to query
            client: BigTable client instance
        """
        self.table_name = table_name
        self.client = client
        self.query_type = self.__class__.__name__

    @abstractmethod
    def build_row_key(self, **kwargs) -> str:
        """
        Build the row key for the query based on parameters
        
        Args:
            **kwargs: Parameters needed to construct the row key
            
        Returns:
            str: Constructed row key
        """
        pass

    @abstractmethod
    def execute(self, **kwargs) -> QueryResult:
        """
        Execute the query and return results
        
        Args:
            **kwargs: Query parameters
            
        Returns:
            QueryResult: Results of query execution
        """
        pass

    def _measure_execution_time(self, func, **kwargs) -> tuple[Any, float]:
        """
        Measure execution time of a function
        
        Args:
            func: Function to measure
            **kwargs: Parameters to pass to the function
            
        Returns:
            tuple: (function result, execution time in seconds)
        """
        start_time = time.time()
        try:
            result = func(**kwargs)
            return result, time.time() - start_time
        except Exception as e:
            return None, time.time() - start_time

    def _create_result(self, execution_time: float, row_count: int, 
                      status: str = "success", error: Optional[str] = None) -> QueryResult:
        """
        Create a QueryResult instance
        
        Args:
            execution_time: Time taken to execute query
            row_count: Number of rows returned
            status: Query execution status
            error: Error message if any
            
        Returns:
            QueryResult: Container with query execution details
        """
        return QueryResult(
            query_type=self.query_type,
            execution_time=execution_time,
            row_count=row_count,
            status=status,
            error=error
        )