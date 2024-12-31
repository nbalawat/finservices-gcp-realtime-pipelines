from dataclasses import dataclass
from typing import Dict, List
from datetime import datetime

@dataclass
class TableConfig:
    """Configuration for BigTable tables and their key structures"""
    project_id: str
    instance_id: str
    tables: Dict[str, str]  # Table name -> Key pattern mapping
    column_families: List[str]

@dataclass
class QueryConfig:
    """Configuration for query execution parameters"""
    num_queries: int = 2500
    concurrent_queries: int = 500
    test_month: str = "2024-03"
    sample_customers: int = 1000  # Number of unique customers to sample for testing
    sample_transaction_types: List[str] = None

@dataclass
class PerformanceConfig:
    """Configuration for performance measurement"""
    timeout_seconds: int = 30
    retry_attempts: int = 3
    metrics_window_size: int = 100  # For rolling metrics calculation

@dataclass
class Config:
    """Global configuration container"""
    table_config: TableConfig
    query_config: QueryConfig
    performance_config: PerformanceConfig

    @classmethod
    def get_default_config(cls) -> 'Config':
        """Returns default configuration"""
        table_config = TableConfig(
            project_id="your-project-id",
            instance_id="your-instance-id",
            tables={
                'payments_by_customer': 'customerId#transaction_date#transaction_type',
                'payments_by_date': 'transaction_date#transaction_type#customerId',
                'payments_by_transaction': 'transaction_id#customerId#transaction_date',
                'payments_by_id': 'transaction_id',
                'payments_by_id_date': 'transaction_id#transaction_date'
            },
            column_families=['cf1']  # Add your column families here
        )

        query_config = QueryConfig(
            sample_transaction_types=['ACH', 'WIRE', 'SWIFT', 'RTP', 'SEPA']
        )

        performance_config = PerformanceConfig()

        return cls(
            table_config=table_config,
            query_config=query_config,
            performance_config=performance_config
        )