import pytest
from unittest.mock import Mock, patch
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
import pandas as pd
from datetime import datetime

from config.settings import Config
from core.client import create_bigtable_client
from queries.customer import CustomerDailyTransactionsQuery, CustomerDateRangeQuery
from queries.generic import TransactionBatchLookupQuery, PaymentTypeAnalysisQuery

@pytest.fixture
def mock_config():
    """Create a mock configuration for testing"""
    return Config.get_default_config()

@pytest.fixture
def mock_bigtable_client():
    """Create a mock BigTable client"""
    mock_client = Mock()
    mock_instance = Mock()
    mock_table = Mock()
    
    mock_client.instance.return_value = mock_instance
    mock_instance.table.return_value = mock_table
    
    return mock_client

class TestCustomerQueries:
    """Test suite for customer-centric queries"""
    
    def test_daily_transactions_query(self, mock_bigtable_client):
        """Test customer daily transactions query"""
        query = CustomerDailyTransactionsQuery(
            table_name="payments_by_customer",
            client=mock_bigtable_client
        )
        
        # Test row key construction
        row_key = query.build_row_key(
            customer_id="CUST001",
            transaction_date="2024-03-15",
            transaction_type="ACH"
        )
        assert row_key == "CUST001#2024-03-15#ACH"
        
        # Test query execution
        mock_rows = [Mock() for _ in range(5)]
        mock_bigtable_client.instance().table().read_rows.return_value = mock_rows
        
        result = query.execute(
            customer_id="CUST001",
            transaction_date="2024-03-15",
            transaction_type="ACH"
        )
        
        assert result.status == "success"
        assert result.row_count == 5
        assert result.execution_time > 0
        
    def test_date_range_query(self, mock_bigtable_client):
        """Test customer date range query"""
        query = CustomerDateRangeQuery(
            table_name="payments_by_customer",
            client=mock_bigtable_client
        )
        
        # Test query execution
        mock_rows = [Mock() for _ in range(10)]
        mock_bigtable_client.instance().table().read_rows.return_value = mock_rows
        
        result = query.execute(
            customer_id="CUST001",
            start_date="2024-03-01",
            end_date="2024-03-31"
        )
        
        assert result.status == "success"
        assert result.row_count == 10
        assert result.execution_time > 0

class TestGenericQueries:
    """Test suite for generic queries"""
    
    def test_transaction_batch_lookup(self, mock_bigtable_client):
        """Test transaction batch lookup query"""
        query = TransactionBatchLookupQuery(
            table_name="payments_by_id",
            client=mock_bigtable_client
        )
        
        # Test query execution
        mock_rows = [Mock() for _ in range(3)]
        mock_bigtable_client.instance().table().read_rows.return_value = mock_rows
        
        result = query.execute(
            transaction_ids=["TX001", "TX002", "TX003"]
        )
        
        assert result.status == "success"
        assert result.row_count == 3
        assert result.execution_time > 0
    
    def test_payment_type_analysis(self, mock_bigtable_client):
        """Test payment type analysis query"""
        query = PaymentTypeAnalysisQuery(
            table_name="payments_by_date",
            client=mock_bigtable_client
        )
        
        # Test query execution
        mock_rows = [Mock() for _ in range(100)]
        mock_bigtable_client.instance().table().read_rows.return_value = mock_rows
        
        result = query.execute(
            start_date="2024-03-01",
            end_date="2024-03-31",
            payment_type="WIRE"
        )
        
        assert result.status == "success"
        assert result.row_count == 100
        assert result.execution_time > 0

class TestBeamPipeline:
    """Test suite for Beam pipeline components"""
    
    def test_query_generator(self, mock_config):
        """Test query generator DoFn"""
        with TestPipeline() as pipeline:
            # Create test input
            input_data = [None]  # Dummy input
            
            # Apply QueryGeneratorDoFn
            output = (
                pipeline
                | beam.Create(input_data)
                | beam.ParDo(QueryGeneratorDoFn(mock_config))
            )
            
            # Verify output
            assert_that(output, lambda x: len(list(x)) == mock_config.query_config.num_queries)
    
    def test_metrics_calculator(self):
        """Test metrics calculator DoFn"""
        with TestPipeline() as pipeline:
            # Create test input
            test_results = [
                {
                    'query_type': 'TestQuery',
                    'execution_time_ms': 100,
                    'row_count': 10,
                    'status': 'success'
                }
                for _ in range(5)
            ]
            
            # Apply MetricsCalculatorDoFn
            output = (
                pipeline
                | beam.Create([('TestQuery', test_results)])
                | beam.ParDo(MetricsCalculatorDoFn())
            )
            
            # Verify output metrics
            def check_metrics(metrics):
                metrics = list(metrics)
                assert len(metrics) == 1
                assert metrics[0].query_type == 'TestQuery'
                assert metrics[0].total_queries == 5
                assert metrics[0].successful_queries == 5
                return True
            
            assert_that(output, check_metrics)

class TestVisualization:
    """Test suite for visualization components"""
    
    def test_performance_visualizer(self, tmp_path):
        """Test visualization generation"""
        # Create test data
        test_data = {
            'Query Type': ['TestQuery1', 'TestQuery2'],
            'Total Queries': [100, 100],
            'Success Rate': [95.0, 98.0],
            'Avg Time (ms)': [150.0, 120.0],
            'Median Time (ms)': [145.0, 115.0],
            'P95 Time (ms)': [200.0, 180.0],
            'P99 Time (ms)': [250.0, 220.0]
        }
        results_df = pd.DataFrame(test_data)
        
        # Test visualization generation
        visualizer = PerformanceVisualizer(results_df)
        figures = create_visualizations(results_df)
        
        assert 'latency_distribution' in figures
        assert 'success_rates' in figures
        assert 'percentile_comparison' in figures
        
        # Test dashboard generation
        output_dir = str(tmp_path)
        visualizer.create_dashboard(output_dir)
        assert (tmp_path / "performance_dashboard.html").exists()