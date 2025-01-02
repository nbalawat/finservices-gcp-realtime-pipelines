import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.metrics import Metrics, MetricsFilter
from typing import Iterator, List, Tuple, Dict, Any
from datetime import datetime, timedelta
import random
import logging

class QueryExecutionDoFn(beam.DoFn):
    """DoFn for executing individual queries"""
    
    def setup(self):
        """Initialize the BigTable client during worker setup"""
        from google.cloud import bigtable
        from google.cloud.bigtable import Client
        
        self.client = Client(project=self.project_id, admin=True)
        self.instance = self.client.instance(self.instance_id)
        self.successful_queries = Metrics.counter(self.__class__, 'successful_queries')
        self.failed_queries = Metrics.counter(self.__class__, 'failed_queries')
        self.query_latency = Metrics.distribution(self.__class__, 'query_latency_ms')
    
    def __init__(self, project_id: str, instance_id: str):
        """
        Initialize the DoFn with configuration
        
        Args:
            project_id: GCP project ID
            instance_id: BigTable instance ID
        """
        super().__init__()
        self.project_id = project_id
        self.instance_id = instance_id
    
    def process(self, query_spec: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
        """
        Process a single query
        
        Args:
            query_spec: Dictionary containing query parameters
            
        Yields:
            Dictionary containing query results and metrics
        """
        try:
            table = self.instance.table(query_spec['table_name'])
            start_time = time.time()
            
            # Execute query based on query_spec type
            result = self._execute_query(table, query_spec)
            
            execution_time_ms = (time.time() - start_time) * 1000
            self.query_latency.update(execution_time_ms)
            self.successful_queries.inc()
            
            yield {
                'query_type': query_spec['query_type'],
                'execution_time_ms': execution_time_ms,
                'row_count': result.row_count,
                'status': 'success'
            }
            
        except Exception as e:
            self.failed_queries.inc()
            logging.error(f"Query execution failed: {str(e)}")
            yield {
                'query_type': query_spec['query_type'],
                'error': str(e),
                'status': 'failed'
            }
    
    def _execute_query(self, table, query_spec: Dict[str, Any]) -> QueryResult:
        """Execute the specific query type"""
        # Implementation will vary based on query type
        pass

class QueryGeneratorDoFn(beam.DoFn):
    """DoFn for generating query specifications"""
    
    def __init__(self, config: Config):
        """
        Initialize with configuration
        
        Args:
            config: Global configuration object
        """
        super().__init__()
        self.config = config
    
    def process(self, dummy_input: Any) -> Iterator[Dict[str, Any]]:
        """
        Generate query specifications
        
        Args:
            dummy_input: Unused input element
            
        Yields:
            Dictionary containing query parameters
        """
        query_types = [
            'CustomerDailyTransactions',
            'CustomerDateRangeTransactions',
            'CustomerTypeTransactions',
            'GenericTransactionLookup',
            'DailyCustomerActivity',
            'DateRangeActivity',
            'PaymentTypeAnalysis'
        ]
        
        for _ in range(self.config.query_config.num_queries):
            query_type = random.choice(query_types)
            yield self._generate_query_spec(query_type)
    
    def _generate_query_spec(self, query_type: str) -> Dict[str, Any]:
        """Generate parameters for a specific query type"""
        # Implementation specific to each query type
        pass

def create_pipeline(config: Config) -> beam.Pipeline:
    """
    Create and return the Beam pipeline for query execution
    
    Args:
        config: Global configuration object
        
    Returns:
        beam.Pipeline: Configured Beam pipeline
    """
    pipeline_options = PipelineOptions([
        '--runner=DirectRunner',  # Can be changed to DataflowRunner for cloud execution
        f'--project={config.table_config.project_id}',
        '--save_main_session=True'
    ])
    
    pipeline = beam.Pipeline(options=pipeline_options)
    
    # Create the main processing pipeline
    (pipeline
     | 'Create Query Trigger' >> beam.Create([None])
     | 'Generate Queries' >> beam.ParDo(QueryGeneratorDoFn(config))
     | 'Execute Queries' >> beam.ParDo(QueryExecutionDoFn(
         config.table_config.project_id,
         config.table_config.instance_id))
     | 'Group By Query Type' >> beam.GroupBy(lambda x: x['query_type'])
     | 'Calculate Metrics' >> beam.ParDo(MetricsCalculatorDoFn()))
    
    return pipeline