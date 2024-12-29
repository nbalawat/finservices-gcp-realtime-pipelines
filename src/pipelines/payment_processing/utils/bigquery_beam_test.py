"""Apache Beam pipeline for testing BigQuery writes."""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import logging
from datetime import datetime
import json
from typing import Dict, Any

class FormatDataFn(beam.DoFn):
    """Format data for BigQuery insertion"""
    def process(self, element: Dict[str, Any]):
        try:
            # Ensure all fields are of correct type
            formatted = {
                'transaction_id': str(element.get('transaction_id', '')),
                'payment_type': str(element.get('payment_type', '')),
                'customer_id': str(element.get('customer_id', '')),
                'amount': float(element.get('amount', 0.0)),
                'currency': str(element.get('currency', '')),
                'sender_account': str(element.get('sender_account', '')),
                'receiver_account': str(element.get('receiver_account', '')),
                'timestamp': str(element.get('timestamp', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))),
                'status': str(element.get('status', '')),
                'error_message': str(element.get('error_message', '')),
                'metadata': json.dumps(element.get('metadata', {})) if isinstance(element.get('metadata'), dict) else str(element.get('metadata', ''))
            }
            logging.debug(f"Formatted record: {formatted}")
            yield formatted
        except Exception as e:
            logging.error(f"Error formatting record: {str(e)}")
            logging.error(f"Problematic record: {element}")

class BigQueryWriter:
    def __init__(self, project_id='agentic-experiments-446019', 
                 dataset_id='pipeline_data_test'):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.pipeline_options = PipelineOptions([
            '--project=' + project_id,
            '--runner=DirectRunner',
            '--temp_location=/tmp/beam-temp'
        ])

    def generate_test_data(self):
        """Generate sample test data"""
        return [{
            'transaction_id': f'TEST{datetime.now().strftime("%Y%m%d%H%M%S")}',
            'payment_type': 'CREDIT',
            'customer_id': 'CUST001',
            'amount': 100.0,
            'currency': 'USD',
            'sender_account': 'ACC001',
            'receiver_account': 'ACC002',
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'status': 'COMPLETED',
            'error_message': '',
            'metadata': json.dumps({})
        }]

    def run(self, data=None):
        """Run the pipeline with data validation"""
        if data is None:
            data = self.generate_test_data()
            
        table_spec = f"{self.project_id}.{self.dataset_id}.payments"
        logging.info(f"Writing to table: {table_spec}")
        
        try:
            with beam.Pipeline(options=self.pipeline_options) as pipeline:
                (pipeline 
                 | 'Create Events' >> beam.Create(data)
                 | 'Format Data' >> beam.ParDo(FormatDataFn())
                 | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
                     table=table_spec,
                     schema='transaction_id:STRING,payment_type:STRING,customer_id:STRING,'
                            'amount:FLOAT64,currency:STRING,sender_account:STRING,'
                            'receiver_account:STRING,timestamp:TIMESTAMP,status:STRING,'
                            'error_message:STRING,metadata:STRING',
                     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                     write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                     method='STREAMING_INSERTS',
                     additional_bq_parameters={
                         'timePartitioning': {
                             'type': 'DAY',
                             'field': 'timestamp'
                         }
                     }
                 ))
                
            logging.info("Pipeline completed successfully")
            return True
            
        except Exception as e:
            logging.error(f"Pipeline failed: {str(e)}")
            return False

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    print("\n=== Testing BigQuery Beam Pipeline ===")
    
    # Test data
    test_data = [{
        'transaction_id': f'TEST{datetime.now().strftime("%Y%m%d%H%M%S")}',
        'payment_type': 'CREDIT',
        'customer_id': 'CUST001',
        'amount': 100.0,
        'currency': 'USD',
        'sender_account': 'ACC001',
        'receiver_account': 'ACC002',
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'status': 'COMPLETED',
        'error_message': '',
        'metadata': json.dumps({})
    }]
    
    print("\nTest Data:")
    print(json.dumps(test_data[0], indent=2))
    
    pipeline = BigQueryWriter(dataset_id='pipeline_data_test')
    if pipeline.run(test_data):
        print("\nPipeline completed successfully! ✅")
        print("Check BigQuery table: pipeline_data_test.payments")
    else:
        print("\nPipeline failed! ❌")
