"""Streaming payment data generator and BigQuery writer."""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import logging
from datetime import datetime
import json
import time
import random
from typing import Dict, Any, Generator
import uuid

class PaymentDataGenerator:
    """Generates streaming payment data"""
    def __init__(self):
        self.payment_types = ['CREDIT', 'DEBIT', 'WIRE', 'ACH']
        self.currencies = ['USD', 'EUR', 'GBP', 'JPY']
        self.statuses = ['COMPLETED', 'PENDING', 'FAILED']
        self.channels = ['WEB', 'MOBILE', 'API', 'BATCH']
        self.regions = ['NA', 'EU', 'ASIA', 'SA', 'AF']

    def generate_payment(self) -> Dict[str, Any]:
        """Generate a single payment record"""
        now = datetime.now()
        amount = round(random.uniform(10.0, 1000.0), 2)
        status = random.choice(self.statuses)
        
        # Generate different accounts ensuring they're not the same
        sender = f'ACC{random.randint(1, 1000):04d}'
        while True:
            receiver = f'ACC{random.randint(1, 1000):04d}'
            if receiver != sender:
                break
        
        return {
            'transaction_id': f'STREAM_{uuid.uuid4()}',
            'payment_type': random.choice(self.payment_types),
            'customer_id': f'CUST{random.randint(1, 1000):04d}',
            'amount': amount,
            'currency': random.choice(self.currencies),
            'sender_account': sender,
            'receiver_account': receiver,
            'timestamp': now.strftime('%Y-%m-%d %H:%M:%S'),
            'status': status,
            'error_message': 'Transaction failed' if status == 'FAILED' else '',
            'metadata': json.dumps({
                'request_id': str(uuid.uuid4()),
                'channel': random.choice(self.channels),
                'region': random.choice(self.regions),
                'processing_time': random.uniform(0.1, 2.0),
                'retry_count': random.randint(0, 3) if status == 'FAILED' else 0
            })
        }

    def generate_stream(self, interval: float = 1.0) -> Generator[Dict[str, Any], None, None]:
        """Generate continuous stream of payment data"""
        records_generated = 0
        start_time = time.time()
        
        while True:
            yield self.generate_payment()
            records_generated += 1
            
            # Calculate and log statistics every 10 records
            if records_generated % 10 == 0:
                elapsed_time = time.time() - start_time
                rate = records_generated / elapsed_time
                logging.info(f"Generated {records_generated} records at {rate:.2f} records/second")
            
            time.sleep(interval)

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

class StreamingBigQueryWriter:
    def __init__(self, project_id='agentic-experiments-446019', 
                 dataset_id='pipeline_data_test',
                 interval=1.0):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.interval = interval
        
        # Configure pipeline options for streaming
        self.pipeline_options = PipelineOptions([
            '--project=' + project_id,
            '--runner=DirectRunner',
            '--temp_location=/tmp/beam-temp',
            '--streaming'  # Enable streaming mode
        ])
        
        # Set streaming options
        streaming_options = self.pipeline_options.view_as(StandardOptions)
        streaming_options.streaming = True

    def run_pipeline(self):
        """Run the streaming pipeline"""
        table_spec = f"{self.project_id}.{self.dataset_id}.payments"
        logging.info(f"Starting streaming pipeline writing to: {table_spec}")
        logging.info(f"Generating records every {self.interval} seconds")
        
        try:
            with beam.Pipeline(options=self.pipeline_options) as pipeline:
                (pipeline 
                 | 'Read From Generator' >> beam.Create(PaymentDataGenerator().generate_stream(self.interval))
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
    
    print("\n=== Starting Streaming Payment Pipeline ===")
    print("Press Ctrl+C to stop the pipeline")
    print("\nConfiguration:")
    print("- Project: agentic-experiments-446019")
    print("- Dataset: pipeline_data_test")
    print("- Table: payments")
    print("- Interval: 1.0 seconds")
    
    pipeline = StreamingBigQueryWriter(
        dataset_id='pipeline_data_test',
        interval=1.0
    )
    
    try:
        pipeline.run_pipeline()
    except KeyboardInterrupt:
        print("\nPipeline stopped by user")
    except Exception as e:
        print(f"\nPipeline failed: {str(e)}")
