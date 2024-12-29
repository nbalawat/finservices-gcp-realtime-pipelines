import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms import window
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from google.cloud import bigtable
import logging
import json
from datetime import datetime, timezone
from typing import Dict, Any

class ProcessPayment(beam.DoFn):
    """Process and enrich payment data"""
    
    def process(self, element):
        try:
            # Parse message
            payment = json.loads(element.decode('utf-8'))
            
            # Get processing timestamp
            process_time = datetime.now(timezone.utc)
            process_timestamp = process_time.strftime('%Y-%m-%d %H:%M:%S')
            
            # Parse existing metadata
            metadata = json.loads(payment['metadata'])
            
            # Add processing information
            metadata['processing'] = {
                'processed_timestamp': process_timestamp,
                'processor_version': '1.0.0',
                'environment': 'development'
            }
            
            # Add payment analysis
            amount = float(payment['amount'])
            metadata['payment_analysis'] = {
                'amount_category': 'large' if amount >= 1000 else 'medium' if amount >= 100 else 'small',
                'amount_cents': int(amount * 100),
                'is_high_value': amount >= 1000
            }
            
            # Add account analysis
            metadata['account_analysis'] = {
                'accounts_match': payment['sender_account'] == payment['receiver_account'],
                'account_prefix': payment['sender_account'][:3],
                'is_internal': payment['sender_account'][:3] == payment['receiver_account'][:3]
            }
            
            # Update metadata in payment
            payment['metadata'] = json.dumps(metadata)
            
            yield payment
            
        except Exception as e:
            logging.error(f"Error processing payment: {str(e)}")
            logging.error(f"Failed message: {element}")
            return None

class WriteToBigtable(beam.DoFn):
    """Write payment data to BigTable"""
    
    def __init__(self, project_id: str, instance_id: str, table_id: str):
        self.project_id = project_id
        self.instance_id = instance_id
        self.table_id = table_id
        self.client = None
        self.table = None
        
    def setup(self):
        """Initialize BigTable client"""
        self.client = bigtable.Client(project=self.project_id, admin=True)
        instance = self.client.instance(self.instance_id)
        self.table = instance.table(self.table_id)
        
    def process(self, payment):
        try:
            if not self.table:
                self.setup()
                
            # Create row key: customerId#timestamp#transactionType
            timestamp_str = payment['timestamp'].replace(' ', 'T')
            row_key = f"{payment['customer_id']}#{timestamp_str}#{payment['payment_type']}"
            
            # Get the row
            row = self.table.direct_row(row_key)
            
            # Add transaction info
            for key in ['transaction_id', 'payment_type', 'amount', 'currency', 'timestamp', 'status']:
                row.set_cell(
                    'transaction_info',
                    key.encode(),
                    str(payment[key]).encode(),
                    timestamp=datetime.now()
                )
            
            # Add account info
            for key in ['sender_account', 'receiver_account', 'customer_id']:
                row.set_cell(
                    'account_info',
                    key.encode(),
                    str(payment[key]).encode(),
                    timestamp=datetime.now()
                )
            
            # Add metadata
            metadata = json.loads(payment['metadata'])
            for section in ['processing', 'payment_analysis', 'account_analysis']:
                if section in metadata:
                    for key, value in metadata[section].items():
                        row.set_cell(
                            'metadata',
                            f'{section}:{key}'.encode(),
                            str(value).encode(),
                            timestamp=datetime.now()
                        )
            
            # Commit the row
            row.commit()
            logging.info(f"Wrote to BigTable: {row_key}")
            
            # Return payment for further processing
            yield payment
            
        except Exception as e:
            logging.error(f"Error writing to BigTable: {str(e)}")
            logging.error(f"Failed payment: {payment}")

def run_pipeline():
    """Run the streaming pipeline"""
    # Configuration
    project_id = 'agentic-experiments-446019'
    subscription_id = 'payments-sub-dev'
    bq_dataset = 'pipeline_data_test'
    bq_table = 'payments'
    bt_instance = 'pipeline-dev'
    bt_table = 'payments'
    
    # Pipeline options
    options = PipelineOptions([
        '--streaming',
        '--project', project_id,
        '--runner', 'DirectRunner'
    ])
    options.view_as(StandardOptions).streaming = True
    
    # BigQuery schema
    schema = 'transaction_id:STRING,payment_type:STRING,customer_id:STRING,' \
            'amount:FLOAT64,currency:STRING,sender_account:STRING,' \
            'receiver_account:STRING,timestamp:TIMESTAMP,status:STRING,' \
            'error_message:STRING,metadata:STRING'

    subscription_path = f'projects/{project_id}/subscriptions/{subscription_id}'
    bq_table_spec = f"{project_id}.{bq_dataset}.{bq_table}"
    
    logging.info("Starting pipeline...")
    logging.info(f"Reading from: {subscription_path}")
    logging.info(f"Writing to BigQuery: {bq_table_spec}")
    logging.info(f"Writing to BigTable: {bt_instance}/{bt_table}")

    with beam.Pipeline(options=options) as pipeline:
        # Read and process messages
        messages = (pipeline 
                   | 'Read from PubSub' >> ReadFromPubSub(subscription=subscription_path)
                   | 'Process Payments' >> beam.ParDo(ProcessPayment())
                   | 'Filter None' >> beam.Filter(lambda x: x is not None))
        
        # Write to BigQuery
        _ = (messages 
             | 'Write to BigQuery' >> WriteToBigQuery(
                 table=bq_table_spec,
                 schema=schema,
                 create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                 write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                 method='STREAMING_INSERTS'
             ))
        
        # Write to BigTable
        _ = (messages
             | 'Write to BigTable' >> beam.ParDo(WriteToBigtable(
                 project_id=project_id,
                 instance_id=bt_instance,
                 table_id=bt_table
             )))
        
        # adding information about the message
        _ = (messages
            | 'Log Placeholder' >> beam.Map(lambda msg: logging.info(f'Received message: {msg}')))
        
        _ = (messages
                | 'Apply Fixed Window' >> beam.WindowInto(window.FixedWindows(10))  
                | 'Count Messages in Window' >> beam.combiners.Count.Globally().without_defaults()
                | 'Log Windowed Counts' >> beam.Map(
                    lambda count: logging.info(f'Processed {count} messages in the last 10 seconds')))


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    try:
        run_pipeline()
        
        # Keep the pipeline running
        while True:
            import time
            time.sleep(1)
            
    except KeyboardInterrupt:
        logging.info("\nPipeline stopped by user")
    except Exception as e:
        logging.error(f"Pipeline error: {str(e)}")
        raise