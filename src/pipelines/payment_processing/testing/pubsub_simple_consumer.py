import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.gcp.pubsub import ReadFromPubSub
import logging
import json
from datetime import datetime, timezone
import pytz
import time

class ProcessPayment(beam.DoFn):
    """Process and transform payment data"""
    
    def __init__(self):
        self.payment_thresholds = {
            'small': 100,
            'medium': 1000,
            'large': float('inf')
        }
        self.processed_count = 0
        self.start_time = time.time()
        
    def _calculate_metrics(self):
        """Calculate processing metrics"""
        elapsed_time = time.time() - self.start_time
        rate = self.processed_count / elapsed_time if elapsed_time > 0 else 0
        return {
            'processed_count': self.processed_count,
            'elapsed_time': round(elapsed_time, 2),
            'processing_rate': round(rate, 2)
        }
        
    def _calculate_payment_metrics(self, amount: float, currency: str) -> dict:
        """Calculate additional payment metrics"""
        if amount < self.payment_thresholds['small']:
            category = 'small'
        elif amount < self.payment_thresholds['medium']:
            category = 'medium'
        else:
            category = 'large'
            
        return {
            'amount_category': category,
            'amount_cents': int(amount * 100),
            'is_high_value': amount >= self.payment_thresholds['medium']
        }
    
    def process(self, element):
        try:
            # Parse JSON message
            payment = json.loads(element.decode('utf-8'))
            
            # Parse existing metadata
            existing_metadata = json.loads(payment['metadata'])
            
            # Get processing timestamp
            process_time = datetime.now(timezone.utc)
            
            # Calculate processing delay
            tx_time = datetime.strptime(payment['timestamp'], '%Y-%m-%d %H:%M:%S')
            tx_time = pytz.UTC.localize(tx_time)
            processing_delay_ms = int((process_time - tx_time).total_seconds() * 1000)
            
            # Calculate payment metrics
            payment_metrics = self._calculate_payment_metrics(
                float(payment['amount']), 
                payment['currency']
            )
            
            # Add enriched metadata
            enriched_metadata = {
                **existing_metadata,
                'processing': {
                    'processed_timestamp': process_time.strftime('%Y-%m-%d %H:%M:%S'),
                    'processing_delay_ms': processing_delay_ms,
                    'processor_version': '1.0.0'
                },
                'payment_metrics': payment_metrics,
                'account_analysis': {
                    'accounts_match': payment['sender_account'] == payment['receiver_account'],
                    'account_prefix': payment['sender_account'][:3],
                    'is_internal': payment['sender_account'][:3] == payment['receiver_account'][:3]
                }
            }
            
            # Create transformed payment
            transformed_payment = {
                'transaction_id': payment['transaction_id'],
                'payment_type': payment['payment_type'],
                'customer_id': payment['customer_id'],
                'amount': float(payment['amount']),
                'currency': payment['currency'],
                'sender_account': payment['sender_account'],
                'receiver_account': payment['receiver_account'],
                'timestamp': payment['timestamp'],
                'status': payment['status'],
                'error_message': payment.get('error_message', ''),
                'metadata': json.dumps(enriched_metadata)
            }
            
            # Update metrics
            self.processed_count += 1
            metrics = self._calculate_metrics()
            
            # Log processing details
            logging.info(
                f"Processed payment {self.processed_count}:\n"
                f"  Transaction ID: {payment['transaction_id']}\n"
                f"  Amount: {payment['currency']} {payment['amount']}\n"
                f"  Category: {payment_metrics['amount_category']}\n"
                f"  Processing Delay: {processing_delay_ms}ms\n"
                f"  Stats: Processed {metrics['processed_count']} messages "
                f"in {metrics['elapsed_time']}s "
                f"(Rate: {metrics['processing_rate']}/s)"
            )
            
            yield transformed_payment
            
        except Exception as e:
            logging.error(f"Error processing payment: {str(e)}")
            logging.error(f"Failed message: {element}")

def run_pipeline():
    """Run the streaming pipeline"""
    project_id = 'agentic-experiments-446019'
    subscription_path = f'projects/{project_id}/subscriptions/payments-sub-dev'
    table_spec = f"{project_id}.pipeline_data_test.payments"

    # Pipeline options
    options = PipelineOptions([
        '--streaming',
        '--project', project_id,
        '--runner', 'DirectRunner'
    ])
    options.view_as(StandardOptions).streaming = True

    schema = 'transaction_id:STRING,payment_type:STRING,customer_id:STRING,' \
            'amount:FLOAT64,currency:STRING,sender_account:STRING,' \
            'receiver_account:STRING,timestamp:TIMESTAMP,status:STRING,' \
            'error_message:STRING,metadata:STRING'

    logging.info(f"Starting pipeline with transformations...")
    logging.info(f"Reading from: {subscription_path}")
    logging.info(f"Writing to: {table_spec}")

    with beam.Pipeline(options=options) as pipeline:
        # Add monitoring
        messages = (pipeline 
                   | 'Read from PubSub' >> ReadFromPubSub(subscription=subscription_path)
                   | 'Process Payments' >> beam.ParDo(ProcessPayment()))
        
        # Write to BigQuery
        _ = (messages | 'Write to BigQuery' >> WriteToBigQuery(
                table=table_spec,
                schema=schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                method='STREAMING_INSERTS'
            ))

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    try:
        run_pipeline()
        
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        logging.info("\nPipeline stopped by user")
    except Exception as e:
        logging.error(f"Pipeline error: {str(e)}")
        raise
