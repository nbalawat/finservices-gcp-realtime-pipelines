import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.gcp.pubsub import ReadFromPubSub
import logging
import json
from datetime import datetime, timezone
import pytz

class ProcessPayment(beam.DoFn):
    """Process and transform payment data"""
    
    def __init__(self):
        self.payment_thresholds = {
            'small': 100,
            'medium': 1000,
            'large': float('inf')
        }
        
    def _calculate_payment_metrics(self, amount: float, currency: str) -> dict:
        """Calculate additional payment metrics"""
        # Categorize payment
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
    
    def _enrich_metadata(self, payment: dict, existing_metadata: dict) -> dict:
        """Add enriched metadata"""
        # Get processing timestamp in UTC
        process_time = datetime.now(timezone.utc)
        
        # Calculate time since transaction
        try:
            tx_time = datetime.strptime(payment['timestamp'], '%Y-%m-%d %H:%M:%S')
            tx_time = pytz.UTC.localize(tx_time)
            processing_delay_ms = int((process_time - tx_time).total_seconds() * 1000)
        except Exception:
            processing_delay_ms = None
            
        # Add processing information
        processing_info = {
            'processed_timestamp': process_time.strftime('%Y-%m-%d %H:%M:%S'),
            'processing_delay_ms': processing_delay_ms,
            'processor_version': '1.0.0',
            'environment': 'development'
        }
        
        # Add payment analysis
        payment_metrics = self._calculate_payment_metrics(
            float(payment['amount']), 
            payment['currency']
        )
        
        # Add account information
        account_info = {
            'accounts_match': payment['sender_account'] == payment['receiver_account'],
            'account_prefix': payment['sender_account'][:3],
            'is_internal': payment['sender_account'][:3] == payment['receiver_account'][:3]
        }
        
        # Combine all metadata
        return {
            **existing_metadata,
            'processing': processing_info,
            'payment_metrics': payment_metrics,
            'account_analysis': account_info,
            'derived_fields': {
                'hour_of_day': tx_time.hour if 'tx_time' in locals() else None,
                'is_business_hours': 9 <= tx_time.hour <= 17 if 'tx_time' in locals() else None,
                'same_currency_pair': True  # Could be enhanced with currency pair logic
            }
        }
    
    def process(self, element):
        try:
            # Parse JSON message
            payment = json.loads(element.decode('utf-8'))
            
            # Parse existing metadata
            existing_metadata = json.loads(payment['metadata'])
            
            # Enrich metadata with transformations
            enriched_metadata = self._enrich_metadata(payment, existing_metadata)
            
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
            
            logging.debug(f"Processed payment: {transformed_payment['transaction_id']}")
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

    # Schema matching existing table
    schema = 'transaction_id:STRING,payment_type:STRING,customer_id:STRING,' \
            'amount:FLOAT64,currency:STRING,sender_account:STRING,' \
            'receiver_account:STRING,timestamp:TIMESTAMP,status:STRING,' \
            'error_message:STRING,metadata:STRING'

    logging.info(f"Starting pipeline with transformations...")
    logging.info(f"Reading from: {subscription_path}")
    logging.info(f"Writing to: {table_spec}")

    with beam.Pipeline(options=options) as pipeline:
        (pipeline 
         | 'Read from PubSub' >> ReadFromPubSub(subscription=subscription_path)
         | 'Process Payments' >> beam.ParDo(ProcessPayment())
         | 'Write to BigQuery' >> WriteToBigQuery(
             table=table_spec,
             schema=schema,
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
             method='STREAMING_INSERTS'
         ))

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    try:
        run_pipeline()
        
        while True:
            import time
            time.sleep(1)
            
    except KeyboardInterrupt:
        logging.info("\nPipeline stopped by user")
    except Exception as e:
        logging.error(f"Pipeline error: {str(e)}")
        raise
