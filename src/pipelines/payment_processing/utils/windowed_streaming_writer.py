"""Windowed streaming payment data generator and BigQuery writer."""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.trigger import (
    AfterWatermark, AfterProcessingTime, AfterCount, 
    AccumulationMode, Repeatedly
)
from apache_beam.transforms.window import FixedWindows, TimestampedValue
from apache_beam.utils.timestamp import Timestamp
import logging
from datetime import datetime, timedelta, timezone
import json
import time
import random
import uuid
from typing import Dict, Any, Generator, List, Tuple

class PaymentGenerator:
    """Enhanced payment data generator with realistic patterns."""
    
    def __init__(self):
        self.payment_types = ['CREDIT', 'DEBIT', 'WIRE', 'ACH']
        self.currencies = {
            'USD': 0.4,  # 40% probability
            'EUR': 0.3,  # 30% probability
            'GBP': 0.2,  # 20% probability
            'JPY': 0.1   # 10% probability
        }
        self.channels = {
            'WEB': 0.5,     # 50% probability
            'MOBILE': 0.3,  # 30% probability
            'API': 0.15,    # 15% probability
            'BATCH': 0.05   # 5% probability
        }
        self.regions = {
            'NA': 0.4,   # 40% probability
            'EU': 0.3,   # 30% probability
            'ASIA': 0.2, # 20% probability
            'SA': 0.07,  # 7% probability
            'AF': 0.03   # 3% probability
        }
        # Time-based failure probabilities
        self.base_failure_rate = 0.05  # 5% base failure rate
        
    def _weighted_choice(self, choices: Dict[str, float]) -> str:
        """Make a weighted random choice from a dictionary of probabilities."""
        total = sum(choices.values())
        r = random.uniform(0, total)
        upto = 0.0
        for choice, prob in choices.items():
            if upto + prob >= r:
                return choice
            upto += prob
        return list(choices.keys())[0]  # Fallback
        
    def _calculate_failure_probability(self, hour: int) -> float:
        """Calculate failure probability based on hour of day."""
        # Higher failure rates during peak hours (9-11 AM and 2-4 PM)
        if 9 <= hour <= 11 or 14 <= hour <= 16:
            return self.base_failure_rate * 1.5
        # Lower failure rates during off-hours (11 PM - 5 AM)
        elif 23 <= hour or hour <= 5:
            return self.base_failure_rate * 0.5
        return self.base_failure_rate
        
    def _generate_amount(self, payment_type: str) -> float:
        """Generate realistic amount based on payment type."""
        if payment_type == 'WIRE':
            return round(random.uniform(1000.0, 50000.0), 2)
        elif payment_type == 'ACH':
            return round(random.uniform(500.0, 10000.0), 2)
        elif payment_type == 'CREDIT':
            return round(random.uniform(10.0, 5000.0), 2)
        else:  # DEBIT
            return round(random.uniform(10.0, 1000.0), 2)

    def generate_payment(self, timestamp: datetime = None) -> Dict[str, Any]:
        """Generate a single payment record with realistic patterns."""
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)
        elif timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
            
        payment_type = random.choice(self.payment_types)
        amount = self._generate_amount(payment_type)
        
        # Calculate failure probability
        failure_prob = self._calculate_failure_probability(timestamp.hour)
        status = 'FAILED' if random.random() < failure_prob else 'COMPLETED'
        
        # Generate different accounts ensuring they're not the same
        sender = f'ACC{random.randint(1, 1000):04d}'
        while True:
            receiver = f'ACC{random.randint(1, 1000):04d}'
            if receiver != sender:
                break
        
        # Add processing time and retries for failed transactions
        processing_time = random.uniform(0.1, 2.0)
        retry_count = random.randint(0, 3) if status == 'FAILED' else 0
        
        return {
            'transaction_id': f'STREAM_{uuid.uuid4()}',
            'payment_type': payment_type,
            'customer_id': f'CUST{random.randint(1, 1000):04d}',
            'amount': amount,
            'currency': self._weighted_choice(self.currencies),
            'sender_account': sender,
            'receiver_account': receiver,
            'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            'status': status,
            'error_message': f'Transaction failed after {retry_count} retries' if status == 'FAILED' else '',
            'metadata': json.dumps({
                'request_id': str(uuid.uuid4()),
                'channel': self._weighted_choice(self.channels),
                'region': self._weighted_choice(self.regions),
                'processing_time': processing_time,
                'retry_count': retry_count,
                'peak_time': 9 <= timestamp.hour <= 17,
                'weekend': timestamp.weekday() >= 5
            })
        }

class GeneratePaymentsFn(beam.DoFn):
    """Generate payment records with timestamps."""
    
    def __init__(self, rate: int = 10):
        """Initialize with records per second rate."""
        self.rate = rate
        self.generator = PaymentGenerator()
    
    def process(self, element, window=beam.DoFn.WindowParam):
        """Generate multiple payment records."""
        now = datetime.now(timezone.utc)
        
        for _ in range(self.rate):
            # Add some jitter to timestamps
            jitter = random.uniform(-0.5, 0.5)  # ±0.5 seconds
            timestamp = now + timedelta(seconds=jitter)
            
            payment = self.generator.generate_payment(timestamp)
            yield TimestampedValue(payment, Timestamp.from_utc_datetime(timestamp))

class FormatPaymentsFn(beam.DoFn):
    """Format payments for BigQuery insertion."""
    
    def process(self, element: Dict[str, Any]):
        """Format a single payment record."""
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
                'timestamp': str(element.get('timestamp', '')),
                'status': str(element.get('status', '')),
                'error_message': str(element.get('error_message', '')),
                'metadata': element.get('metadata', '{}')
            }
            logging.debug(f"Formatted record: {formatted}")
            yield formatted
        except Exception as e:
            logging.error(f"Error formatting record: {str(e)}")
            logging.error(f"Problematic record: {element}")

def run_pipeline(rate: int = 10, window_size: int = 5):
    """Run the streaming pipeline with specified parameters."""
    
    # Pipeline configuration
    options = PipelineOptions([
        '--project=agentic-experiments-446019',
        '--runner=DirectRunner',
        '--temp_location=/tmp/beam-temp'
    ])
    
    # Enable streaming
    standard_options = options.view_as(StandardOptions)
    standard_options.streaming = True
    
    # Table specification
    table_spec = 'agentic-experiments-446019.pipeline_data_test.payments'
    
    try:
        with beam.Pipeline(options=options) as pipeline:
            (pipeline
             | 'Create Timer' >> beam.Create([None])
             | 'Generate Payments' >> beam.ParDo(GeneratePaymentsFn(rate))
             | 'Window' >> beam.WindowInto(
                 FixedWindows(window_size),  # Window size in seconds
                 trigger=Repeatedly(
                     AfterWatermark(early=AfterProcessingTime(1))
                 ),
                 accumulation_mode=AccumulationMode.DISCARDING
             )
             | 'Format Records' >> beam.ParDo(FormatPaymentsFn())
             | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
                 table=table_spec,
                 schema='transaction_id:STRING,payment_type:STRING,customer_id:STRING,'
                        'amount:FLOAT64,currency:STRING,sender_account:STRING,'
                        'receiver_account:STRING,timestamp:TIMESTAMP,status:STRING,'
                        'error_message:STRING,metadata:STRING',
                 create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                 write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                 method='STREAMING_INSERTS'
             ))
        
        logging.info("Pipeline started successfully")
        logging.info(f"Generating {rate} records/second")
        logging.info(f"Window size: {window_size} seconds")
        logging.info("Press Ctrl+C to stop")
        
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        logging.info("\nPipeline stopped by user")
    except Exception as e:
        logging.error(f"Pipeline error: {str(e)}")

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    run_pipeline(rate=10, window_size=5)
