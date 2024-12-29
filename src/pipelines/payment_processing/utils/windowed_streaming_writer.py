import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.trigger import AfterWatermark, AfterProcessingTime, Repeatedly, AfterCount
from apache_beam.transforms.trigger import AccumulationMode
from apache_beam.io.gcp.bigquery import WriteToBigQuery
import logging
from datetime import datetime
import json
import time
import random
import uuid
from typing import Any, Dict, Generator, Iterator

class GeneratePayments(beam.PTransform):
    """A Custom PTransform for generating payment data."""

    def expand(self, pcoll):
        return (
            pcoll
            | 'Generate Time' >> beam.Create([time.time()])
            | 'Generate Data' >> beam.ParDo(PaymentGeneratorFn())
        )

class PaymentGeneratorFn(beam.DoFn):
    """Generates payment data continuously"""
    
    def __init__(self):
        self.counter = 0
        self.payment_types = ['CREDIT', 'DEBIT', 'WIRE', 'ACH']
        self.currencies = ['USD', 'EUR', 'GBP', 'JPY']

    def process(self, element: float) -> Iterator[Dict[str, Any]]:
        """Generate a single payment record"""
        current_time = datetime.utcnow()
        
        self.counter += 1
        payment = {
            'transaction_id': f'STREAM_{uuid.uuid4()}',
            'payment_type': random.choice(self.payment_types),
            'customer_id': f'CUST{random.randint(1, 1000):04d}',
            'amount': round(random.uniform(10.0, 1000.0), 2),
            'currency': random.choice(self.currencies),
            'sender_account': f'ACC{random.randint(1, 1000):04d}',
            'receiver_account': f'ACC{random.randint(1, 1000):04d}',
            'timestamp': current_time.strftime('%Y-%m-%d %H:%M:%S'),
            'status': 'COMPLETED',
            'error_message': '',
            'metadata': json.dumps({
                'counter': self.counter,
                'batch_time': time.time(),
                'generation_id': str(uuid.uuid4())
            })
        }
        
        logging.info(f"Generated payment {self.counter}")
        yield payment

def run_pipeline():
    """Run the streaming pipeline"""
    project_id = 'agentic-experiments-446019'
    table_spec = f"{project_id}.pipeline_data_test.payments"

    # Set up pipeline options
    options = PipelineOptions([
        '--streaming',
        '--project', project_id,
        '--runner', 'DirectRunner'
    ])
    options.view_as(StandardOptions).streaming = True

    schema = """
        transaction_id:STRING,
        payment_type:STRING,
        customer_id:STRING,
        amount:FLOAT64,
        currency:STRING,
        sender_account:STRING,
        receiver_account:STRING,
        timestamp:TIMESTAMP,
        status:STRING,
        error_message:STRING,
        metadata:STRING
    """

    logging.info("Starting streaming pipeline...")

    def print_element(element):
        logging.info(f"Processing: {element['transaction_id']}")
        return element

    with beam.Pipeline(options=options) as pipeline:
        # Create an initial trigger every second
        trigger = (pipeline
                  | 'Create Triggers' >> beam.Create([None])
                  | 'Add Triggers' >> beam.Map(lambda x: beam.window.TimestampedValue(x, time.time()))
                  | 'Window Triggers' >> beam.WindowInto(
                      FixedWindows(1),  # 1 second windows
                      trigger=Repeatedly(AfterCount(1)),
                      accumulation_mode=AccumulationMode.DISCARDING
                  ))
        
        # Generate payments
        payments = (trigger
                   | 'Generate Payments' >> GeneratePayments()
                   | 'Log Elements' >> beam.Map(print_element)
                   | 'Add Windows' >> beam.WindowInto(
                       FixedWindows(5),  # 5 second windows
                       trigger=AfterProcessingTime(1),
                       accumulation_mode=AccumulationMode.DISCARDING
                   ))

        # Write to BigQuery
        _ = (payments | 'Write to BigQuery' >> WriteToBigQuery(
            table=table_spec,
            schema=schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            method='STREAMING_INSERTS'
        ))

        # Add monitoring
        _ = (payments 
             | 'Count Elements' >> beam.CombineGlobally(beam.combiners.CountCombineFn()).without_defaults()
             | 'Log Counts' >> beam.Map(lambda count: logging.info(f'Processed {count} records')))

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    try:
        logging.info("Pipeline starting...")
        run_pipeline()
        
        # Keep the main thread alive
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        logging.info("Pipeline stopped by user")
    except Exception as e:
        logging.error(f"Pipeline error: {str(e)}")
        raise