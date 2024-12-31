import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.trigger import AfterWatermark, AfterProcessingTime
from apache_beam.transforms.trigger import AccumulationMode
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.gcp.pubsub import ReadFromPubSub
import logging
import json
from typing import Dict, Any

class ParseJsonMessage(beam.DoFn):
    """Parse PubSub message and validate schema"""
    
    def process(self, element):
        """Process a single PubSub message"""
        try:
            # Parse JSON message
            message = json.loads(element.decode('utf-8'))
            
            # Basic validation
            required_fields = [
                'transaction_id', 'payment_type', 'amount', 'currency',
                'timestamp', 'status'
            ]
            
            if all(field in message for field in required_fields):
                logging.debug(f"Processed message: {message['transaction_id']}")
                yield message
            else:
                logging.error(f"Invalid message schema: {message}")
                
        except Exception as e:
            logging.error(f"Error processing message: {str(e)}")

def run_pipeline(project_id: str, subscription_id: str):
    """Run the streaming pipeline"""
    subscription_path = f'projects/{project_id}/subscriptions/{subscription_id}'
    table_spec = f"{project_id}.pipeline_data_test.payments"

    # Pipeline options
    options = PipelineOptions([
        '--streaming',
        '--project', project_id,
        '--runner', 'DirectRunner'
    ])
    options.view_as(StandardOptions).streaming = True

    # BigQuery schema
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

    logging.info(f"Starting pipeline reading from: {subscription_path}")
    logging.info(f"Writing to table: {table_spec}")

    with beam.Pipeline(options=options) as pipeline:
        # Read from PubSub
        messages = (pipeline 
                   | 'Read from PubSub' >> ReadFromPubSub(
                       subscription=subscription_path
                   )
                   | 'Parse Messages' >> beam.ParDo(ParseJsonMessage())
                   | 'Window' >> beam.WindowInto(
                       FixedWindows(30),  # 30-second windows
                       trigger=AfterWatermark(early=AfterProcessingTime(5)),
                       accumulation_mode=AccumulationMode.DISCARDING
                   ))

        # Write to BigQuery
        _ = (messages | 'Write to BigQuery' >> WriteToBigQuery(
            table=table_spec,
            schema=schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            method='STREAMING_INSERTS'
        ))

        # Add monitoring
        _ = (messages 
             | 'Count Messages' >> beam.CombineGlobally(beam.combiners.CountCombineFn()).without_defaults()
             | 'Log Counts' >> beam.Map(lambda count: 
                 logging.info(f'Processed {count} messages in window')))

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    # Configuration
    PROJECT_ID = 'agentic-experiments-446019'
    SUBSCRIPTION_ID = 'payments-sub-dev'
    
    try:
        logging.info("Starting pipeline...")
        run_pipeline(PROJECT_ID, SUBSCRIPTION_ID)
        
        # Keep the pipeline running
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        logging.info("\nPipeline stopped by user")
    except Exception as e:
        logging.error(f"Pipeline error: {str(e)}")
        raise
