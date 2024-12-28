import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromPubSub
from apache_beam.transforms import window
import logging
import json
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
import os
from ..config import PaymentPipelineOptions

class PaymentJsonParsingDoFn(beam.DoFn):
    def process(self, element):
        try:
            if isinstance(element, bytes):
                data = element
            else:
                data = element.data
            
            parsed = json.loads(data.decode('utf-8'))
            # Add metadata
            parsed['processing_timestamp'] = datetime.now(timezone.utc).isoformat()
            parsed['pipeline_version'] = '1.0'
            yield parsed
        except Exception as e:
            logging.error(f"Error parsing JSON: {e}, Data: {data}")
            # You might want to write to a dead letter queue here
            yield beam.pvalue.TaggedOutput('parsing_errors', {
                'error': str(e),
                'data': data.decode('utf-8') if isinstance(data, bytes) else str(data),
                'timestamp': datetime.now(timezone.utc).isoformat()
            })

class BasePaymentPipeline(ABC):
    """Base class for all payment processing pipelines"""
    
    def __init__(self, pipeline_options: PaymentPipelineOptions):
        self.pipeline_options = pipeline_options
        self.pipeline = beam.Pipeline(options=pipeline_options)

    @abstractmethod
    def transform_payment(self, payment: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Override this method to implement payment-specific transformation logic"""
        pass

    @abstractmethod
    def standardize_payment(self, payment: Dict[str, Any]) -> Dict[str, Any]:
        """Override this method to implement payment-specific standardization logic"""
        pass

    def build_pipeline(self):
        """Build the base pipeline structure"""
        main, errors = (
            self.pipeline
            | 'Read from PubSub' >> ReadFromPubSub(
                subscription=self.pipeline_options.subscription,
                with_attributes=True
            )
            | 'Window' >> beam.WindowInto(
                window.FixedWindows(self.pipeline_options.window_size)
            )
            | 'Parse JSON' >> beam.ParDo(PaymentJsonParsingDoFn()).with_outputs('parsing_errors', main='main')
        )

        # Process main payment flow
        processed_payments = (
            main
            | 'Transform Payments' >> beam.Map(self.transform_payment)
            | 'Filter None' >> beam.Filter(lambda x: x is not None)
            | 'Standardize Payments' >> beam.Map(self.standardize_payment)
        )

        # Handle errors
        _ = (
            errors
            | 'Write Errors to BigQuery' >> beam.io.WriteToBigQuery(
                f"{self.pipeline_options.output_table}_errors",
                schema='timestamp:TIMESTAMP,error:STRING,data:STRING',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

        return processed_payments

    def run(self):
        """Run the pipeline"""
        result = self.pipeline.run()
        result.wait_until_finish()
