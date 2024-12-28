from ..common.base_pipeline import BasePaymentPipeline
from ..models.payment_types import PaymentTypeRegistry, StandardizedPayment
from typing import Dict, Any, Optional
from datetime import datetime, timezone
import apache_beam as beam
import json

class ACHPipeline(BasePaymentPipeline):
    def transform_payment(self, payment: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Transform ACH-specific payment data"""
        if not PaymentTypeRegistry.validate_payment('ACH', payment):
            return None

        # Add ACH-specific transformations
        payment['processed_amount'] = float(payment['amount'])
        payment['full_sender_account'] = (
            f"{payment['sender_routing_number']}:{payment['sender_account_number']}"
        )
        payment['full_receiver_account'] = (
            f"{payment['receiver_routing_number']}:{payment['receiver_account_number']}"
        )

        return payment

    def standardize_payment(self, payment: Dict[str, Any]) -> Dict[str, Any]:
        """Standardize ACH payment to common format"""
        return {
            'transaction_id': payment['ach_transaction_id'],
            'payment_type': 'ACH',
            'amount': payment['processed_amount'],
            'currency': 'USD',  # ACH is always in USD
            'sender_account': payment['full_sender_account'],
            'receiver_account': payment['full_receiver_account'],
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'status': 'PROCESSED',
            'metadata': {
                'sec_code': payment.get('sec_code'),
                'memo': payment.get('memo'),
                'company_entry_description': payment.get('company_entry_description')
            }
        }

    def prepare_for_bigtable(self, payment: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare payment data for BigTable storage"""
        # Use transaction_id as row key
        row_key = payment['transaction_id']
        
        # Create column families and columns
        return {
            'row_key': row_key,
            'column_families': {
                'payment_info': {
                    'type': 'ACH',
                    'amount': str(payment['amount']),
                    'currency': payment['currency'],
                    'status': payment['status'],
                    'timestamp': payment['timestamp']
                },
                'accounts': {
                    'sender': payment['sender_account'],
                    'receiver': payment['receiver_account']
                },
                'metadata': {
                    key: str(value) for key, value in payment['metadata'].items()
                }
            }
        }

    def prepare_for_bigquery(self, payment: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare payment data for BigQuery storage"""
        # Convert metadata to string for BigQuery
        payment_copy = payment.copy()
        payment_copy['metadata'] = json.dumps(payment_copy['metadata'])
        return payment_copy

    def build_and_run(self):
        """Build and run the ACH pipeline with parallel writes to BigQuery and BigTable"""
        # Get the base pipeline
        parsed_messages = self.build_pipeline()
        
        # Process the payments
        processed_payments = (
            parsed_messages
            | 'Transform ACH Payments' >> beam.Map(self.transform_payment)
            | 'Filter Invalid Payments' >> beam.Filter(lambda x: x is not None)
            | 'Standardize ACH Payments' >> beam.Map(self.standardize_payment)
        )

        # Branch 1: Write to BigQuery
        bigquery_branch = (
            processed_payments
            | 'Prepare for BigQuery' >> beam.Map(self.prepare_for_bigquery)
            | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
                self.pipeline_options.output_table,
                schema='transaction_id:STRING,payment_type:STRING,amount:FLOAT,currency:STRING,'
                       'sender_account:STRING,receiver_account:STRING,timestamp:TIMESTAMP,'
                       'status:STRING,metadata:STRING',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

        # Branch 2: Write to BigTable
        bigtable_branch = (
            processed_payments
            | 'Prepare for BigTable' >> beam.Map(self.prepare_for_bigtable)
            | 'Write to BigTable' >> beam.io.gcp.bigtable.WriteToBigTable(
                project_id=self.pipeline_options.project,
                instance_id=self.pipeline_options.bigtable_instance,
                table_id=self.pipeline_options.bigtable_table
            )
        )
        
        self.run()
