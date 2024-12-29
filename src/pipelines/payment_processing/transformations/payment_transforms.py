"""Payment transformation logic for different payment types."""

from typing import Dict, Any, Optional, Generator
from datetime import datetime, timezone
import apache_beam as beam
import json
import logging

class PaymentTransform(beam.DoFn):
    """Transform raw payment data based on payment type."""
    
    def process(self, element: Dict[str, Any]) -> Generator[Dict[str, Any], None, None]:
        payment_type = element.get('payment_type', '').upper()
        logging.info(f"Processing payment type: {payment_type}")
        
        if not payment_type:
            logging.warning("No payment type found in element")
            return
            
        transform_method = getattr(self, f'transform_{payment_type.lower()}', None)
        if not transform_method:
            logging.warning(f"No transform method found for payment type: {payment_type}")
            return
            
        try:
            result = transform_method(element)
            if result:
                logging.info(f"Successfully transformed {payment_type} payment: {result}")
                yield result
            else:
                logging.warning(f"Transform returned None for {payment_type} payment")
        except Exception as e:
            logging.error(f"Error transforming {payment_type} payment: {e}")
            return

    def transform_ach(self, payment: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Transform ACH payment data."""
        try:
            return {
                'transaction_id': payment['ach_transaction_id'],
                'payment_type': 'ACH',
                'customer_id': payment['customer_id'],
                'amount': float(payment['amount']),
                'currency': 'USD',
                'sender_account': f"{payment['sender_routing_number']}:{payment['sender_account_number']}",
                'receiver_account': f"{payment['receiver_routing_number']}:{payment['receiver_account_number']}",
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'status': 'PROCESSED',
                'metadata': {
                    'sec_code': payment.get('sec_code'),
                    'memo': payment.get('memo'),
                    'company_entry_description': payment.get('company_entry_description'),
                    'customer_type': payment.get('customer_type', 'INDIVIDUAL')
                }
            }
        except KeyError:
            return None

    def transform_wire(self, payment: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Transform Wire payment data."""
        try:
            return {
                'transaction_id': payment['wire_id'],
                'payment_type': 'WIRE',
                'customer_id': payment['customer_id'],
                'amount': float(payment['amount']),
                'currency': payment.get('currency', 'USD'),
                'sender_account': f"{payment['sender_bank_code']}:{payment['sender_account']}",
                'receiver_account': f"{payment['receiver_bank_code']}:{payment['receiver_account']}",
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'status': 'PROCESSED',
                'metadata': {
                    'correspondent_bank': payment.get('correspondent_bank'),
                    'purpose_code': payment.get('purpose_code'),
                    'instruction_code': payment.get('instruction_code'),
                    'customer_type': payment.get('customer_type', 'INDIVIDUAL')
                }
            }
        except KeyError:
            return None

    def transform_rtp(self, payment: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Transform RTP (Real-Time Payment) data."""
        try:
            return {
                'transaction_id': payment['rtp_id'],
                'payment_type': 'RTP',
                'customer_id': payment['customer_id'],
                'amount': float(payment['amount']),
                'currency': 'USD',  # RTP is USD only
                'sender_account': f"{payment['sender_bank_id']}:{payment['sender_account']}",
                'receiver_account': f"{payment['receiver_bank_id']}:{payment['receiver_account']}",
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'status': 'PROCESSED',
                'metadata': {
                    'settlement_speed': payment.get('settlement_speed', 'INSTANT'),
                    'purpose': payment.get('purpose'),
                    'remittance_info': payment.get('remittance_info'),
                    'customer_type': payment.get('customer_type', 'INDIVIDUAL')
                }
            }
        except KeyError:
            return None

class PrepareForStorage(beam.DoFn):
    """Prepare standardized payment data for different storage systems."""
    
    def process(self, element: Dict[str, Any]) -> Generator[Dict[str, Any], None, None]:
        """Emit payment data formatted for different storage systems."""
        if not element:
            logging.warning("Received empty element in PrepareForStorage")
            return

        logging.info(f"Preparing payment for storage: {element}")

        # Common BigTable data structure
        base_data = {
            'column_families': {
                'payment_info': {
                    'type': element['payment_type'],
                    'amount': str(element['amount']),
                    'currency': element['currency'],
                    'status': element['status'],
                    'timestamp': element['timestamp'],
                    'customer_id': element['customer_id'],
                    'transaction_id': element['transaction_id']
                },
                'accounts': {
                    'sender': element['sender_account'],
                    'receiver': element['receiver_account']
                },
                'metadata': {
                    key: str(value) for key, value in element['metadata'].items()
                }
            }
        }

        # Prepare for BigQuery (main output)
        bigquery_data = self.prepare_for_bigquery(element)
        logging.info(f"Prepared BigQuery data: {bigquery_data}")
        yield bigquery_data

        # Prepare for different BigTable tables with various row key strategies
        
        # 1. Customer ID + Timestamp + Payment Type
        customer_time_type_data = base_data.copy()
        customer_time_type_data['row_key'] = (
            f"{element['customer_id']}#"
            f"{element['timestamp']}#"
            f"{element['payment_type']}"
        )
        yield beam.pvalue.TaggedOutput('customer_time_type', customer_time_type_data)

        # 2. Customer ID + Timestamp
        customer_time_data = base_data.copy()
        customer_time_data['row_key'] = (
            f"{element['customer_id']}#"
            f"{element['timestamp']}"
        )
        yield beam.pvalue.TaggedOutput('customer_time', customer_time_data)

        # 3. Timestamp + Customer ID
        time_customer_data = base_data.copy()
        time_customer_data['row_key'] = (
            f"{element['timestamp']}#"
            f"{element['customer_id']}"
        )
        yield beam.pvalue.TaggedOutput('time_customer', time_customer_data)

        # 4. Transaction ID only
        transaction_data = base_data.copy()
        transaction_data['row_key'] = element['transaction_id']
        yield beam.pvalue.TaggedOutput('transaction', transaction_data)

        # 5. Customer ID + Payment Type
        customer_type_data = base_data.copy()
        customer_type_data['row_key'] = (
            f"{element['customer_id']}#"
            f"{element['payment_type']}"
        )
        yield beam.pvalue.TaggedOutput('customer_type', customer_type_data)

    def prepare_for_bigquery(self, payment: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare payment data for BigQuery storage."""
        payment_copy = payment.copy()
        payment_copy['metadata'] = json.dumps(payment_copy['metadata'])
        return payment_copy
