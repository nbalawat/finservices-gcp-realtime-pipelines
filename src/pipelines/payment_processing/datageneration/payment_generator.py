import apache_beam as beam
import uuid
import random
import json
from datetime import datetime, timedelta
from typing import Dict, Any, List
import logging

# Configure logging with a more detailed format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PaymentTypeGenerator:
    PAYMENT_TYPES = {
        'ACH': 0.30,
        'WIRE': 0.15,
        'CARD': 0.25,
        'RTP': 0.10,
        'SEPA': 0.10,
        'CHECK': 0.05,
        'P2P': 0.05
    }

    def __init__(self, customer_id: str, transaction_time: datetime):
        self.customer_id = customer_id
        self.transaction_time = transaction_time
        self.transaction_id = str(uuid.uuid4())

    def generate_base_payment(self, payment_type: str) -> Dict[str, Any]:
        return {
            'payment_header': {
                'payment_id': self.transaction_id,
                'payment_type': payment_type,
                'version': '1.0',
                'created_at': self.transaction_time.isoformat(),
                'updated_at': self.transaction_time.isoformat(),
                'status': random.choice(['COMPLETED', 'PROCESSING', 'PENDING'])
            },
            'amount': self._generate_amount(),
            'parties': self._generate_parties()
        }

    def _generate_amount(self) -> Dict[str, Any]:
        return {
            'value': round(random.uniform(10.0, 10000.0), 2),
            'currency': random.choice(['USD', 'EUR', 'GBP'])
        }

    def _generate_parties(self) -> Dict[str, Any]:
        return {
            'originator': {
                'party_id': self.customer_id,
                'party_type': random.choice(['INDIVIDUAL', 'CORPORATE']),
                'account_info': {
                    'account_id': f"ACC{random.randint(100000, 999999)}",
                    'routing_info': {
                        'type': random.choice(['ABA', 'SWIFT', 'IBAN']),
                        'value': f"ROUTE{random.randint(100000, 999999)}"
                    }
                }
            }
        }

    def generate_ach_payment(self) -> Dict[str, Any]:
        payment = self.generate_base_payment('ACH')
        payment['type_specific_data'] = {
            'sec_code': random.choice(['CCD', 'PPD', 'WEB']),
            'addenda': [f"INVOICE{random.randint(1000, 9999)}"]
        }
        return payment

    def generate_wire_payment(self) -> Dict[str, Any]:
        payment = self.generate_base_payment('WIRE')
        payment['type_specific_data'] = {
            'message_type': 'MT103',
            'charges_instruction': random.choice(['OUR', 'BEN', 'SHA'])
        }
        return payment

    # Add other payment type generators...

class PaymentGeneratorFn(beam.DoFn):
    def __init__(self, num_customers: int, start_date: str, end_date: str):
        self.num_customers = num_customers
        self.start_date = datetime.strptime(start_date, '%Y-%m-%d')
        self.end_date = datetime.strptime(end_date, '%Y-%m-%d')

    def _get_random_date(self) -> datetime:
        time_diff = self.end_date - self.start_date
        random_days = random.randint(0, time_diff.days)
        random_seconds = random.randint(0, 24 * 60 * 60)
        return self.start_date + timedelta(days=random_days, seconds=random_seconds)

    def _get_payment_type(self) -> str:
        r = random.random()
        cumulative = 0
        for payment_type, prob in PaymentTypeGenerator.PAYMENT_TYPES.items():
            cumulative += prob
            if r <= cumulative:
                return payment_type
        return list(PaymentTypeGenerator.PAYMENT_TYPES.keys())[0]

    def process(self, element: Any) -> List[Dict[str, Any]]:
        customer_id = f"CUST{random.randint(1, self.num_customers):06d}"
        transaction_time = self._get_random_date()
        payment_type = self._get_payment_type()
        
        # Log payment generation start
        logger.info(f"Generating payment - Type: {payment_type}, Customer: {customer_id}, Time: {transaction_time}")
        
        generator = PaymentTypeGenerator(customer_id, transaction_time)
        
        if payment_type == 'ACH':
            logger.info(f"Creating ACH payment for customer {customer_id}")
            yield generator.generate_ach_payment()
        elif payment_type == 'WIRE':
            logger.info(f"Creating WIRE payment for customer {customer_id}")
            yield generator.generate_wire_payment()
        else:
            logger.info(f"Creating {payment_type} payment for customer {customer_id}")
            yield generator.generate_base_payment(payment_type)

class FormatForBigTableFn(beam.DoFn):
    def __init__(self, row_key_format: str):
        self.row_key_format = row_key_format

    def process(self, element: Dict[str, Any]) -> List[Dict[str, Any]]:
        header = element['payment_header']
        customer_id = element['parties']['originator']['party_id']
        transaction_date = header['created_at'].split('T')[0]
        
        # Log BigTable formatting details
        logger.info(f"Formatting payment for BigTable - Customer: {customer_id}, Date: {transaction_date}, Type: {header['payment_type']}")
        
        components = {
            'customerId': customer_id,
            'transaction_date': transaction_date,
            'transaction_type': header['payment_type'],
            'transaction_id': header['payment_id']
        }
        
        row_key = self.row_key_format.format(**components)
        logger.info(f"Created BigTable row key: {row_key}")
        
        yield {
            'row_key': row_key,
            'data': element
        }

def run(argv=None):
    # Log pipeline startup
    logger.info("=== Starting Payment Generation Pipeline ===")
    
    parser = beam.options.pipeline_options.PipelineOptions()
    parser.add_argument('--num_records', type=int, default=10)
    parser.add_argument('--project_id', required=True)
    parser.add_argument('--bigtable_instance', required=True)
    
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    # Log configuration parameters
    logger.info(f"Configuration:")
    logger.info(f"  - Number of records: {known_args.num_records}")
    logger.info(f"  - Project ID: {known_args.project_id}")
    logger.info(f"  - BigTable Instance: {known_args.bigtable_instance}")
    
    pipeline_options = beam.options.pipeline_options.PipelineOptions(pipeline_args)
    
    with beam.Pipeline(options=pipeline_options) as pipeline:
        logger.info("Building pipeline...")
        
        # Generate payments
        payments = (
            pipeline
            | "Create Records" >> beam.Create(range(known_args.num_records))
            | "Generate Payments" >> beam.ParDo(PaymentGeneratorFn(
                num_customers=1000,
                start_date='2024-01-01',
                end_date='2024-12-31'
            ))
        )
        
        # Format for BigTable
        bigtable_rows = payments | "Format for BigTable" >> beam.ParDo(
            FormatForBigTableFn(row_key_format="{customerId}#{transaction_date}#{transaction_type}#{transaction_id}")
        )
        
        logger.info("Pipeline built successfully")

if __name__ == '__main__':
    logger.info("=== Payment Generator Script Started ===")
    run()
    logger.info("=== Payment Generator Script Completed ===")