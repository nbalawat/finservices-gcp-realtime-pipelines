from google.cloud import pubsub_v1
import json
import random
import uuid
import time
import logging
from datetime import datetime
from typing import Dict, Any

class PaymentGenerator:
    """Generates payment data and publishes to PubSub"""
    
    def __init__(self, project_id: str, topic_id: str):
        self.project_id = project_id
        self.topic_id = topic_id
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path(project_id, topic_id)
        
        # Define distribution probabilities
        self.payment_types = {
            'CREDIT': 0.4,
            'DEBIT': 0.3,
            'WIRE': 0.2,
            'ACH': 0.1
        }
        self.currencies = {
            'USD': 0.5,
            'EUR': 0.25,
            'GBP': 0.15,
            'JPY': 0.1
        }
        self.counter = 0

    def _weighted_choice(self, choices: Dict[str, float]) -> str:
        """Make a weighted random choice"""
        total = sum(choices.values())
        r = random.uniform(0, total)
        upto = 0.0
        for choice, prob in choices.items():
            if upto + prob >= r:
                return choice
            upto += prob
        return list(choices.keys())[0]

    def generate_payment(self) -> Dict[str, Any]:
        """Generate a single payment record"""
        self.counter += 1
        current_time = datetime.utcnow()
        
        return {
            'transaction_id': f'STREAM_{uuid.uuid4()}',
            'payment_type': self._weighted_choice(self.payment_types),
            'customer_id': f'CUST{random.randint(1, 1000):04d}',
            'amount': round(random.uniform(10.0, 1000.0), 2),
            'currency': self._weighted_choice(self.currencies),
            'sender_account': f'ACC{random.randint(1, 1000):04d}',
            'receiver_account': f'ACC{random.randint(1, 1000):04d}',
            'timestamp': current_time.strftime('%Y-%m-%d %H:%M:%S'),
            'status': 'COMPLETED',
            'error_message': '',
            'metadata': json.dumps({
                'generation_id': str(uuid.uuid4()),
                'counter': self.counter,
                'publish_time': time.time()
            })
        }

    def publish_payment(self, callback=None):
        """Publish a single payment to PubSub"""
        try:
            data = self.generate_payment()
            message = json.dumps(data).encode('utf-8')
            future = self.publisher.publish(self.topic_path, message)
            
            if callback:
                future.add_done_callback(callback)
            
            return data
            
        except Exception as e:
            logging.error(f"Error publishing message: {str(e)}")
            raise

def publish_callback(future):
    """Callback for async publish"""
    try:
        message_id = future.result()
        logging.info(f"Published message with ID: {message_id}")
    except Exception as e:
        logging.error(f"Publishing error: {str(e)}")

def run_publisher(project_id: str, topic_id: str, rate: float = 1.0):
    """Run the publisher at specified rate"""
    publisher = PaymentGenerator(project_id, topic_id)
    logging.info(f"Starting publisher for topic: {topic_id}")
    logging.info(f"Publishing rate: {rate} messages/second")
    
    try:
        while True:
            payment = publisher.publish_payment(callback=publish_callback)
            logging.info(f"Generated payment: {payment['transaction_id']}")
            time.sleep(1/rate)  # Sleep to control rate
            
    except KeyboardInterrupt:
        logging.info("\nPublisher stopped by user")
    except Exception as e:
        logging.error(f"Publisher error: {str(e)}")
        raise

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Configuration
    PROJECT_ID = 'agentic-experiments-446019'
    TOPIC_ID = 'payments-dev'
    RATE = 10  # messages per second
    
    run_publisher(PROJECT_ID, TOPIC_ID, RATE)
