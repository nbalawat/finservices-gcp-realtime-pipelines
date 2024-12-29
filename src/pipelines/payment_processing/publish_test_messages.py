"""Script to publish test messages to Pub/Sub."""

from google.cloud import pubsub_v1
import json
import logging
from .sample_messages import generate_sample_messages
from .common.env import load_env, get_env_var

def publish_messages(num_messages=5):
    """Publish sample messages to Pub/Sub topic."""
    # Load environment variables
    load_env()
    
    # Create publisher client
    publisher = pubsub_v1.PublisherClient()
    topic_path = get_env_var('PUBSUB_TOPIC')
    
    logging.info("Publishing {} messages to {}".format(num_messages, topic_path))
    
    # Generate and publish messages
    messages = generate_sample_messages(num_messages)
    for message in messages:
        # Convert dictionary to JSON string
        data = json.dumps(message).encode('utf-8')
        future = publisher.publish(topic_path, data=data)
        message_id = future.result()
        logging.info("Published message ID: {}".format(message_id))
        logging.debug("Message content: {}".format(message))

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    publish_messages()
