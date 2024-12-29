# src/pipelines/payment_processing/common/env.py
import os
from dotenv import load_dotenv
from pathlib import Path
import logging

def load_env():
    """Load environment variables from .env file."""
    # Find the project root directory (where .env is located)
    project_root = Path(__file__).resolve().parents[4]  # Changed from 3 to 4 to go up one more level
    dotenv_path = project_root / '.env'
    
    # Add debug logging
    logging.info(f"Looking for .env file at: {dotenv_path}")
    logging.info(f"File exists: {dotenv_path.exists()}")
    
    # Load the .env file
    success = load_dotenv(dotenv_path)
    logging.info(f"Load_dotenv result: {success}")
    
    # Debug: print all environment variables
    logging.info("Environment variables after loading:")
    for key in ['PROJECT_ID', 'ENVIRONMENT', 'PUBSUB_TOPIC', 'PUBSUB_SUBSCRIPTION', 'SERVICE_ACCOUNT_EMAIL']:
        logging.info(f"{key}: {os.getenv(key)}")

def get_env_var(name: str, default: str = None) -> str:
    """Get an environment variable with optional default."""
    value = os.getenv(name, default)
    if value is None:
        raise ValueError(f"Environment variable {name} is not set")
    return value