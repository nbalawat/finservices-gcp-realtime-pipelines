#!/usr/bin/env python3
import os
import shutil
from pathlib import Path

def setup_env():
    """Setup the .env file from template"""
    root_dir = Path(__file__).parent
    template_path = root_dir / '.env.template'
    env_path = root_dir / '.env'

    # Check if .env already exists
    if env_path.exists():
        response = input('.env file already exists. Do you want to overwrite it? (y/N): ')
        if response.lower() != 'y':
            print('Setup cancelled.')
            return

    # Copy template to .env
    shutil.copy(template_path, env_path)
    print(f'Created .env file from template at {env_path}')
    print('\nPlease edit the .env file and set the following required variables:')
    print('  - GCP_PROJECT_ID')
    print('  - GCS_BUCKET')
    print('  - BIGQUERY_DATASET')
    print('\nOptional but recommended variables:')
    print('  - GOOGLE_APPLICATION_CREDENTIALS')
    print('  - PUBSUB_ACH_SUBSCRIPTION')
    print('  - ERROR_DATASET')
    print('  - MONITORING_DATASET')

if __name__ == '__main__':
    setup_env()
