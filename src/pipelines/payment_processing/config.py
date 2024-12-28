import os
from dotenv import load_dotenv
from pathlib import Path
from typing import Dict, Any

class PipelineConfig:
    """Configuration manager for payment processing pipelines"""

    def __init__(self, env_file: str = None):
        """Initialize configuration from environment variables"""
        if env_file:
            load_dotenv(env_file)
        else:
            # Try to load from .env file in the project root
            env_path = Path(__file__).parent.parent.parent.parent / '.env'
            if env_path.exists():
                load_dotenv(env_path)

    @property
    def project_id(self) -> str:
        return os.getenv('GCP_PROJECT_ID')

    @property
    def region(self) -> str:
        return os.getenv('GCP_REGION', 'us-central1')

    @property
    def gcs_bucket(self) -> str:
        return os.getenv('GCS_BUCKET')

    @property
    def bigquery_dataset(self) -> str:
        return os.getenv('BIGQUERY_DATASET')

    @property
    def window_size(self) -> int:
        return int(os.getenv('WINDOW_SIZE', '60'))

    @property
    def runner(self) -> str:
        return os.getenv('RUNNER', 'DataflowRunner')

    def get_subscription(self, payment_type: str) -> str:
        """Get Pub/Sub subscription for a specific payment type"""
        subscription = os.getenv(f'PUBSUB_${payment_type.upper()}_SUBSCRIPTION')
        if not subscription:
            raise ValueError(f"No subscription configured for payment type: {payment_type}")
        return f"projects/{self.project_id}/subscriptions/{subscription}"

    def get_pipeline_options(self, payment_type: str) -> Dict[str, Any]:
        """Get Apache Beam pipeline options for a specific payment type"""
        job_name = f"{os.getenv('JOB_NAME_PREFIX', 'payment-processing')}-{payment_type}"
        
        return {
            'project': self.project_id,
            'region': self.region,
            'temp_location': os.getenv('TEMP_LOCATION') or f'gs://{self.gcs_bucket}/temp',
            'staging_location': os.getenv('STAGING_LOCATION') or f'gs://{self.gcs_bucket}/staging',
            'setup_file': './setup.py',
            'runner': self.runner,
            'job_name': job_name,
            'subscription': self.get_subscription(payment_type),
            'output_table': f'{self.project_id}:{self.bigquery_dataset}.{payment_type}_payments',
            'window_size': self.window_size,
            'streaming': True,
            'save_main_session': True,
        }

    @property
    def error_table(self) -> str:
        """Get the full path to the error table"""
        error_dataset = os.getenv('ERROR_DATASET', self.bigquery_dataset)
        return f'{self.project_id}:{error_dataset}.payment_processing_errors'

    @property
    def monitoring_enabled(self) -> bool:
        """Check if monitoring is enabled"""
        return os.getenv('ENABLE_MONITORING', 'false').lower() == 'true'

    @property
    def monitoring_table(self) -> str:
        """Get the full path to the monitoring table"""
        monitoring_dataset = os.getenv('MONITORING_DATASET', self.bigquery_dataset)
        return f'{self.project_id}:{monitoring_dataset}.pipeline_monitoring'

    def validate(self) -> None:
        """Validate required configuration"""
        required_vars = [
            'GCP_PROJECT_ID',
            'GCS_BUCKET',
            'BIGQUERY_DATASET',
        ]
        
        missing = [var for var in required_vars if not os.getenv(var)]
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
