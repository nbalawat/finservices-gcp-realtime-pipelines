import os
from google.cloud import bigquery
from google.auth import default
import logging

def verify_credentials():
    """Verify GCP credentials and print relevant information"""
    logging.info("Checking GCP credentials...")
    
    # Set project ID explicitly
    os.environ['GOOGLE_CLOUD_PROJECT'] = 'agentic-experiments-446019'
    
    # Set service account key path explicitly
    key_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), 
                           'beam-bigquery-test.json')
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = key_path
    
    # Check environment variables
    cred_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT')
    logging.info(f"GOOGLE_APPLICATION_CREDENTIALS env var: {cred_path}")
    logging.info(f"GOOGLE_CLOUD_PROJECT env var: {project_id}")
    
    if not os.path.exists(cred_path):
        logging.error(f"Credentials file not found at: {cred_path}")
        return False
        
    # Check default credentials
    try:
        credentials, project = default()
        logging.info(f"Default project: {project}")
        logging.info(f"Credentials type: {type(credentials).__name__}")
        
        # Test BigQuery access
        client = bigquery.Client(project=project_id)
        # List datasets to verify access
        try:
            datasets = list(client.list_datasets())
            logging.info(f"Successfully listed {len(datasets)} datasets")
            for dataset in datasets:
                logging.info(f"Found dataset: {dataset.dataset_id}")
        except Exception as e:
            logging.error(f"Failed to list datasets: {str(e)}")
            return False
            
        return True
        
    except Exception as e:
        logging.error(f"Failed to get default credentials: {str(e)}")
        logging.info("""
        To fix credential issues:
        1. Run 'gcloud auth application-default login'
        2. Or set GOOGLE_APPLICATION_CREDENTIALS environment variable
        3. Or run 'gcloud auth login' if not yet authenticated
        """)
        return False

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    verify_credentials()
