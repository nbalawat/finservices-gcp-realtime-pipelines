from google.cloud import bigquery
import os

def delete_dataset():
    # Set credentials
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/nbalawat/development/apache-beam-examples/key.json'
    
    # Create client
    client = bigquery.Client(project='agentic-experiments-446019')
    
    # Delete dataset
    dataset_ref = client.dataset('pipeline_data_dev')
    client.delete_dataset(dataset_ref, delete_contents=True, not_found_ok=True)
    print("Dataset deleted successfully")

if __name__ == "__main__":
    delete_dataset()
