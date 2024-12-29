from google.cloud import bigquery
import os

def list_datasets():
    # Set credentials
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/nbalawat/development/apache-beam-examples/key.json'
    
    # Create client
    client = bigquery.Client(project='agentic-experiments-446019')
    
    # List datasets
    print("\nDatasets in project:")
    print("-------------------")
    for dataset in client.list_datasets():
        print(f"Dataset ID: {dataset.dataset_id}")
        print(f"Full path: {dataset.full_dataset_id}")
        print("-------------------")

if __name__ == "__main__":
    list_datasets()
