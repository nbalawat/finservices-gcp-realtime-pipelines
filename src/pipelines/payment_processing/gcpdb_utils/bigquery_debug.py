"""Utility to debug BigQuery access issues."""

from google.cloud import bigquery
import logging
import json
import google.auth
from google.api_core import exceptions

def debug_bigquery_access():
    """Debug BigQuery access issues"""
    project_id = 'agentic-experiments-446019'
    dataset_id = 'pipeline_data_test'
    
    print("\n=== BigQuery Debug Information ===")
    
    print("\n1. Authentication Information:")
    try:
        credentials, project = google.auth.default()
        print(f"Credential type: {type(credentials).__name__}")
        print(f"Default project: {project}")
        print(f"Service account: {getattr(credentials, 'service_account_email', 'N/A')}")
    except Exception as e:
        print(f"Error getting credentials: {str(e)}")
    
    try:
        client = bigquery.Client(project=project_id)
        print(f"\n2. Client Information:")
        print(f"Project ID: {client.project}")
        print(f"Location: {client.location}")
        
        print("\n3. Available Datasets:")
        try:
            datasets = list(client.list_datasets())
            if datasets:
                for dataset in datasets:
                    print(f"\nDataset: {dataset.dataset_id}")
                    try:
                        # Get detailed information
                        full_dataset = client.get_dataset(dataset.reference)
                        print(f"  Location: {full_dataset.location}")
                        print(f"  Created: {full_dataset.created}")
                        print(f"  Full path: {full_dataset.full_dataset_id}")
                        print(f"  Access Entries:")
                        for entry in full_dataset.access_entries:
                            print(f"    - {entry.role}: {entry.entity_type}")
                        
                        # List tables in dataset
                        tables = list(client.list_tables(dataset))
                        if tables:
                            print("  Tables:")
                            for table in tables:
                                print(f"    - {table.table_id}")
                                try:
                                    full_table = client.get_table(table)
                                    print(f"      Schema: {[field.name for field in full_table.schema]}")
                                    print(f"      Num rows: {full_table.num_rows}")
                                except Exception as e:
                                    print(f"      Error getting table details: {str(e)}")
                        else:
                            print("  No tables found")
                    except exceptions.NotFound:
                        print(f"  Warning: Dataset exists in list but cannot be accessed directly")
                    except Exception as e:
                        print(f"  Error getting dataset details: {str(e)}")
            else:
                print("No datasets found")
        except Exception as e:
            print(f"Error listing datasets: {str(e)}")
        
        print("\n4. Specific Dataset Check:")
        try:
            dataset_ref = f"{project_id}.{dataset_id}"
            dataset = client.get_dataset(dataset_ref)
            print(f"Found dataset: {dataset.dataset_id}")
            print(f"Dataset reference: {dataset_ref}")
            print(f"Dataset path: {dataset.path}")
            print(f"Dataset full ID: {dataset.full_dataset_id}")
            
            # Try a test query
            query = f"""
            SELECT 1 as test
            FROM `{project_id}.{dataset_id}.__TABLES_SUMMARY__`
            LIMIT 1
            """
            print("\n5. Test Query:")
            print(query)
            query_job = client.query(query)
            results = list(query_job.result())
            print("Query successful!")
            
        except exceptions.NotFound:
            print(f"Dataset {dataset_ref} not found")
        except Exception as e:
            print(f"Error checking specific dataset: {str(e)}")
        
        print("\n6. API Path Check:")
        # Check both dot and colon notation
        dot_notation = f"{project_id}.{dataset_id}"
        colon_notation = f"{project_id}:{dataset_id}"
        print(f"Dot notation: {dot_notation}")
        print(f"Colon notation: {colon_notation}")
        
    except Exception as e:
        print(f"\nError initializing BigQuery client: {str(e)}")

if __name__ == "__main__":
    debug_bigquery_access()
