"""Utility to debug BigQuery permissions in detail."""

from google.cloud import bigquery, iam
from google.api_core import retry, exceptions
import logging
import json
from datetime import datetime

def debug_permissions():
    """Debug BigQuery permissions in detail"""
    project_id = 'agentic-experiments-446019'
    dataset_id = 'pipeline_data_test'
    service_account = 'beam-local-pipeline@agentic-experiments-446019.iam.gserviceaccount.com'
    
    client = bigquery.Client(project=project_id)
    
    print("\n=== BigQuery Permission Debug ===")
    print(f"Time: {datetime.now().isoformat()}")
    print(f"Project: {project_id}")
    print(f"Dataset: {dataset_id}")
    print(f"Service Account: {service_account}")
    
    # 1. Test basic dataset operations
    print("\n1. Dataset Operations:")
    try:
        dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
        dataset = client.get_dataset(dataset_ref)
        print(f"✅ Can access dataset: {dataset.dataset_id}")
        print(f"  Location: {dataset.location}")
        print(f"  Created: {dataset.created}")
        print(f"  Modified: {dataset.modified}")
        
        # Get access entries
        print("\nAccess Entries:")
        for entry in dataset.access_entries:
            print(f"- Role: {entry.role if hasattr(entry, 'role') else 'N/A'}")
            print(f"  Entity Type: {entry.entity_type if hasattr(entry, 'entity_type') else 'N/A'}")
            print(f"  Entity ID: {entry.entity_id if hasattr(entry, 'entity_id') else 'N/A'}")
            
    except exceptions.NotFound:
        print(f"❌ Dataset not found: {dataset_id}")
        # Try to create dataset
        try:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"
            dataset = client.create_dataset(dataset, exists_ok=True)
            print("✅ Created new dataset")
        except Exception as create_e:
            print(f"❌ Cannot create dataset: {str(create_e)}")
    except Exception as e:
        print(f"❌ Cannot access dataset: {str(e)}")
    
    # 2. Test specific permissions
    print("\n2. Testing Permissions:")
    permissions_to_test = [
        'bigquery.datasets.get',
        'bigquery.datasets.update',
        'bigquery.tables.create',
        'bigquery.tables.get',
        'bigquery.tables.update',
        'bigquery.tables.getData',
        'bigquery.tables.list',
        'bigquery.jobs.create'
    ]
    
    try:
        # Try to get IAM policy
        try:
            dataset_path = f"projects/{project_id}/datasets/{dataset_id}"
            iam_policy = client.get_iam_policy(dataset_ref)
            print("\nIAM Policy:")
            for role in iam_policy.bindings:
                print(f"Role: {role.role}")
                print("Members:")
                for member in role.members:
                    print(f"  - {member}")
        except Exception as e:
            print(f"❌ Cannot get IAM policy: {str(e)}")
        
        # Test table operations
        print("\nTable Operations:")
        try:
            tables = list(client.list_tables(dataset_ref))
            print(f"✅ Can list tables ({len(tables)} found)")
            for table in tables:
                try:
                    full_table = client.get_table(table)
                    print(f"  - {table.table_id}: {full_table.num_rows} rows")
                except Exception as e:
                    print(f"  - {table.table_id}: ❌ Cannot access ({str(e)})")
        except Exception as e:
            print(f"❌ Cannot list tables: {str(e)}")
            
    except Exception as e:
        print(f"❌ Error testing permissions: {str(e)}")
    
    # 3. Test actual operations
    print("\n3. Testing Operations:")
    operations = [
        ("List Datasets", lambda: list(client.list_datasets())),
        ("Get Dataset", lambda: client.get_dataset(dataset_ref)),
        ("List Tables", lambda: list(client.list_tables(dataset_ref))),
        ("Create Test Table", lambda: create_test_table(client, dataset_ref)),
        ("Run Query", lambda: run_test_query(client, project_id, dataset_id))
    ]
    
    for op_name, op_func in operations:
        try:
            op_func()
            print(f"✅ {op_name}: Success")
        except Exception as e:
            print(f"❌ {op_name}: Failed - {str(e)}")
    
    # 4. Suggest fixes
    print("\n4. Recommended Fixes:")
    print("""
    If you're having permission issues, try these commands:
    
    1. Grant dataset access:
       bq update --source --project_id=agentic-experiments-446019 --dataset pipeline_data_dev \
           --authorized_view "serviceAccount:beam-local-pipeline@agentic-experiments-446019.iam.gserviceaccount.com"
           
    2. Grant BigQuery data owner role:
       gcloud projects add-iam-policy-binding agentic-experiments-446019 \
           --member="serviceAccount:beam-local-pipeline@agentic-experiments-446019.iam.gserviceaccount.com" \
           --role="roles/bigquery.dataOwner"
           
    3. Grant BigQuery job user role (for running queries):
       gcloud projects add-iam-policy-binding agentic-experiments-446019 \
           --member="serviceAccount:beam-local-pipeline@agentic-experiments-446019.iam.gserviceaccount.com" \
           --role="roles/bigquery.jobUser"
    
    4. Verify service account has correct permissions:
       gcloud projects get-iam-policy agentic-experiments-446019 \
           --flatten="bindings[].members" \
           --format='table(bindings.role)' \
           --filter="bindings.members:beam-local-pipeline@agentic-experiments-446019.iam.gserviceaccount.com"
    """)

def create_test_table(client, dataset_ref):
    """Try to create a test table"""
    schema = [
        bigquery.SchemaField("test_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("value", "STRING", mode="NULLABLE"),
    ]
    
    table_ref = dataset_ref.table("_test_permissions")
    table = bigquery.Table(table_ref, schema=schema)
    table = client.create_table(table, exists_ok=True)
    return table

def run_test_query(client, project_id, dataset_id):
    """Try to run a test query"""
    query = f"""
    SELECT 1 as test
    FROM `{project_id}.{dataset_id}.__TABLES_SUMMARY__`
    LIMIT 1
    """
    query_job = client.query(query)
    return list(query_job.result())

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    debug_permissions()
