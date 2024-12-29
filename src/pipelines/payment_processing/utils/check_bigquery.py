from google.cloud import bigquery
import logging
import os

def check_bigquery_data():
    try:
        # Set credentials
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/nbalawat/development/apache-beam-examples/key.json'
        
        # Initialize BigQuery client with explicit project
        client = bigquery.Client(project='agentic-experiments-446019')
        
        # Construct the query
        query = """
        SELECT COUNT(*) as count, payment_type
        FROM `agentic-experiments-446019.pipeline_data_dev.payments`
        GROUP BY payment_type
        ORDER BY payment_type
        """
        
        # Run the query
        query_job = client.query(query)
        
        # Get results
        results = query_job.result()
        
        # Print results
        print("\nPayment counts by type:")
        print("----------------------")
        for row in results:
            print(f"{row.payment_type}: {row.count}")
            
        # Get sample records
        sample_query = """
        SELECT *
        FROM `agentic-experiments-446019.pipeline_data_dev.payments`
        LIMIT 2
        """
        sample_results = client.query(sample_query).result()
        
        print("\nSample records:")
        print("--------------")
        for row in sample_results:
            print(f"\nTransaction ID: {row.transaction_id}")
            print(f"Payment Type: {row.payment_type}")
            print(f"Amount: {row.amount} {row.currency}")
            print(f"Status: {row.status}")
            print(f"Timestamp: {row.timestamp}")
            
    except Exception as e:
        print(f"Error querying BigQuery: {str(e)}")
        raise

if __name__ == "__main__":
    check_bigquery_data()
