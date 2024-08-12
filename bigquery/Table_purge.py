from google.cloud import bigquery
import json
import csv
import os

# Load credentials from JSON file
with open('credentials.json') as cred_file:
    credentials = json.load(cred_file)

# Initialize variables
previous_project_id = None
bq_client = None
log_file = 'deleted_tables_log.csv'
log_fields = ['project_id', 'dataset_id', 'table_id', 'status']

# Initialize the log file and write the header
with open(log_file, mode='w', newline='') as file:
    writer = csv.DictWriter(file, fieldnames=log_fields)
    writer.writeheader()

# Assuming `query_results` is the list of dictionaries containing the result rows
query_results = [
    # Sample rows as dictionaries
    {"project_id": "project_1", "dataset_id": "dataset_1", "table_id": "table_1"},
    {"project_id": "project_1", "dataset_id": "dataset_2", "table_id": "table_2"},
    {"project_id": "project_2", "dataset_id": "dataset_3", "table_id": "table_3"},
    # Add more rows as needed
]

# Function to delete a table and log the operation
def delete_table(client, project_id, dataset_id, table_id):
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    try:
        client.delete_table(table_ref)  # API request to delete the table
        print(f"Deleted table: {table_ref}")
        return 'SUCCESS'
    except Exception as e:
        print(f"Error deleting table {table_ref}: {e}")
        return 'FAILED'

# Process each row in the query result
for row in query_results:
    project_id = row['project_id']
    dataset_id = row['dataset_id']
    table_id = row['table_id']

    # Check if the project_id has changed
    if project_id != previous_project_id:
        # Create or reuse the BigQuery client for the new project_id
        if bq_client:
            del bq_client  # Delete the old client if it exists
        bq_client = bigquery.Client.from_service_account_json('credentials.json', project=project_id)
        previous_project_id = project_id

    # Delete the table and get the status
    status = delete_table(bq_client, project_id, dataset_id, table_id)

    # Log the result
    with open(log_file, mode='a', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=log_fields)
        writer.writerow({
            'project_id': project_id,
            'dataset_id': dataset_id,
            'table_id': table_id,
            'status': status
        })

# Clean up the final client if necessary
if bq_client:
    del bq_client

# Load the log file into BigQuery
def load_log_to_bigquery(log_file, destination_table):
    bq_client = bigquery.Client.from_service_account_json('credentials.json')
    table_ref = bq_client.dataset(destination_table.split('.')[1]).table(destination_table.split('.')[2])

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Skip the header row
        autodetect=True,  # Autodetect schema
    )

    with open(log_file, "rb") as file:
        job = bq_client.load_table_from_file(file, table_ref, job_config=job_config)

    job.result()  # Wait for the job to complete
    print(f"Loaded log file to {destination_table}")

# Call the function to load the log file into BigQuery
load_log_to_bigquery(log_file, 'your_project.your_dataset.your_table')

# Clean up the log file if needed
os.remove(log_file)
