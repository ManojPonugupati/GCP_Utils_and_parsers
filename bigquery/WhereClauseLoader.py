import re
import csv
from google.cloud import bigquery, storage

# Initialize BigQuery and Cloud Storage clients
bq_client = bigquery.Client()
storage_client = storage.Client()

# Define the function to parse the WHERE clause and extract key-value pairs
def parse_where_clause(query):
    where_clause = re.search(r'WHERE (.*)', query, re.IGNORECASE)
    if where_clause:
        conditions = where_clause.group(1).split(' AND ')
        key_value_pairs = []
        for condition in conditions:
            key, value = condition.split('=')
            key = key.strip()
            value = value.strip().strip("'")
            key_value_pairs.append({"key": key, "value": value})
        return key_value_pairs
    return []

# Define the function to write results to a file
def write_results_to_file(results, filename):
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["job_id", "query", "key_value_pairs"])
        for result in results:
            writer.writerow([result['job_id'], result['query'], result['key_value_pairs']])

# Fetch data from the original table
query = "SELECT job_id, query FROM `project_id.dataset_id.original_table`"
query_job = bq_client.query(query)

results = []
for row in query_job:
    job_id = row['job_id']
    query = row['query']
    key_value_pairs = parse_where_clause(query)
    results.append({
        "job_id": job_id,
        "query": query,
        "key_value_pairs": key_value_pairs
    })

# Write the results to a file
filename = 'parsed_queries.csv'
write_results_to_file(results, filename)

# Upload the file to Google Cloud Storage
bucket_name = 'your-bucket-name'
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(filename)
blob.upload_from_filename(filename)

# Load the file into BigQuery
dataset_id = 'dataset_id'
table_id = 'new_table'
table_ref = bq_client.dataset(dataset_id).table(table_id)

job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("job_id", "STRING"),
        bigquery.SchemaField("query", "STRING"),
        bigquery.SchemaField("key_value_pairs", "ARRAY", mode="REPEATED", fields=[
            bigquery.SchemaField("key", "STRING"),
            bigquery.SchemaField("value", "STRING")
        ])
    ],
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,
    field_delimiter=","
)

uri = f"gs://{bucket_name}/{filename}"
load_job = bq_client.load_table_from_uri(
    uri, table_ref, job_config=job_config
)

load_job.result()  # Wait for the job to complete

print(f"Loaded {load_job.output_rows} rows into {dataset_id}:{table_id}.")
