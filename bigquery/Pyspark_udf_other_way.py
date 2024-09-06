from pyspark.sql import SparkSession
import requests
import json
import csv
import os
from pyspark.sql.functions import col, expr

# Initialize Spark Session with GCS configuration
spark = SparkSession.builder \
    .appName("API Processing with Hybrid Memory Management") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/path/to/your-service-account-key.json") \
    .getOrCreate()

# Define the BigQuery connector options
bq_connector_options = {
    "table": "region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT",
    "project": "your-project-id",
    "credentialsFile": "/path/to/your-service-account-key.json"
}

# Load the BigQuery table into a DataFrame
df = spark.read.format("bigquery").option(**bq_connector_options).load()

# Filter for the last 100 days
df_filtered = df.filter(col("creation_time") >= expr("TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 100 DAY)"))

# Define the output file path on GCS
output_path = "gs://your-bucket-name/output/results.csv"


# Function to call the API and collect results
def call_api_and_collect(query):
    api_url = "https://your-api-endpoint.com/parse-sql"
    headers = {'Content-Type': 'application/json'}
    payload = {"sql": query.query}
    results = []

    try:
        response = requests.post(api_url, json=payload, headers=headers)
        if response.status_code == 200:
            data = response.json()
            parsing_result = data.get("data", [])

            nested_response = [
                {
                    "seqNum": item["seqNum"],
                    "datasetName": item["datasetName"],
                    "tableName": item["tableName"],
                    "columnName": item["columnName"],
                    "literalValue": item["literalValue"]
                }
                for item in parsing_result
            ]
            results.append((query.job_id, query.user_email, query.query, json.dumps(nested_response)))
        else:
            results.append((query.job_id, query.user_email, query.query, "API call failed"))
    except Exception as e:
        results.append((query.job_id, query.user_email, query.query, f"Error: {str(e)}"))

    return results


# Function to write collected results to CSV
def write_results_to_csv(results):
    with open('/tmp/local_results.csv', mode='a', newline='', encoding='utf-8') as file_handle:
        writer = csv.writer(file_handle)
        writer.writerows(results)


# Function to be executed on each partition
def process_partition(partition):
    local_results = []
    for row in partition:
        local_results.extend(call_api_and_collect(row))

    # After processing the partition, write the results to a local file
    write_results_to_csv(local_results)


# Apply the function to each partition of the filtered DataFrame using RDD
df_filtered.rdd.foreachPartition(process_partition)

# Copy the local CSV file to GCS
spark.sparkContext.addFile("/tmp/local_results.csv")  # Ensure the file is accessible to all nodes
os.system(f"gsutil cp /tmp/local_results.csv {output_path}")

# Load the CSV file from GCS into a BigQuery table
bq_table = "your-project.your_dataset.your_table"
spark.read.format("csv").option("header", "true").load(output_path).write \
    .format("bigquery") \
    .option("table", bq_table) \
    .save()

# Stop the Spark session
spark.stop()
