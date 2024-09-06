from pyspark.sql import SparkSession
import requests
import json
import threading
import csv
import os
from pyspark.sql.functions import col, expr

# Initialize Spark Session with GCS configuration
spark = SparkSession.builder \
    .appName("API Processing with Multi-threading and Direct Write to GCS") \
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

# Thread-safe function to write to CSV
write_lock = threading.Lock()


def write_to_csv(row, file_handle):
    with write_lock:
        writer = csv.writer(file_handle)
        writer.writerow(row)


# Function to call the API and write the response directly to the file
def call_api_and_write(query, file_handle):
    api_url = "https://your-api-endpoint.com/parse-sql"
    headers = {'Content-Type': 'application/json'}
    payload = {"sql": query.query}

    try:
        response = requests.post(api_url, json=payload, headers=headers)
        if response.status_code == 200:
            data = response.json()
            parsing_result = data.get("data", [])

            # Convert API response to the nested format
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

            # Write the result directly to the file
            write_to_csv((query.job_id, query.user_email, query.query, json.dumps(nested_response)), file_handle)
        else:
            write_to_csv((query.job_id, query.user_email, query.query, "API call failed"), file_handle)
    except Exception as e:
        write_to_csv((query.job_id, query.user_email, query.query, f"Error: {str(e)}"), file_handle)


# Main function to process queries with multi-threading
def process_query_with_threading(row):
    with open('/tmp/local_results.csv', mode='a', newline='', encoding='utf-8') as file_handle:
        threads = []
        for _ in range(5):  # Adjust number of threads based on system capability
            thread = threading.Thread(target=call_api_and_write, args=(row, file_handle))
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()


# Apply the function to each row of the filtered DataFrame using RDD
df_filtered.rdd.foreach(process_query_with_threading)

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
