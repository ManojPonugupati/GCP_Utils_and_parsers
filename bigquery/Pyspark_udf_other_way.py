from pyspark.sql import SparkSession
import requests
import json
import threading
import csv

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("API Processing with Multi-threading") \
    .getOrCreate()

# Define the BigQuery connector options
bq_connector_options = {
    "table": "region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT",
    "project": "your-project-id",
    "credentialsFile": "/path/to/your-service-account-key.json"  # Specify the path to your service account JSON key
}

# Load the BigQuery table into a DataFrame
df = spark.read.format("bigquery").option(**bq_connector_options).load()

# Filter for the last 100 days
from pyspark.sql.functions import col, current_timestamp, expr

df_filtered = df.filter(col("creation_time") >= expr("TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 100 DAY)"))


# Function to call the API and parse the response with multi-threading
def call_api_with_threading(query):
    results = []
    lock = threading.Lock()

    def call_api_thread(query):
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

                # Acquire lock to safely append to results list
                with lock:
                    results.append((query.job_id, query.user_email, query.query, json.dumps(nested_response)))
            else:
                with lock:
                    results.append((query.job_id, query.user_email, query.query, "API call failed"))
        except Exception as e:
            with lock:
                results.append((query.job_id, query.user_email, query.query, f"Error: {str(e)}"))

    # Create a thread for each API call (you can limit the number of threads)
    threads = []
    for _ in range(5):  # Adjust this number based on your system's capabilities
        thread = threading.Thread(target=call_api_thread, args=(query,))
        threads.append(thread)
        thread.start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    return results


# Convert the filtered DataFrame to an RDD and process each query with multi-threading
results_rdd = df_filtered.rdd.flatMap(lambda row: call_api_with_threading(row))

# Convert the results back to a DataFrame
results_df = results_rdd.toDF(["job_id", "user_email", "query", "parsing_result"])

# Save the DataFrame to a CSV file
output_path = "/path/to/output.csv"
results_df.write.csv(output_path, header=True, mode="overwrite")

# Optionally, load the CSV file into a BigQuery table
bq_table = "your-project.your_dataset.your_table"
results_df.write \
    .format("bigquery") \
    .option("table", bq_table) \
    .save()

# Stop the Spark session
spark.stop()
