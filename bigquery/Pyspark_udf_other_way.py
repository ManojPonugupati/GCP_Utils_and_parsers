import logging
import requests
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, udf
from pyspark.sql.types import StringType

# Initialize Spark Session with GCS configuration
spark = SparkSession.builder \
    .appName("API Processing with Multi-threading and Logging") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/path/to/your-service-account-key.json") \
    .getOrCreate()

# Set up the logger
logging.basicConfig(filename='/tmp/api_processing.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

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
log_output_path = "gs://your-bucket-name/output/api_processing.log"


# Function to call the API and log the result
def call_api(query):
    api_url = "https://your-api-endpoint.com/parse-sql"
    headers = {'Content-Type': 'application/json'}
    payload = {"sql": query}

    try:
        response = requests.post(api_url, json=payload, headers=headers)
        if response.status_code == 200:
            data = response.json()
            parsing_result = data.get("data", [])

            # Log the parsing result
            logger.info(f"Query: {query}, Parsing Result: {json.dumps(parsing_result)}")

            return json.dumps(parsing_result)
        else:
            logger.error(f"Query: {query}, API call failed with status code {response.status_code}")
            return "API call failed"
    except Exception as e:
        logger.error(f"Query: {query}, Error: {str(e)}")
        return f"Error: {str(e)}"


# Multi-threading function to call API in parallel
def call_api_multithreaded(queries, max_workers=10):
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(call_api, query): query for query in queries}
        for future in as_completed(futures):
            query = futures[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as exc:
                logger.error(f"Query: {query} generated an exception: {exc}")
    return results


# UDF to handle multi-threaded API calls
def call_api_udf(queries):
    queries = queries.split('|||')  # Split the queries string back into a list
    results = call_api_multithreaded(queries)
    return '|||'.join(results)  # Return results as a single string to store in a column


# Register the UDF with a maximum worker thread of 10 for each partition
call_api_udf_spark = udf(call_api_udf, StringType())

# Concatenate queries in each partition into a single string
df_with_combined_queries = df_filtered.withColumn("combined_queries", col("query"))

# Add the multi-threaded API response to the DataFrame
df_with_response = df_with_combined_queries.withColumn("api_response", call_api_udf_spark(col("combined_queries")))

# Write the DataFrame to a CSV file on GCS
df_with_response.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(output_path)

# Upload the log file to GCS
os.system(f"gsutil cp /tmp/api_processing.log {log_output_path}")

# Stop the Spark session
spark.stop() 
