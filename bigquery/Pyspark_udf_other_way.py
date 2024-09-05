from pyspark.sql import SparkSession
import requests
import json
import csv

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("API Processing") \
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


# Define a function to call the API and parse the response
def call_api(query):
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

            return (query.job_id, query.user_email, query.query, json.dumps(nested_response))
        else:
            return (query.job_id, query.user_email, query.query, "API call failed")
    except Exception as e:
        return (query.job_id, query.user_email, query.query, f"Error: {str(e)}")


# Convert the filtered DataFrame to an RDD and process each query with the API
results_rdd = df_filtered.rdd.map(lambda row: call_api(row))

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
