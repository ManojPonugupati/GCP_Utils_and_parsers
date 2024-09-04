import requests
import threading
import csv
from google.cloud import bigquery
from datetime import datetime, timedelta

# Define your BigQuery client
client = bigquery.Client()


# Get queries from BigQuery for the last 100 days
def get_queries():
    query = """
    SELECT job_id, user_email, query
    FROM `region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
    WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 100 DAY)
    """
    query_job = client.query(query)
    return query_job.result()


# Function to call the API and process the response
def call_api(query, lock, file_writer):
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

            # Acquire a lock to safely write to the CSV file
            with lock:
                file_writer.writerow({
                    "job_id": query.job_id,
                    "user_email": query.user_email,
                    "query": query.query,
                    "parsing_result": nested_response
                })
        else:
            print(f"Failed to call API for query {query.job_id}: {response.status_code}")
    except Exception as e:
        print(f"Error processing query {query.job_id}: {str(e)}")


# Main function to orchestrate the flow
def main():
    queries = get_queries()

    lock = threading.Lock()  # Lock to prevent race conditions while writing to the file

    # Open the CSV file for writing
    with open('output.csv', 'w', newline='') as csvfile:
        fieldnames = ["job_id", "user_email", "query", "parsing_result"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        threads = []

        # Create a thread for each query
        for query in queries:
            thread = threading.Thread(target=call_api, args=(query, lock, writer))
            threads.append(thread)
            thread.start()

            # Optional: Limit the number of active threads at a time
            if len(threads) >= 100:  # Adjust this number based on your system's capabilities
                for thread in threads:
                    thread.join()
                threads = []

        # Wait for any remaining threads to complete
        for thread in threads:
            thread.join()

    # Ingest CSV file into a BigQuery table
    table_id = "your-project.your_dataset.your_table"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
    )

    with open("output.csv", "rb") as source_file:
        load_job = client.load_table_from_file(source_file, table_id, job_config=job_config)

    load_job.result()  # Waits for the job to complete

    print(f"Loaded {load_job.output_rows} rows into {table_id}.")


if __name__ == "__main__":
    main()
