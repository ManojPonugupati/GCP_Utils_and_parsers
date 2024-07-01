import json
from google.cloud import bigquery


# Define the parse_query function (assuming it is already defined elsewhere)
def parse_query(query):
    # This is a placeholder for the actual implementation
    return [
        ("name", "John", True),
        ("age", "30", False),
        ("city", "New York", True)
    ]


def parse_and_store_queries(queries, project_id, job_id, output_file):
    data = []

    for query in queries:
        # Parse the query
        result = parse_query(query)

        # Transform result to desired format, considering only records with True boolean value
        parsed_result = {
            'project_id': project_id,
            'job_id': job_id,
            'query': query,
            'parsed_columns': [{'column_name': r[0], 'column_value': r[1]} for r in result if r[2]]
        }

        data.append(parsed_result)

    # Write the results to a JSON file
    with open(output_file, 'w') as f:
        json.dump(data, f)


def load_data_to_bigquery(json_file, dataset_id, table_id):
    # Construct a BigQuery client object.
    client = bigquery.Client()

    # Define the schema for the BigQuery table
    schema = [
        bigquery.SchemaField("project_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("job_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("query", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("parsed_columns", "RECORD", mode="REPEATED", fields=[
            bigquery.SchemaField("column_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("column_value", "STRING", mode="REQUIRED"),
        ]),
    ]

    # Define the table ID
    table_ref = client.dataset(dataset_id).table(table_id)

    # Create the table if it doesn't exist
    try:
        client.get_table(table_ref)
    except Exception:
        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)
        print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")

    # Load the JSON data into the BigQuery table
    with open(json_file, "rb") as source_file:
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )
        load_job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

    load_job.result()  # Waits for the job to complete.
    print(f"Loaded {load_job.output_rows} rows into {dataset_id}:{table_id}.")


# Example usage
queries = [
    "SELECT name FROM dataset.table WHERE age > 30",
    "SELECT city FROM dataset.table WHERE country = 'USA'"
]
project_id = "your_project_id"
job_id = "your_job_id"
output_file = "parsed_queries.json"
dataset_id = "your_dataset_id"
table_id = "your_table_id"

# Parse the queries and store the results in a JSON file
parse_and_store_queries(queries, project_id, job_id, output_file)

# Load the data from the JSON file into the BigQuery table
load_data_to_bigquery(output_file, dataset_id, table_id)
