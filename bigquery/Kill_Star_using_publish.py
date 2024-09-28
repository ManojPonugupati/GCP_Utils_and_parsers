#pip install google-cloud-pubsub google-cloud-bigquery
import re
import json
from google.cloud import pubsub_v1, bigquery

# Initialize Pub/Sub publisher client
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path('your-admin-project-id', 'query-violation-topic')

# Initialize BigQuery client
bq_client = bigquery.Client(project='your-admin-project-id')

# Regular expression to match SELECT * queries without WHERE clause
pattern = r"SELECT\s+\*\s+FROM\s+(\w+\.\w+\.\w+)(\s+|;|$)(?!.*WHERE)"


def publish_to_pubsub(project_id, job_id, query, user_email):
    """Publish the offending query details to Pub/Sub."""
    message = {
        "project_id": project_id,
        "job_id": job_id,
        "offending_query": query,
        "user_email": user_email
    }
    message_json = json.dumps(message).encode('utf-8')
    future = publisher.publish(topic_path, message_json)
    future.result()
    print(f"Published to Pub/Sub: {message}")


def scan_project_jobs(project_id):
    """Scan the JOBS_BY_PROJECT table for a specific project."""
    query = f"""
    SELECT job_id, query, user_email, creation_time, state
    FROM `{project_id}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
    WHERE state = 'RUNNING' OR state = 'PENDING'
    """

    try:
        job_results = bq_client.query(query).result()
        for row in job_results:
            job_id = row.job_id
            query_text = row.query
            user_email = row.user_email

            match = re.search(pattern, query_text, re.IGNORECASE)
            if match:
                # Publish the violation to Pub/Sub
                publish_to_pubsub(project_id, job_id, query_text, user_email)

    except Exception as e:
        print(f"Error querying project {project_id}: {str(e)}")


# List of project IDs
project_ids = ['project1', 'project2', 'project3', 'project4', '...']  # Add all 600 projects here

for project_id in project_ids:
    scan_project_jobs(project_id)
