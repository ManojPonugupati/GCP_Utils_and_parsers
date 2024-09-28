#pip install google-cloud-pubsub google-cloud-bigquery
import re
import json
from google.cloud import bigquery
from concurrent.futures import ThreadPoolExecutor, as_completed
import smtplib
from email.mime.text import MIMEText

# Initialize BigQuery client
bq_client = bigquery.Client(project='your-admin-project-id')

# Regular expression to match SELECT * queries without WHERE clause
pattern = r"SELECT\s+\*\s+FROM\s+(\w+\.\w+\.\w+)(\s+|;|$)(?!.*WHERE)"

# Email configuration
SMTP_SERVER = "smtp.yourmailserver.com"
SMTP_PORT = 587
SMTP_USERNAME = "your-username"
SMTP_PASSWORD = "your-password"
EMAIL_FROM = "admin@yourdomain.com"


def send_email(user_email, query_text):
    """Send an email to the user notifying them of the query violation."""
    subject = "Query Violation: SELECT * Detected"
    body = f"Dear user,\n\nYour query has been terminated because it used SELECT * without a WHERE clause:\n\n{query_text}\n\nPlease modify your query and resubmit."

    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = EMAIL_FROM
    msg['To'] = user_email

    try:
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(SMTP_USERNAME, SMTP_PASSWORD)
        server.sendmail(EMAIL_FROM, user_email, msg.as_string())
        server.quit()
        print(f"Email sent to {user_email}")
    except Exception as e:
        print(f"Failed to send email to {user_email}: {e}")


def kill_query(job_id, project_id):
    """Kill the offending query by canceling the BigQuery job."""
    job = bq_client.cancel_job(job_id, project=project_id)
    job.result()  # Wait for the cancellation to complete
    print(f"Query {job_id} in project {project_id} has been killed.")


def check_and_kill(query, project_id, job_id, user_email):
    """Check if a query violates the rule and kill it if true."""
    match = re.search(pattern, query, re.IGNORECASE)
    if match:
        # Kill the query and send an email to the user
        kill_query(job_id, project_id)
        send_email(user_email, query)
    else:
        print(f"No violation found for query in project {project_id} job {job_id}")


def scan_project_jobs(project_id):
    """Scan the JOBS_BY_PROJECT table for a specific project."""
    query = f"""
    SELECT job_id, query, user_email, creation_time, state
    FROM `{project_id}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
    WHERE state = 'RUNNING' OR state = 'PENDING'
    """

    try:
        # Execute the query on the project's JOBS_BY_PROJECT table
        job_results = bq_client.query(query).result()

        for row in job_results:
            job_id = row.job_id
            query_text = row.query
            user_email = row.user_email

            # Check if query violates the rule and kill if needed
            check_and_kill(query_text, project_id, job_id, user_email)

    except Exception as e:
        print(f"Error querying project {project_id}: {str(e)}")


def scan_all_projects_parallel(project_ids, max_workers=10):
    """Scan all projects in parallel for violating queries."""
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(scan_project_jobs, project_id) for project_id in project_ids]
        for future in as_completed(futures):
            future.result()  # Ensure all tasks complete


# List of project IDs (replace with your actual project IDs)
project_ids = ['project1', 'project2', 'project3', 'project4', '...']  # Add all 600 projects here

# Scan all projects in parallel with a pool of 10 workers
scan_all_projects_parallel(project_ids, max_workers=10)
