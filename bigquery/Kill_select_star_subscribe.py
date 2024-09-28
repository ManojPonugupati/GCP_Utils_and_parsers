#pip install google-cloud-pubsub google-cloud-bigquery
import json
from google.cloud import pubsub_v1, bigquery
import smtplib
from email.mime.text import MIMEText

# Initialize BigQuery client
bq_client = bigquery.Client(project='your-admin-project-id')

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


def process_message(message):
    """Process each Pub/Sub message and kill the offending query."""
    data = json.loads(message.data.decode('utf-8'))
    project_id = data['project_id']
    job_id = data['job_id']
    offending_query = data['offending_query']
    user_email = data['user_email']

    # Kill the query and send email to the user
    kill_query(job_id, project_id)
    send_email(user_email, offending_query)

    message.ack()  # Acknowledge message processing


def listen_to_pubsub():
    """Listen for Pub/Sub messages and handle them."""
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path('your-admin-project-id', 'query-violation-subscription')

    future = subscriber.subscribe(subscription_path, callback
