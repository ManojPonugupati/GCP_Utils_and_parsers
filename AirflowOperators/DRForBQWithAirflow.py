from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from google.cloud import monitoring_v3
from google.api_core.exceptions import GoogleAPICallError, NotFound


def get_safe_location():
    """
    Function to check the status of BigQuery regions using Google Cloud Monitoring.
    Returns the primary region ('us-central1') if healthy, otherwise returns the failover region ('us-east1').
    """
    primary_region = "us-central1"
    failover_region = "us-east1"

    client = monitoring_v3.MetricServiceClient()

    # Define the filter for BigQuery region health metrics
    # Customize this filter based on available metrics or your organization's setup
    region_filter = f'metric.type = "bigquery.googleapis.com/job/completion_rate" AND resource.labels.location = "{primary_region}"'

    try:
        # Query the last 5 minutes of metrics for the primary region
        interval = monitoring_v3.TimeInterval({
            "end_time": datetime.utcnow(),
            "start_time": datetime.utcnow() - timedelta(minutes=5),
        })
        results = client.list_time_series(
            request={
                "name": f"projects/{YOUR_PROJECT_ID}",
                "filter": region_filter,
                "interval": interval,
                "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
            }
        )

        # If results indicate no issues, return the primary region
        if any(series.points[0].value.double_value > 0.9 for series in results):
            return primary_region
    except (GoogleAPICallError, NotFound) as e:
        # Log the error and default to the failover region
        print(f"Error checking region health: {e}")

    # If metrics indicate issues or an error occurs, return the failover region
    return failover_region


default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
        "bigquery_location_disaster_recovery",
        default_args=default_args,
        schedule_interval="@daily",
        start_date=datetime(2024, 1, 1),
) as dag:
    location = get_safe_location()

    bigquery_etl_job = BigQueryInsertJobOperator(
        task_id="run_etl_job",
        configuration={
            "query": {
                "query": "SELECT * FROM `my_project.my_dataset.my_table` WHERE ...",
                "useLegacySql": False,
            }
        },
        location=location,  # Set location dynamically based on disaster recovery needs
    )
