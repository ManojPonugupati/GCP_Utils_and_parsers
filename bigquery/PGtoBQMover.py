import psycopg2
from google.cloud import bigquery
import pandas as pd

class PostgreSQLReader:
    def __init__(self, host, dbname, user, password):
        self.host = host
        self.dbname = dbname
        self.user = user
        self.password = password
        self.connection = None

    def connect(self):
        self.connection = psycopg2.connect(
            host=self.host,
            dbname=self.dbname,
            user=self.user,
            password=self.password
        )

    def fetch_data(self, query):
        if self.connection is None:
            self.connect()
        df = pd.read_sql_query(query, self.connection)
        return df

    def close(self):
        if self.connection:
            self.connection.close()

class BigQueryWriter:
    def __init__(self, project_id):
        self.client = bigquery.Client(project=project_id)

    def write_data(self, df, dataset_id, table_id):
        table_ref = self.client.dataset(dataset_id).table(table_id)
        job = self.client.load_table_from_dataframe(df, table_ref)
        job.result()  # Wait for the job to complete

def main():
    # PostgreSQL connection details
    pg_details = {
        "host": "pg_host.manoj.com",
        "dbname": "manojdb",
        "user": "user_manoj",
        "password": "my_password"
    }

    # Query to fetch data
    query = "SELECT DISTINCT col1, col2 FROM table_name"

    # BigQuery details
    project_id = 'your_project_id'
    dataset_id = 'your_dataset_id'
    table_id = 'table_name'

    # Read data from PostgreSQL
    pg_reader = PostgreSQLReader(**pg_details)
    df = pg_reader.fetch_data(query)
    pg_reader.close()

    # Write data to BigQuery
    bq_writer = BigQueryWriter(project_id)
    bq_writer.write_data(df, dataset_id, table_id)

if __name__ == "__main__":
    main()
