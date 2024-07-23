from google.cloud import bigquery

# Initialize clients for both environments
prod_client = bigquery.Client(project='your-production-project-id')
dev_client = bigquery.Client(project='your-development-project-id')


def get_table_details(client, dataset_id, table_id):
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    table = client.get_table(table_ref)

    schema = table.schema
    num_rows = table.num_rows
    table_size = table.num_bytes

    return {
        "schema": schema,
        "num_rows": num_rows,
        "table_size": table_size
    }


def compare_schemas(prod_schema, dev_schema):
    prod_fields = set((field.name, field.field_type) for field in prod_schema)
    dev_fields = set((field.name, field.field_type) for field in dev_schema)

    differences = {
        "in_prod_not_in_dev": prod_fields - dev_fields,
        "in_dev_not_in_prod": dev_fields - prod_fields
    }

    return differences


def analyze_column_sizes(client, dataset_id, table_id):
    query = f"""
    SELECT
      column_name,
      data_type,
      ROUND(SUM(size_bytes)/1024/1024, 2) as size_mb
    FROM
      `{dataset_id}.INFORMATION_SCHEMA.COLUMN_FIELD_DETAILS`
    WHERE
      table_name = '{table_id}'
    GROUP BY
      column_name, data_type
    ORDER BY
      size_mb DESC
    """
    query_job = client.query(query)
    return list(query_job)


def analyze_storage_breakdown(client, dataset_id, table_id):
    query = f"""
    SELECT
      column_name,
      data_type,
      MAX(approx_field_size) AS max_approx_size,
      MIN(approx_field_size) AS min_approx_size,
      AVG(approx_field_size) AS avg_approx_size
    FROM
      `{dataset_id}.INFORMATION_SCHEMA.COLUMN_FIELD_DETAILS`
    WHERE
      table_name = '{table_id}'
    GROUP BY
      column_name, data_type
    ORDER BY
      avg_approx_size DESC
    """
    query_job = client.query(query)
    return list(query_job)


def get_null_and_empty_counts(client, dataset_id, table_id):
    query = f"""
    SELECT
      column_name,
      COUNTIF({column_name} IS NULL) AS null_count,
      COUNTIF({column_name} = '') AS empty_count
    FROM
      `{dataset_id}.{table_id}`
    GROUP BY
      column_name
    """
    query_job = client.query(query)
    return list(query_job)

prod_table_details = get_table_details(prod_client, 'your_dataset', 'your_table')
dev_table_details = get_table_details(dev_client, 'your_dataset', 'your_table')

print(f"Production Table Size: {prod_table_details['table_size']} bytes, Rows: {prod_table_details['num_rows']}")
print(f"Development Table Size: {dev_table_details['table_size']} bytes, Rows: {dev_table_details['num_rows']}")

schema_differences = compare_schemas(prod_table_details['schema'], dev_table_details['schema'])
print("Schema differences:", schema_differences)

prod_column_sizes = analyze_column_sizes(prod_client, 'your_dataset', 'your_table')
dev_column_sizes = analyze_column_sizes(dev_client, 'your_dataset', 'your_table')

print("Production column sizes:")
for row in prod_column_sizes:
    print(row)

print("Development column sizes:")
for row in dev_column_sizes:
    print(row)

prod_storage_breakdown = analyze_storage_breakdown(prod_client, 'your_dataset', 'your_table')
dev_storage_breakdown = analyze_storage_breakdown(dev_client, 'your_dataset', 'your_table')

print("Production storage breakdown:")
for row in prod_storage_breakdown:
    print(row)

print("Development storage breakdown:")
for row in dev_storage_breakdown:
    print(row)

prod_null_and_empty_counts = get_null_and_empty_counts(prod_client, 'your_dataset', 'your_table')
dev_null_and_empty_counts = get_null_and_empty_counts(dev_client, 'your_dataset', 'your_table')

print("Production null and empty counts:")
for row in prod_null_and_empty_counts:
    print(row)

print("Development null and empty counts:")
for row in dev_null_and_empty_counts:
    print(row)
