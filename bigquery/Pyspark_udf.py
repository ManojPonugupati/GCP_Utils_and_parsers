from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf, PandasUDFType, from_json
import requests
import json
import pandas as pd

# Initialize Spark session
spark = SparkSession.builder \
    .appName("API Call Example") \
    .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.26.0") \
    .getOrCreate()

# Read data from BigQuery
df = spark.read.format("bigquery").option("table", "your_project.your_dataset.your_table").load()


# Define the API call function
def call_api(record):
    url = "https://api.example.com/endpoint"
    headers = {"Authorization": "Bearer your_token"}
    response = requests.post(url, json={"query": record}, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        return {}


# Define a function to parse the AST tree
def parse_ast(ast):
    columns = []
    where_clause = ""

    # Traverse the AST tree to extract columns and WHERE clause
    def traverse(node):
        nonlocal where_clause
        if isinstance(node, dict):
            if node.get("nodeType") == "COLUMN":
                columns.append(node.get("columnName"))
            elif node.get("nodeType") == "WHERE_CLAUSE":
                where_clause = node.get("sql")
            for key, value in node.items():
                traverse(value)
        elif isinstance(node, list):
            for item in node:
                traverse(item)

    traverse(ast)
    return {"columns": columns, "where_clause": where_clause}


# Define a Pandas UDF to apply the API call and parse the AST
@pandas_udf("string", PandasUDFType.SCALAR)
def api_udf(record: pd.Series) -> pd.Series:
    results = record.apply(lambda x: call_api(x))
    parsed_results = results.apply(lambda x: parse_ast(x))
    return parsed_results.apply(json.dumps)


# Apply the UDF to your DataFrame
result_df = df.withColumn("api_response", api_udf(col("query_column")))

# Convert the JSON string column back to a struct
result_df = result_df.withColumn("parsed_response",
                                 from_json(col("api_response"), "struct<columns:array<string>,where_clause:string>"))

# Select the required columns
final_df = result_df.select("parsed_response.columns", "parsed_response.where_clause")

# Save the result to BigQuery
final_df.write.format("bigquery").option("table", "your_project.your_dataset.result_table").mode("overwrite").save()

# Stop the Spark session
spark.stop()
