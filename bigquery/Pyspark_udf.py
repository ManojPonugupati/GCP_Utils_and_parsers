from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf, PandasUDFType
import requests
import pandas as pd

# Initialize Spark session
spark = SparkSession.builder \
    .appName("API Call Example") \
    .getOrCreate()

# Read data from your table into a DataFrame
# Adjust this to your data source, e.g., reading from a CSV file, database, etc.
df = spark.read.format("jdbc").options(
    url="jdbc:mysql://your_mysql_server:3306/your_database",
    driver="com.mysql.jdbc.Driver",
    dbtable="your_table",
    user="your_user",
    password="your_password"
).load()

# Define the API call function
def call_api(record):
    url = "https://api.example.com/endpoint"
    headers = {"Authorization": "Bearer your_token"}
    response = requests.post(url, json=record, headers=headers)
    return response.json()

# Define a Pandas UDF to apply the API call function
@pandas_udf("string", PandasUDFType.SCALAR)
def api_udf(record: pd.Series) -> pd.Series:
    return record.apply(lambda x: call_api(x))

# Apply the UDF to your DataFrame
# Assuming 'record_column' contains the data you need to send to the API
result_df = df.withColumn("api_response", api_udf(col("record_column")))

# Show the result
result_df.show()

# Save the result to a new table or file
result_df.write.format("jdbc").options(
    url="jdbc:mysql://your_mysql_server:3306/your_database",
    driver="com.mysql.jdbc.Driver",
    dbtable="your_result_table",
    user="your_user",
    password="your_password"
).mode("append").save()

# Stop the Spark session
spark.stop()
