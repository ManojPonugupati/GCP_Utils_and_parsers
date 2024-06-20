import csv
import sqlparse
from collections import defaultdict
import re

# Function to extract join conditions from a SQL query
def extract_join_conditions(query):
    join_conditions = []
    parsed = sqlparse.parse(query)
    for stmt in parsed:
        for token in stmt.tokens:
            if isinstance(token, sqlparse.sql.TokenList):
                for subtoken in token.tokens:
                    if isinstance(subtoken, sqlparse.sql.TokenList):
                        for subsubtoken in subtoken.tokens:
                            if isinstance(subsubtoken, sqlparse.sql.Comparison) and 'JOIN' in subsubtoken.parent.value.upper():
                                join_conditions.append(subsubtoken.value)
    return join_conditions

# Function to extract column names from join conditions
def extract_column_names(join_conditions):
    columns = []
    for condition in join_conditions:
        columns.extend(re.findall(r'\b\w+\.\w+\b', condition))
    return columns

# Dictionary to count column occurrences
column_count = defaultdict(int)

# Read the query logs CSV file
with open('query_logs.csv', 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        query = row['query']
        join_conditions = extract_join_conditions(query)
        columns = extract_column_names(join_conditions)
        for col in columns:
            column_count[col] += 1

# Identify the most frequently joined column
most_frequent_column = max(column_count, key=column_count.get)
print(f"The most frequently joined column is: {most_frequent_column} with {column_count[most_frequent_column]} joins")
