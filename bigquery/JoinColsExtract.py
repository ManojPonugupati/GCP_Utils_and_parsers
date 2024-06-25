import sqlparse
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML


def extract_tables_and_columns(query):
    # Parse the SQL query
    parsed = sqlparse.parse(query)[0]
    tables = []
    columns = []

    # Flag to identify the part of the query we're in
    from_seen = False
    join_seen = False
    where_seen = False

    for token in parsed.tokens:
        if token.ttype is DML and token.value.upper() == 'SELECT':
            continue

        # Identify table names
        if token.ttype is Keyword and token.value.upper() == 'FROM':
            from_seen = True
            continue

        if from_seen and token.ttype is Keyword and token.value.upper() == 'ON':
            join_seen = True
            from_seen = False
            continue

        if join_seen and token.ttype is Keyword and token.value.upper() == 'WHERE':
            where_seen = True
            join_seen = False
            continue

        if from_seen or join_seen:
            if isinstance(token, IdentifierList):
                for identifier in token.get_identifiers():
                    tables.append(identifier.get_real_name())
            elif isinstance(token, Identifier):
                tables.append(token.get_real_name())
            from_seen = False

        # Extract columns used in join condition
        if join_seen:
            if isinstance(token, IdentifierList):
                for identifier in token.get_identifiers():
                    columns.append(identifier.get_real_name())
            elif isinstance(token, Identifier):
                columns.append(token.get_real_name())

    # Extract columns used in where condition
    if where_seen:
        where_tokens = token.tokens if isinstance(token, IdentifierList) else [token]
        for token in where_tokens:
            if isinstance(token, Identifier):
                columns.append(token.get_real_name())

    # Deduplicate and return the list
    columns = list(set(columns))

    return tables, columns


# SQL query
query = "SELECT a.*, b.* FROM table1 a ON tabl2 b ON a.col1 = b.cola AND a.colx = b.col4 WHERE a.col2 IS NOT NULL"

tables, columns = extract_tables_and_columns(query)

# Print the result
for table in tables:
    print(f"Table: {table}")

for column in columns:
    print(f"Column: {column}")
