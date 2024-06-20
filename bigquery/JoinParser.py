import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Comparison
from sqlparse.tokens import Keyword, DML, Punctuation

def is_subselect(parsed):
    if not parsed.is_group:
        return False
    for item in parsed.tokens:
        if item.ttype is DML and item.value.upper() == 'SELECT':
            return True
    return False

def extract_from_part(parsed):
    from_seen = False
    for item in parsed.tokens:
        if from_seen:
            if is_subselect(item):
                yield from extract_from_part(item)
            elif item.ttype is Keyword and item.value.upper() in ['ORDER BY', 'GROUP BY', 'HAVING', 'LIMIT', 'WHERE']:
                return
            else:
                yield item
        elif item.ttype is Keyword and item.value.upper() == 'FROM':
            from_seen = True

def extract_tables(parsed):
    tables = {}
    for item in extract_from_part(parsed):
        if isinstance(item, IdentifierList):
            for identifier in item.get_identifiers():
                table = identifier.get_real_name()
                alias = identifier.get_alias() or table
                tables[alias] = table
        elif isinstance(item, Identifier):
            table = item.get_real_name()
            alias = item.get_alias() or table
            tables[alias] = table
    return tables

def extract_join_conditions(parsed, tables):
    columns = set()
    for item in parsed.tokens:
        if isinstance(item, sqlparse.sql.Where):
            for token in item.tokens:
                if isinstance(token, Comparison):
                    for sub_token in token.tokens:
                        if isinstance(sub_token, Identifier):
                            parts = sub_token.get_real_name().split('.')
                            if len(parts) == 2:
                                alias, column = parts
                                if alias in tables:
                                    table = tables[alias]
                                    columns.add(f"{table} {column}")
        elif isinstance(item, sqlparse.sql.Token) and item.ttype is Punctuation and item.value.upper() == 'JOIN':
            join = item.parent
            for token in join.tokens:
                if isinstance(token, Comparison):
                    for sub_token in token.tokens:
                        if isinstance(sub_token, Identifier):
                            parts = sub_token.get_real_name().split('.')
                            if len(parts) == 2:
                                alias, column = parts
                                if alias in tables:
                                    table = tables[alias]
                                    columns.add(f"{table} {column}")
        elif is_subselect(item):
            sub_tables = extract_tables(item)
            sub_columns = extract_join_conditions(item, sub_tables)
            columns.update(sub_columns)
    return columns

def extract_tables_and_columns(query):
    parsed = sqlparse.parse(query)[0]
    tables = extract_tables(parsed)
    columns = extract_join_conditions(parsed, tables)
    return columns

query = "select a.*, b.* from table1 a join tabl2 b on a.col1 = b.cola and a.colx = b.col4 where a.col2 is not null"
columns = extract_tables_and_columns(query)
for col in sorted(columns):
    print(col)
