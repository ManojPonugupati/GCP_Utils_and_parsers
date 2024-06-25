import sqlparse
from sqlparse.sql import Where, Comparison, Identifier, Parenthesis, Function
from sqlparse.tokens import Keyword, Literal, Comparison as ComparisonToken, Punctuation, Name
import ast

class TargetColumns:
    TARGET_COLUMNS = ['col1', 'col3']

def extract_comparisons(tokens, target_columns):
    comparisons = []
    for token in tokens:
        if isinstance(token, Comparison):
            left = get_identifier_name(token.left)
            if left in target_columns:
                comparisons.append(token)
        elif token.is_group:
            comparisons.extend(extract_comparisons(token.tokens, target_columns))
    return comparisons

def flatten_nested_tuples(input_str):
    parsed_tuple = ast.literal_eval(input_str)
    def flatten(t):
        if isinstance(t, tuple):
            for item in t:
                yield from flatten(item)
        else:
            yield t
    result = [str(item) for item in flatten(parsed_tuple)]
    return result

def extract_in_clauses(tokens, target_columns):
    in_clauses = []
    in_found = False
    current_col = None

    for token in tokens:
        print(in_found, token.value, current_col)
        if token.ttype == Keyword and token.value.upper() == 'IN':
            in_found = True
        elif token.ttype == Name and token.value in target_columns:
            current_col = token.value
        elif in_found and token.ttype == Punctuation and token.value == '(' and current_col:
            for item in flatten_nested_tuples(str(token.parent)):
                in_clauses.append({'col_name': current_col, 'value': item})
            in_found = False
            current_col = None
        elif token.is_group:
            in_clauses.extend(extract_in_clauses(token.tokens, target_columns))
    return in_clauses

def get_identifier_name(identifier):
    if isinstance(identifier, Identifier):
        return identifier.get_real_name()
    elif isinstance(identifier, Function):
        return get_identifier_name(identifier.tokens[-1])
    elif isinstance(identifier, Parenthesis):
        return get_identifier_name(identifier.tokens[1])
    elif identifier.ttype in (Literal.String.Single, Literal.Number.Integer, Literal.Number.Float):
        return identifier.value.strip("'")
    else:
        return str(identifier)

def is_static_value(token):
    return token.ttype in (Literal.String.Single, Literal.Number.Integer, Literal.Number.Float)

def parse_query(query, target_columns):
    parsed_query = sqlparse.parse(query)[0]
    where_clauses = []

    def find_where_clauses(token):
        if isinstance(token, Where):
            where_clauses.append(token)
        elif token.is_group:
            for sub_token in token.tokens:
                find_where_clauses(sub_token)

    find_where_clauses(parsed_query)
    column_value_pairs = []

    for clause in where_clauses:
        comparisons = extract_comparisons(clause.tokens, target_columns)
        in_clauses = extract_in_clauses(clause.tokens, target_columns)

        for comp in comparisons:
            comp_tokens = [t for t in comp.tokens if not t.is_whitespace]
            if len(comp_tokens) >= 3:
                left, operator, right = comp_tokens[0], comp_tokens[1], comp_tokens[2]
                left_value = get_identifier_name(left)
                right_value = get_identifier_name(right)

                if operator.ttype == ComparisonToken:
                    if is_static_value(right):
                        column_value_pairs.append((left_value, right_value.strip("'"), True))
                    elif is_static_value(left):
                        column_value_pairs.append((right_value, left_value.strip("'"), True))
                    else:
                        column_value_pairs.append((left_value, right_value, False))

        for item in in_clauses:
            left = item['col_name']
            val = item['value']
            column_value_pairs.append((left, val, True))

    return column_value_pairs

# Example queries with IN clause
queries = [
    """
    select col2, col3, col4 from table where 
     col4 in ('12345')
     and col1 in ('sample')
     and col3 in ('112345')
     and col2 in (2345)
     and col5 = 'col5'
    """
]

# Process the queries
for query in queries:
    print(f"Query: {query.strip()}")
    result = parse_query(query, TargetColumns.TARGET_COLUMNS)
    for column, value, is_static in result:
        print(f"Column: {column}, Matching_value: {value}, static: {is_static}")
    print("\n---\n")
