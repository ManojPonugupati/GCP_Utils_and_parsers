import sqlparse
from sqlparse.sql import Where, Comparison, Identifier, Parenthesis, Function, Token
from sqlparse.tokens import Keyword, Literal, Comparison as ComparisonToken, Punctuation, Name
import ast

class TargetColumns:
    TARGET_COLUMNS = ['col1', 'col3']

def extract_comparisons(tokens):
    comparisons = []
    for token in tokens:
        if isinstance(token, Comparison):
            comparisons.append(token)
        elif token.is_group:
            comparisons.extend(extract_comparisons(token.tokens))
    return comparisons

def flatten_nested_tuples(input_str):
    # Parse the input string using ast.literal_eval
    parsed_tuple = ast.literal_eval(input_str)

    # Flatten nested tuples and extract strings
    def flatten(t):
        if isinstance(t, tuple):
            for item in t:
                yield from flatten(item)
        else:
            yield t

    # Extract strings from flattened structure
    result = [str(item) for item in flatten(parsed_tuple)]
    return result

def extract_in_clauses(tokens, in_found,name_found, indx, col_vals):
    in_clauses = []
    for token in tokens:
        print(token, in_found, name_found)
        if token.ttype == Keyword and token.value.upper() == 'IN':
            in_found = True
        elif token.ttype == Name and token.value in TargetColumns.TARGET_COLUMNS:
            col_vals.append(token.value)
            indx = indx + 1
            name_found = True
        elif in_found and name_found and token.ttype == Punctuation and token.value == '(':
            for item in flatten_nested_tuples(str(token.parent)):
                in_clauses.append({'col_name': col_vals[indx], 'value': item})
            in_found = False
            name_found = False
        elif token.is_group:
            in_clauses.extend(extract_in_clauses(token.tokens, in_found, name_found, indx, col_vals))
    return in_clauses

def get_identifier_name(identifier):
    if isinstance(identifier, Identifier):
        return identifier.get_real_name()
    elif isinstance(identifier, Function):
        return get_identifier_name(identifier.tokens[-1])
    elif isinstance(identifier, Parenthesis):
        return get_identifier_name(identifier.tokens[1])  # Assume single token in parentheses
    elif identifier.ttype in (Literal.String.Single, Literal.Number.Integer, Literal.Number.Float):
        return identifier.value.strip("'")
    else:
        return str(identifier)

def is_static_value(token):
    return token.ttype in (Literal.String.Single, Literal.Number.Integer, Literal.Number.Float)

def parse_query(query):
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

    def add_column_value_pair(column, value, is_static):
        if column in TargetColumns.TARGET_COLUMNS:
            column_value_pairs.append((column, value, is_static))

    for clause in where_clauses:
        comparisons = extract_comparisons(clause.tokens)
        in_clauses = extract_in_clauses(clause.tokens, False, False, -1, [])

        for comp in comparisons:
            comp_tokens = [t for t in comp.tokens if not t.is_whitespace]
            if len(comp_tokens) >= 3:
                left, operator, right = comp_tokens[0], comp_tokens[1], comp_tokens[2]
                left_value = get_identifier_name(left)
                right_value = get_identifier_name(right)

                if operator.ttype == ComparisonToken:
                    if is_static_value(right):
                        add_column_value_pair(left_value, right_value.strip("'"), True)
                    elif is_static_value(left):
                        add_column_value_pair(right_value, left_value.strip("'"), True)
                    else:
                        add_column_value_pair(left_value, right_value, False)

        for item in in_clauses:
            left = item['col_name']
            val = item['value']
            add_column_value_pair(left, val, True)

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
    result = parse_query(query)
    for column, value, is_static in result:
        print(f"Column: {column}, Matching_value: {value}, static: {is_static}")
    print("\n---\n")
