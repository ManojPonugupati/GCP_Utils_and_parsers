import sqlparse
from sqlparse.sql import Where, Comparison, Identifier, Parenthesis, Function
from sqlparse.tokens import Keyword, Literal, Comparison as ComparisonToken

def extract_comparisons(tokens):
    comparisons = []
    for token in tokens:
        if isinstance(token, Comparison):
            comparisons.append(token)
        elif token.is_group:
            comparisons.extend(extract_comparisons(token.tokens))
    return comparisons

def get_identifier_name(identifier):
    if isinstance(identifier, Identifier):
        return identifier.get_real_name()
    elif isinstance(identifier, Parenthesis):
        return str(identifier)  # Keep as string to process later if needed
    elif identifier.ttype in (Keyword, Literal.String.Single, Literal.Number.Integer, Literal.Number.Float):
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

    for clause in where_clauses:
        comparisons = extract_comparisons(clause.tokens)
        for comp in comparisons:
            comp_tokens = [t for t in comp.tokens if not t.is_whitespace]
            if len(comp_tokens) >= 3:
                left, operator, right = comp_tokens[0], comp_tokens[1], comp_tokens[2]
                left_value = get_identifier_name(left)
                right_value = get_identifier_name(right)

                if operator.ttype == ComparisonToken:
                    if isinstance(left, Function) and is_static_value(right):
                        column_value_pairs.append((left.tokens[1].get_real_name(), right_value.strip("'"), True))
                    elif isinstance(right, Function) and is_static_value(left):
                        column_value_pairs.append((right.tokens[1].get_real_name(), left_value.strip("'"), True))
                    elif is_static_value(right):
                        column_value_pairs.append((left_value, right_value.strip("'"), True))
                    elif is_static_value(left):
                        column_value_pairs.append((right_value, left_value.strip("'"), True))
                    elif right_value.startswith("%") and right_value.endswith("%"):
                        column_value_pairs.append((left_value, right_value.strip("%"), True))
                    else:
                        column_value_pairs.append((left_value, right_value, False))

    return column_value_pairs

query1 = "select decrypt(cm15), cm13 from tabl21 a where decrypt(cm15) = '12345' and name like '%Manoj%' and cm13 = 1234567 and col3 = col4"
query2 = """
select * from  (
    select column_name clo4 from tabl1 where column1 = 'First String' 
) inner_1 left outer join ( 
    select colum1, column3 from tabl2 where column2 = 'Column2 String'
) main_1 on inner_1.col4 = main_1.column1  
where column3 > 100
"""
query3 = "select * from tabl where date(col3) = '2024-06-17' or col3 = date(col4)"

for query in [query1, query2, query3]:
    result = parse_query(query)
    for column, value, is_static in result:
        print(f"column: {column}, Matching_value: {value}, static: {is_static}")
