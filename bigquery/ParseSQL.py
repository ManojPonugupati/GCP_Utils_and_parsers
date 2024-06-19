import sqlparse
from sqlparse.sql import Where, Comparison, Identifier, Parenthesis, Function, Token, IdentifierList
from sqlparse.tokens import Keyword, Literal, Comparison as ComparisonToken, Punctuation

def extract_comparisons(tokens):
    comparisons = []
    for token in tokens:
        if isinstance(token, Comparison):
            comparisons.append(token)
        elif token.is_group:
            comparisons.extend(extract_comparisons(token.tokens))
    return comparisons

def extract_in_clauses(tokens):
    in_clauses = []
    for token in tokens:
        if isinstance(token, Comparison):
            for sub_token in token.tokens:
                if sub_token.ttype == Keyword and sub_token.value.upper() == 'IN':
                    in_clauses.append(token)
        elif token.is_group:
            in_clauses.extend(extract_in_clauses(token.tokens))
    return in_clauses

def get_identifier_name(identifier):
    if isinstance(identifier, Identifier):
        return identifier.get_real_name()
    elif isinstance(identifier, Function):
        return get_identifier_name(identifier.tokens[-1])
    elif isinstance(identifier, Parenthesis):
        return get_identifier_name(identifier.tokens[1])  # Assume single token in parentheses
    elif identifier.ttype in (Keyword, Literal.String.Single, Literal.Number.Integer, Literal.Number.Float):
        return identifier.value.strip("'")
    elif isinstance(identifier, IdentifierList):
        names = [get_identifier_name(token) for token in identifier.tokens if isinstance(token, Identifier)]
        return names if len(names) > 1 else names[0]
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
        in_clauses = extract_in_clauses(clause.tokens)

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

        for clause in in_clauses:
            clause_tokens = [t for t in clause.tokens if not t.is_whitespace]
            left = clause_tokens[0]
            values = clause_tokens[3]
            if isinstance(values, Parenthesis):
                value_tokens = [t for t in values.tokens if not t.is_whitespace and t.ttype in (Literal.String.Single, Literal.Number.Integer, Literal.Number.Float)]
                for value_token in value_tokens:
                    column_value_pairs.append((get_identifier_name(left), value_token.value.strip("'"), True))

    # Filter out function names and only get column names
    final_pairs = []
    for column, value, is_static in column_value_pairs:
        if column:
            if '`' in column:
                col_name = column.split('(')[-1].split(')')[0].split(',')[-1].strip()
                final_pairs.append((col_name, value, is_static))
            else:
                final_pairs.append((column, value, is_static))

    return final_pairs

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
query4 = "select col2, col3, col4 from table where col4 in ('12345', '23456', '34567')"
query5 = "select col2, col3, col4 from table1 where `project_id.dataset_id.routine_id`('String_value', col1) = 'ManojKumar'"
query6 = "select col1, col2, col3 from tbl2 where `project_id.dataset_id.routine_id`(`project_id.dataset_id.another_routine`('tabl2', 'col4'), col4) = '12345'"

for query in [query1, query2, query3, query4, query5, query6]:
    result = parse_query(query)
    for column, value, is_static in result:
        print(f"column: {column}, Matching_value: {value}, static: {is_static}")
