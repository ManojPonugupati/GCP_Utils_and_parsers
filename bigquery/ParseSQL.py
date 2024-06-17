import sqlparse
import re

def parse_query(query):
    columns = []
    where_clause = None

    # Split query into tokens
    parsed = sqlparse.parse(query)[0]
    tokens = parsed.tokens
    print(tokens)
    # Identify WHERE clause
    for token in tokens:
        print('--------------------------------')
        print(token._get_repr_name())
        print(token._get_repr_value())
        print(token.ttype)
        if token.ttype is None and token._get_repr_name() == 'Where':
            where_clause = token
            print("Where clasuse is " + str(where_clause))
            break
    print('=========================')
    print(where_clause)
    if where_clause:
        # Extract columns and values from WHERE clause
        where_str = str(where_clause).replace('where ', '', 1).strip()
        conditions = re.split(r' and | or ', where_str, flags=re.IGNORECASE)

        for condition in conditions:
            match = re.match(r"(\w+|\w+\(\w+\))\s*([<>=!]+|like|between)\s*('[^']*'|[^'\s]*)", condition, re.IGNORECASE)
            if match:
                column = match.group(1)
                value = match.group(3)
                static = True if re.match(r"'.*'", value) or value.isdigit() else False
                if static:
                    value = value.strip("'")
                columns.append([column, value, static])
            else:
                columns_in_condition = re.findall(r'(\w+)', condition)
                if columns_in_condition:
                    columns.append([columns_in_condition[0], columns_in_condition[1], False])

    return columns


def format_output(parsed_data):
    header = ["column", "Matching_value", "static"]
    rows = [header]
    for row in parsed_data:
        rows.append(row)
    return rows


query = "select decrypt(cm15), cm13 from tabl21 a where decrypt(cm15) = '12345' and name like '%Manoj%' and cm13 = 1234567 and col3 = col4"
parsed_data = parse_query(query)
output = format_output(parsed_data)

for row in output:
    print("\t".join(map(str, row)))
