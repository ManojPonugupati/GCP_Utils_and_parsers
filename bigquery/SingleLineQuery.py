import re

def convert_to_single_line(sql_query):
    # Replace all whitespace characters (spaces, tabs, newlines) with a single space
    single_line_query = re.sub(r'\s+', ' ', sql_query)
    # Remove any leading or trailing spaces
    return single_line_query.strip()

# Example usage
sql_query = """
SELECT               first_name,   last_name
FROM      users 
WHERE     age > 30                                         
ORDER BY  first_name;
"""

single_line_query = convert_to_single_line(sql_query)
print(single_line_query)
