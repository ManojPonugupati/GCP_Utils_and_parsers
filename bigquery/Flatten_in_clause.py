import ast
import re

# Input strings
string1 = "(('12345','abc'),('xyz'),'Manoj, Kumar',((('Kumar'))))"
string2 = "('1234567')"
string3 = "(34567)"
string4 = "(01,02,03,04,05)"

# List of input strings
input_strings = [string1, string2, string3, string4]

# Function to recursively extract values from nested structures
def extract_values(item):
    if isinstance(item, (tuple, list)):
        for sub_item in item:
            yield from extract_values(sub_item)
    else:
        yield str(item)

# Function to preprocess the input strings for ast.literal_eval
def preprocess_string(input_string):
    # Replace single quotes with double quotes
    input_string = input_string.replace("'", '"')
    # Replace list-like structures with actual list literals
    input_string = re.sub(r'\(([^()]*?)\)', lambda m: f'[{m.group(1)}]', input_string)
    # Ensure that elements with leading zeros are treated as strings
    input_string = re.sub(r'(?<=\[|,)(\s*0\d+)', r'"\1"', input_string)
    return input_string

# Parse each string and extract values
for input_string in input_strings:
    preprocessed_string = preprocess_string(input_string)
    try:
        parsed_data = ast.literal_eval(preprocessed_string)
        for value in extract_values(parsed_data):
            print(value)
    except (SyntaxError, ValueError) as e:
        print(f"Error: Unable to parse the input string: {input_string}. Error: {e}")
