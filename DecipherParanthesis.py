import ast

String1 = "(('12345','abc'),('xyz'),'Manoj',((('Kumar'))))"
String2 = "('1234567')"
String3 = "(34567)"

# Function to flatten nested tuples and extract strings
def extract_strings(input_str):
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

# Extract strings from each input string
output1 = extract_strings(String1)
output2 = extract_strings(String2)
output3 = extract_strings(String3)

# Print the outputs
for item in output1:
    print(item)
for item in output2:
    print(item)
for item in output3:
    print(item)