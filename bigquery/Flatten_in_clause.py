import re

# Input strings
string1 = "(('12345','abc'),('xyz'),'Manoj Kumar',((('Kumar'))))"
string2 = "('1234567')"
string3 = "(34567)"
string4 = "(01,02,03,04,05)"

# Combine all strings into one for simplicity
combined_string = string1 + string2 + string3 + string4

# Use regex to find all sequences of alphanumeric characters, sequences of alphanumeric characters with spaces within single quotes, or numbers within parentheses
pattern = re.compile(r"'([^']*)'|\(([^()]*)\)|\b\w+\b")
matches = pattern.findall(combined_string)

# Flatten the list of matches and filter out empty strings
flattened_matches = [item for sublist in matches for item in sublist if item]

# Print each match on a new line
for match in flattened_matches:
    values = match.split(',')
    for value in values:
        print(value.strip())
