# Python program to call main() using __name__
import datetime
# function - main()
def main():
    hi()


# function - hi()
def hi():
    print("Hi")

    # Get the current year
    current_year = datetime.datetime.now().year

    # Create an empty dictionary
    Dict = {}

    # Loop through the months from 1 to 12
    for month in range(1, 13):
        # Format the month with leading zero if necessary
        formatted_month = str(month).zfill(2)
        # Create the key using the current year and formatted month
        key = f"{current_year}-{formatted_month}"

        # Create the value for the key
        value = f"{formatted_month} month"

        # Add the key-value pair to the dictionary
        Dict[key] = value

    # Print the dictionary
    print(Dict)


# calling main function using __name__
if __name__ == "__main__":
    main()
