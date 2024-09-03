import datetime

def julian_day_format(year, month, day):
    date = datetime.datetime(year, month, day)
    # Get the day of the year (Julian day)
    julian_day = date.strftime('%j')  # 'j' gives the day of the year (001-366)
    # Combine year and Julian day
    julian_date = f"{year}{julian_day}"
    return julian_date

# Example usage
year = 2024
month = 9
day = 1

julian_day_result = julian_day_format(year, month, day)
print(f"The Julian Day in YYYYJJJ format for {year}-{month:02d}-{day:02d} is {julian_day_result}.")
