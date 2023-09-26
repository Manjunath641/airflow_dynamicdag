from airflow.providers.timetable import timetable
from datetime import date

# Define a timetable for working hours (e.g., 9 AM to 5 PM)
working_hours_timetable = timetable(valid_window=("09:00", "17:00"), weekdays=[0, 1, 2, 3, 4])

# Define a list of public holidays (date objects)
public_holidays = [date(2023, 12, 25), date(2023, 7, 4)]  # Add your holiday dates

# Function to check if a day is a public holiday
def is_public_holiday(day):
    return day in public_holidays

# Define a custom timetable that combines working hours and excludes public holidays
custom_timetable = working_hours_timetable & ~timetable(exclude_dates=public_holidays)

# Test the timetable for a specific date
test_date = date(2023, 12, 25)  # Christmas Day (a public holiday)
is_valid = custom_timetable.is_valid(test_date)

print(f"Is {test_date} a valid schedule date? {is_valid}")
