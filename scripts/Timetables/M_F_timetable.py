from airflow.providers.timetable import timetable

# Define a custom timetable
custom_timetable = timetable(
    valid_window=("09:00", "17:00"),  # Define the time window
    weekdays=[0, 1, 2, 3, 4],        # Monday to Friday
)