from airflow.providers.timetable import timetable

hourly_timetable = timetable(
    valid_window=("07:00", "08:00"),
    valid_window=("12:00", "13:00"),
    valid_window=("18:00", "19:00"),
    weekdays=[0, 1, 2, 3, 4, 5, 6]  # Every day
)