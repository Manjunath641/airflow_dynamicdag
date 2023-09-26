from airflow.providers.timetable import timetable

monthly_timetable = timetable(
    valid_window=("00:00", "00:01"),
    months=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],  # All months
    days_of_month=[1]  # 1st day of the month
)