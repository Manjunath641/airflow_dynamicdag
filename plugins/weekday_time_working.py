from datetime import timedelta
from typing import Optional
from pendulum import Date, DateTime, Time, timezone

from airflow.plugins_manager import AirflowPlugin
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable

UTC = timezone("UTC")


class CustomHolidays(Timetable):

    def infer_manual_data_interval(self, run_after: DateTime) -> DataInterval:
        delta = timedelta(days=1)
        print(delta)

        start = (run_after-delta).set(hour=6, minute=0).replace(tzinfo=UTC)
        end = (run_after-delta).set(hour=16, minute=30).replace(tzinfo=UTC)
        return DataInterval(start=start, end=end)

class CustomHolidaysPlugin(AirflowPlugin):
    name = "uneven_intervals_timetable_plugin"
    timetables = [CustomHolidays]