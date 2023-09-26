from pendulum import UTC, Date, DateTime, Time

from airflow.plugins_manager import AirflowPlugin
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable
import logging
import holidays

log = logging.getLogger(__name__)
try:
    holiday_calendar = holidays.India()
except ImportError:
    log.warning("Could not import pandas. Holidays will not be considered.")
    holiday_calendar = None


class AfterWorkdayTimetable(Timetable):
    def get_next_workday(self, d: DateTime, incr=1) -> DateTime:
        next_start = d
        while True:
            if next_start.weekday() in (5, 6):  # If next start is in the weekend go to next day
                next_start = next_start + incr * timedelta(days=1)
                continue
            if holiday_calendar is not None:
                holidays = holiday_calendar.holidays(start=next_start, end=next_start).to_pydatetime()
                if next_start in holidays:  # If next start is a holiday go to next day
                    next_start = next_start + incr * timedelta(days=1)
                    continue
            break
        return next_start
    def infer_manual_data_interval(self, run_after: DateTime) -> DataInterval:
        start = DateTime.combine((run_after - timedelta(days=1)).date(), Time.min).replace(tzinfo=UTC)
        # Skip backwards over weekends and holidays to find last run
        start = self.get_next_workday(start, incr=-1)
        return DataInterval(start=start, end=(start + timedelta(days=1)))
    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: DataInterval | None,
        restriction: TimeRestriction,
    ) -> DagRunInfo | None:
        if last_automated_data_interval is not None:  # There was a previous run on the regular schedule.
            last_start = last_automated_data_interval.start
            next_start = DateTime.combine((last_start + timedelta(days=1)).date(), Time.min).replace(
                tzinfo=UTC
            )
        else:  # This is the first ever run on the regular schedule.
            next_start = restriction.earliest
            if next_start is None:  # No start_date. Don't schedule.
                return None
            if not restriction.catchup:
                # If the DAG has catchup=False, today is the earliest to consider.
                next_start = max(next_start, DateTime.combine(Date.today(), Time.min).replace(tzinfo=UTC))
            elif next_start.time() != Time.min:
                # If earliest does not fall on midnight, skip to the next day.
                next_start = DateTime.combine(next_start.date() + timedelta(days=1), Time.min).replace(
                    tzinfo=UTC
                )
        # Skip weekends and holidays
        next_start = self.get_next_workday(next_start)

        if restriction.latest is not None and next_start > restriction.latest:
            return None  # Over the DAG's scheduled end; don't schedule.
        return DagRunInfo.interval(start=next_start, end=(next_start + timedelta(days=1)))


class WorkdayTimetablePlugin(AirflowPlugin):
    name = "workday_timetable_plugin"
    timetables = [AfterWorkdayTimetable]