import datetime as dt

from schedex import PeriodicSchedule

schedule = PeriodicSchedule.periodically(
    start_at=dt.datetime.now(tz=dt.UTC),
    delay=dt.timedelta(hours=1),
).with_jitter(
    dt.timedelta(minutes=60),
)
