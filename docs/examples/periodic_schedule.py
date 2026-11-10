import datetime as dt

from schedex import PeriodicSchedule

schedule = PeriodicSchedule.backoff(
    start_at=dt.datetime.now(tz=dt.UTC) + dt.timedelta(minutes=1),
    delay=dt.timedelta(seconds=30),
    multiplier=2.0,
).with_max_count(6)
