import datetime as dt
import zoneinfo
from typing import Optional

from schedex import PeriodicSchedule, Schedule
from schedex.schedule.cron import CronSchedule
from schedex.serializer.json import JsonSerializable


class CombinedSchedule(Schedule, JsonSerializable):
    periodic: PeriodicSchedule | None = None
    cron: CronSchedule | None = None

    def next_run(self, now: dt.datetime, prev: Optional[dt.datetime], count: int) -> Optional[dt.datetime]:
        if self.periodic is not None:
            return self.periodic.next_run(now, prev, count)
        elif self.cron is not None:
            return self.cron.next_run(now, prev, count)
        else:
            return None


cron_schedule = CombinedSchedule(
    cron=CronSchedule(
        "30 0/12 1-7 * *",
        timezone=zoneinfo.ZoneInfo("UTC"),
        start_at=dt.datetime.now(tz=dt.UTC),
    )
)

periodic_schedule = CombinedSchedule(
    periodic=PeriodicSchedule.backoff(
        start_at=dt.datetime.now(tz=dt.UTC) + dt.timedelta(minutes=1),
        delay=dt.timedelta(seconds=30),
        multiplier=2.0,
    )
)
