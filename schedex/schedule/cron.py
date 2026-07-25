import datetime as dt
import zoneinfo
from typing import Optional

import crontools as ct

from schedex.serializer.json import JsonSerializable

from .base import Schedule


class CronSchedule(JsonSerializable, Schedule, tag="cron"):
    """
    Crontab based schedule.

    :param crontab: crontab
    :param timezone: crontab timezone
    :param start_at: job first start time
    """

    crontab: str
    timezone: zoneinfo.ZoneInfo
    start_at: Optional[dt.datetime] = None

    def next_run(self, now: dt.datetime, prev: Optional[dt.datetime], count: int) -> Optional[dt.datetime]:
        crontab = ct.Crontab.parse(self.crontab, tz=self.timezone)

        return crontab.next_fire_time(max(now, self.start_at or dt.datetime.min.replace(tzinfo=dt.timezone.utc)))
