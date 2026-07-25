import datetime as dt
from typing import Optional

from schedex.serializer.json import JsonSerializable

from .base import Schedule


class CombinedSchedule(JsonSerializable, Schedule, tag="combined"):
    """
    Scheduler that combines multiple schedules.
    """

    schedules: tuple[Schedule, ...]

    def next_run(self, now: dt.datetime, prev: Optional[dt.datetime], count: int) -> Optional[dt.datetime]:
        next_run = [
            next_run for schedule in self.schedules if (next_run := schedule.next_run(now, prev, count)) is not None
        ]
        return min(next_run) if next_run else None
