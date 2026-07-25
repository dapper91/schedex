import datetime as dt
import math
import random as rnd
from typing import Optional, Self

from schedex.serializer.json import JsonSerializable

from .base import Schedule


class PeriodicSchedule(JsonSerializable, Schedule, tag="periodic"):
    """
    Periodic task schedule.

    :param start_at: the time when the schedule starts
    :param delay: delay between executions
    :param stop_at: timestamp after which the job will be completed
    :param backoff_multiplier: exponential backoff multiplier
    :param max_delay: maximum execution delay
    :param max_count: maximum number of executions
    :param jitter: random delay variation (uniformly distributed from 0 to jitter)
    :param drifting: allows the schedule to drift. If for some reason the execution is behind the schedule
                     the next run time will be calculated based on the actual run time not the scheduled run time.
                     This option helps to prevent uncontrolled task spawning on workers long term outage.
    """

    start_at: dt.datetime
    delay: Optional[dt.timedelta]
    stop_at: Optional[dt.datetime] = None
    backoff_multiplier: float = 1.0
    max_delay: Optional[dt.timedelta] = None
    max_count: Optional[int] = None
    jitter: Optional[dt.timedelta] = None
    drifting: bool = False

    @classmethod
    def after(cls, delay: dt.timedelta) -> Self:
        """
        Run a job once after `delay`.
        """

        return cls(
            start_at=dt.datetime.now(dt.UTC) + delay,
            delay=None,
            max_count=1,
        )

    @classmethod
    def at(cls, at: dt.datetime) -> Self:
        """
        Run a job once at `datetime`.
        """

        return cls(
            start_at=at,
            delay=None,
            max_count=1,
        )

    @classmethod
    def periodically(cls, start_at: dt.datetime, delay: dt.timedelta) -> Self:
        """
        Run a job each `delay` interval starting at `start_at` .
        """

        return cls(
            start_at=start_at,
            delay=delay,
        )

    @classmethod
    def backoff(
        cls,
        start_at: dt.datetime,
        delay: dt.timedelta,
        multiplier: float,
        max_delay: Optional[dt.timedelta] = None,
    ) -> Self:
        """
        Run a job periodically starting at `start_at` with interval
        increasing exponentially from `delay` to `max_delay` by `multiplier`.
        """

        if multiplier < 0.0:
            raise AssertionError("multiplier cannot be negative")

        return cls(
            start_at=start_at,
            delay=delay,
            backoff_multiplier=multiplier,
            max_delay=max_delay,
        )

    def stopped_at(self, at: dt.datetime) -> Self:
        """
        Cancel a job at `datetime`.
        """

        return self.__class__(
            start_at=self.start_at,
            stop_at=at,
            delay=self.delay,
            backoff_multiplier=self.backoff_multiplier,
            max_delay=self.max_delay,
            max_count=self.max_count,
            drifting=self.drifting,
            jitter=self.jitter,
        )

    def with_max_count(self, count: int) -> Self:
        """
        Cancel a job after `count` executions.
        """

        return self.__class__(
            start_at=self.start_at,
            stop_at=self.stop_at,
            delay=self.delay,
            backoff_multiplier=self.backoff_multiplier,
            max_delay=self.max_delay,
            max_count=count,
            drifting=self.drifting,
            jitter=self.jitter,
        )

    def with_jitter(self, jitter: dt.timedelta) -> Self:
        """
        Adds a jitter to the execution time.
        """

        return self.__class__(
            start_at=self.start_at,
            stop_at=self.stop_at,
            delay=self.delay,
            backoff_multiplier=self.backoff_multiplier,
            max_delay=self.max_delay,
            max_count=self.max_count,
            drifting=self.drifting,
            jitter=jitter,
        )

    def may_drift(self, drifting: bool) -> Self:
        """
        Allows the schedule to drift. If for some reason the task is behind the schedule
        the next run time will be calculated based on the actual run time not the scheduled run time.
        This option helps to prevent uncontrolled task spawning on workers long term outage.
        """

        return self.__class__(
            start_at=self.start_at,
            stop_at=self.stop_at,
            delay=self.delay,
            backoff_multiplier=self.backoff_multiplier,
            max_delay=self.max_delay,
            max_count=self.max_count,
            drifting=drifting,
            jitter=self.jitter,
        )

    def next_run(self, now: dt.datetime, prev: Optional[dt.datetime], count: int) -> Optional[dt.datetime]:
        """
        Returns the next job execution timestamp. If `None` is returned the job must not be executed anymore.

        :param now: current time
        :param prev: time of the previous execution
        :param count: number of job executions
        :return: the next job execution timestamp
        """

        if self.max_count is not None and count >= self.max_count:
            return None

        if prev is not None:
            if self.drifting:
                prev = max(prev, now)

            try:
                delay = (self.delay or dt.timedelta.max) * self.backoff_multiplier**count
            except OverflowError:
                delay = dt.timedelta.max

            delay = min(self.max_delay or dt.timedelta.max, delay)

            try:
                next_run = prev + delay
            except OverflowError:
                return None

            if self.stop_at is not None and next_run >= self.stop_at:
                return None

        else:
            next_run = self.start_at if not self.drifting else max(self.start_at, now)

        if self.jitter is not None:
            jitter = rnd.uniform(0, self.jitter.total_seconds())
            milliseconds, seconds = math.modf(jitter)
            return next_run + dt.timedelta(seconds=seconds, milliseconds=int(milliseconds * 1000))
        else:
            return next_run
