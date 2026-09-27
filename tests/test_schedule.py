import datetime as dt
import json
import zoneinfo
from typing import Any, Optional

import pytest

from schedex.schedule import PeriodicSchedule, Schedule
from schedex.schedule.cron import CronSchedule

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    "schedule, fields",
    [
        (
            PeriodicSchedule(
                start_at=dt.datetime(2026, 8, 25),
                delay=None,
                stop_at=None,
                backoff_multiplier=1.0,
                max_delay=None,
                max_count=None,
                drifting=True,
            ),
            {
                "type": "periodic",
                "start_at": "2026-08-25T00:00:00",
                "delay": None,
                "stop_at": None,
                "backoff_multiplier": 1.0,
                "max_delay": None,
                "max_count": None,
                "jitter": None,
                "drifting": True,
            },
        ),
        (
            PeriodicSchedule(
                start_at=dt.datetime(2026, 8, 25, 12, 5, 1, tzinfo=dt.timezone.utc),
                delay=dt.timedelta(days=1, seconds=1, microseconds=1),
                stop_at=dt.datetime(2100, 12, 31, 23, 59, 59, tzinfo=dt.timezone.utc),
                backoff_multiplier=555555.555555,
                max_delay=dt.timedelta(days=30000, seconds=80000, microseconds=555555),
                max_count=2**64,
                drifting=False,
            ),
            {
                "type": "periodic",
                "start_at": "2026-08-25T12:05:01Z",
                "delay": "P1DT1.000001S",
                "stop_at": "2100-12-31T23:59:59Z",
                "backoff_multiplier": 555555.555555,
                "max_delay": "P30000DT80000.555555S",
                "max_count": 2**64,
                "jitter": None,
                "drifting": False,
            },
        ),
        (
            CronSchedule(
                crontab="* * * * *",
                timezone=zoneinfo.ZoneInfo("Europe/Moscow"),
                start_at=dt.datetime(2026, 8, 25, 12, 5, 1, tzinfo=dt.timezone.utc),
            ),
            {
                "type": "cron",
                "crontab": "* * * * *",
                "timezone": "Europe/Moscow",
                "start_at": "2026-08-25T12:05:01Z",
            },
        ),
    ],
)
def test_schedule_serialization(schedule: Schedule, fields: dict[str, Any]):
    actual_data = schedule.serialize()
    expected_data = json.dumps(fields, separators=(",", ":")).encode()
    assert actual_data == expected_data

    actual_schedule = type(schedule).deserialize(actual_data)
    assert actual_schedule == schedule


@pytest.mark.parametrize(
    "schedule, now, prev, count, result",
    [
        (
            PeriodicSchedule(
                start_at=dt.datetime(2026, 8, 29, 12, 1, 0),
                delay=None,
            ),
            dt.datetime(2026, 8, 29, 12, 0, 0),
            None,
            0,
            dt.datetime(2026, 8, 29, 12, 1, 0),
        ),
        (
            PeriodicSchedule(
                start_at=dt.datetime(2026, 8, 29, 12, 0, 0),
                delay=dt.timedelta(minutes=1),
            ),
            dt.datetime(2026, 8, 29, 12, 1, 0),
            dt.datetime(2026, 8, 29, 12, 0, 0),
            0,
            dt.datetime(2026, 8, 29, 12, 1, 0),
        ),
        (
            PeriodicSchedule(
                start_at=dt.datetime(2026, 8, 29, 12, 0, 0),
                delay=dt.timedelta(minutes=1),
                stop_at=dt.datetime(2026, 8, 29, 12, 3, 0),
            ),
            dt.datetime(2026, 8, 29, 12, 3, 0),
            dt.datetime(2026, 8, 29, 12, 2, 0),
            0,
            None,
        ),
        (
            PeriodicSchedule(
                start_at=dt.datetime(2026, 8, 29, 12, 0, 0),
                delay=dt.timedelta(minutes=1),
                backoff_multiplier=2.0,
            ),
            dt.datetime(2026, 8, 29, 12, 2, 0),
            dt.datetime(2026, 8, 29, 12, 0, 0),
            1,
            dt.datetime(2026, 8, 29, 12, 2, 0),
        ),
        (
            PeriodicSchedule(
                start_at=dt.datetime(2026, 8, 29, 12, 0, 0),
                delay=dt.timedelta(minutes=1),
                backoff_multiplier=2.0,
            ),
            dt.datetime(2026, 8, 29, 12, 2, 0),
            dt.datetime(2026, 8, 29, 12, 0, 0),
            3,
            dt.datetime(2026, 8, 29, 12, 8, 0),
        ),
        (
            PeriodicSchedule(
                start_at=dt.datetime(2026, 8, 29, 12, 0, 0),
                delay=dt.timedelta(minutes=2),
                max_delay=dt.timedelta(minutes=1),
            ),
            dt.datetime(2026, 8, 29, 12, 1, 0),
            dt.datetime(2026, 8, 29, 12, 0, 0),
            0,
            dt.datetime(2026, 8, 29, 12, 1, 0),
        ),
        (
            PeriodicSchedule(
                start_at=dt.datetime(2026, 8, 29, 12, 0, 0),
                delay=dt.timedelta(minutes=1),
                max_count=1,
            ),
            dt.datetime(2026, 8, 29, 12, 1, 0),
            dt.datetime(2026, 8, 29, 12, 0, 0),
            1,
            None,
        ),
        (
            PeriodicSchedule(
                start_at=dt.datetime(2026, 8, 29, 12, 0, 0),
                delay=dt.timedelta(minutes=1),
                drifting=True,
            ),
            dt.datetime(2026, 8, 29, 12, 2, 0),
            dt.datetime(2026, 8, 29, 12, 0, 0),
            0,
            dt.datetime(2026, 8, 29, 12, 3, 0),
        ),
        (
            PeriodicSchedule(
                start_at=dt.datetime(2026, 8, 29, 12, 0, 0),
                delay=dt.timedelta(minutes=1),
                drifting=False,
            ),
            dt.datetime(2026, 8, 29, 12, 2, 0),
            dt.datetime(2026, 8, 29, 12, 0, 0),
            0,
            dt.datetime(2026, 8, 29, 12, 1, 0),
        ),
        (
            PeriodicSchedule(
                start_at=dt.datetime(2026, 8, 29, 12, 0, 0),
                delay=None,
                jitter=dt.timedelta(minutes=0),
            ),
            dt.datetime(2026, 8, 29, 12, 0, 0),
            None,
            0,
            dt.datetime(2026, 8, 29, 12, 0, 0),
        ),
    ],
)
def test_periodic_schedule(
    schedule: Schedule,
    now: dt.datetime,
    prev: Optional[dt.datetime],
    count: int,
    result: Optional[dt.datetime],
):
    assert result == schedule.next_run(now, prev, count)
