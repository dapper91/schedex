import datetime as dt
import uuid

import psycopg_pool as ppg_pool
import pytest

from schedex.contrib.psycopg import PsycopgEventReceiver, PsycopgEventSender
from schedex.eventbus import Event, EventKind

pytestmark = pytest.mark.psycopg


@pytest.mark.timeout(1.0)
async def test_even_bus(connection_pool: ppg_pool.AsyncConnectionPool):
    sender = PsycopgEventSender(connection_pool)
    receiver = PsycopgEventReceiver(connection_pool)

    expected_events = [
        Event(id=uuid.uuid4().hex, timestamp=dt.datetime.now(tz=dt.timezone.utc), kind=EventKind.JobReady),
        Event(id=uuid.uuid4().hex, timestamp=dt.datetime.now(tz=dt.timezone.utc), kind=EventKind.TaskReady),
    ]
    async with receiver.connect() as event_source:
        for event in expected_events:
            await sender.send(event)

        for expected_event in expected_events:
            actual_event = await anext(event_source)
            assert actual_event == expected_event
