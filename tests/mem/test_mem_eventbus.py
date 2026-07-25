import datetime as dt
import uuid

import pytest

from schedex.eventbus import Event, EventKind
from schedex.mem import eventbus

pytestmark = pytest.mark.unit


@pytest.mark.timeout(1.0)
async def test_inmemory_even_bus():
    sender, receiver = eventbus.build()

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
