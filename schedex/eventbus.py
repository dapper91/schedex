import abc
import asyncio as aio
import contextlib as cl
import datetime as dt
import uuid
from enum import IntEnum
from types import TracebackType
from typing import AsyncGenerator, AsyncIterator, Optional, Self

import msgspec

from schedex.guard import ErrorGuard, RetryPolicy, exponential_delay
from schedex.serializer import Serializable
from schedex.serializer.json import JsonSerializable


class EventKind(IntEnum):
    """
    Event type.
    """

    # Job created or released.
    JobReady = 0
    # Job execution canceled.
    JobCanceled = 1
    # Job completed.
    JobCompleted = 2

    # Task created or released.
    TaskReady = 3
    # Task successfully finished.
    TaskSucceeded = 4
    # Task execution failed.
    TaskFailed = 5

    # Scheduler node joined the cluster.
    NodeJoined = 6
    # Scheduler node left the cluster.
    NodeLeft = 7

    def __str__(self) -> str:
        return self.name


class Event(JsonSerializable, Serializable):
    """
    Scheduler event.

    :param kind: event type
    :param timestamp: timestamp when the event was created
    :param id: event unique identifier
    """

    kind: EventKind
    timestamp: dt.datetime = msgspec.field(default_factory=lambda: dt.datetime.now(tz=dt.timezone.utc))
    id: str = msgspec.field(default_factory=lambda: str(uuid.uuid4()))

    def __str__(self) -> str:
        return f"{self.kind}[{self.id}]"


class EventSender(abc.ABC):
    """
    Event sender.
    """

    @abc.abstractmethod
    async def send(self, event: Event) -> None:
        """
        Sends an event.

        :param event: event to be sent
        """


class EventReceiver(abc.ABC):
    """
    Event receiver.
    """

    @abc.abstractmethod
    @cl.asynccontextmanager
    def connect(self) -> AsyncGenerator[AsyncIterator[Event], None]:
        """
        Opens an event stream.
        """


class BufferedEventSender(EventSender):
    """
    Event sender adapter that provides an api to send event synchronously.
    The event is not actually sent but saved in internal buffer and scheduled for sending ASAP.

    :param wrapped: wrapped event sender
    :param buffer_size: internal queue max size
    :param background_sender: start background sender
    :param retry_policy: retry policy for background event sender
    """

    def __init__(
        self,
        wrapped: EventSender,
        buffer_size: Optional[int] = None,
        background_sender: bool = False,
        retry_policy: Optional[RetryPolicy] = None,
        graceful_timeout: float = 0,
    ):
        self._wrapped = wrapped
        self._buffer = aio.Queue[Event](maxsize=buffer_size or 0)
        self._bg_sender: Optional[aio.Task[None]] = (
            aio.create_task(self._send_buffered_events()) if background_sender else None
        )
        self._retry_policy = retry_policy or exponential_delay(
            initial=dt.timedelta(seconds=1),
            maximum=dt.timedelta(seconds=30),
            factor=1.5,
        )
        self._graceful_timeout = graceful_timeout

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> Optional[bool]:
        await self.shutdown(self._graceful_timeout)

        return False

    async def shutdown(self, graceful_timeout: float = 0) -> None:
        """
        Shuts down the event sender flushing the internal buffer if necessary.

        :param graceful_timeout: flush wait period
        """

        await aio.sleep(graceful_timeout)

        if sender := self._bg_sender:
            sender.cancel()
            with cl.suppress(aio.CancelledError):
                await sender

        return None

    def append(self, event: Event) -> None:
        """
        Adds an event to the internal buffer for later sending.
        :param event: event to be added
        """

        self._buffer.put_nowait(event)

    async def flush(self) -> None:
        """
        Sends events from the internal buffer.
        """

        while not self._buffer.empty():
            await self._wrapped.send(self._buffer.get_nowait())

    async def send(self, event: Event) -> None:
        await self.flush()
        await self._wrapped.send(event)

    async def _send_buffered_events(self) -> None:
        with ErrorGuard(self._retry_policy, reset_on_success=True) as guard:
            while ticket := await guard.acquire_async():
                with ticket:
                    event = await self._buffer.get()
                    await self._wrapped.send(event)
