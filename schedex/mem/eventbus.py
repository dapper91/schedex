import asyncio as aio
import contextlib as cl
import logging
from typing import AsyncGenerator, AsyncIterator, Optional

from schedex.eventbus import Event, EventReceiver, EventSender

logger = logging.getLogger(__name__)


class InMemoryEventReceiver(EventReceiver):
    """
    In-memory event receiver.
    """

    def __init__(self, maxsize: Optional[int] = None):
        self._maxsize = maxsize
        self._queues: dict[int, aio.Queue[Event]] = {}

    def broadcast(self, event: Event) -> None:
        for queue in self._queues.values():
            try:
                queue.put_nowait(event)
            except aio.QueueFull:
                logger.warning("receiver queue is full")

    @cl.asynccontextmanager
    async def connect(self) -> AsyncGenerator[AsyncIterator[Event], None]:
        async def receiver(queue: aio.Queue[Event]) -> AsyncGenerator[Event, None]:
            while True:
                yield await queue.get()

        receiver_queue: aio.Queue[Event] = aio.Queue(maxsize=self._maxsize or 0)
        self._queues[id(receiver_queue)] = receiver_queue
        try:
            gen = receiver(receiver_queue)
            try:
                yield gen
            finally:
                await gen.aclose()
        finally:
            self._queues.pop(id(receiver_queue))


class InMemoryEventSender(EventSender):
    """
    In-memory event sender.
    """

    def __init__(self, receiver: InMemoryEventReceiver):
        self._receiver = receiver

    async def send(self, event: Event) -> None:
        self._receiver.broadcast(event)


def build(queue_maxsize: Optional[int] = None) -> tuple[InMemoryEventSender, InMemoryEventReceiver]:
    """
    Builds in-memory event sender/receiver pair.

    :param queue_maxsize: internal queue size
    :return: bound sender/receiver pair
    """

    receiver = InMemoryEventReceiver(maxsize=queue_maxsize)
    sender = InMemoryEventSender(receiver)

    return sender, receiver
