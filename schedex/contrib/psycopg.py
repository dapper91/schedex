import base64
import contextlib as cl
import logging
from types import TracebackType
from typing import Any, AsyncGenerator, AsyncIterator, Callable, Optional, Self

import psycopg as ppg
import psycopg_pool as ppg_pool
from psycopg import sql

from schedex import Event, EventReceiver, EventSender

logger = logging.getLogger(__name__)


class PsycopgEventSender(EventSender):
    """
    Psycopg event sender.
    """

    def __init__(
        self,
        pool: ppg_pool.AsyncConnectionPool,
        channel: str = "schedex_events",
        encoder: Callable[[bytes], str] = lambda d: base64.b64encode(d).decode(),
    ):
        self._pool = pool
        self._channel = channel
        self._encoder = encoder

    async def send(self, event: Event) -> None:
        serialized = event.serialize()
        encoded = self._encoder(serialized)

        async with self._pool.connection() as conn:
            await conn.execute("SELECT pg_notify(%s, %s)", (self._channel, encoded))
            logger.debug("sent event: %s", event)


class EventSource(AsyncIterator[Event]):
    """
    Psycopg event source.

    :param conn: database connection
    :param decoder: binary data decoder
    """

    def __init__(self, conn: ppg.AsyncConnection[Any], decoder: Callable[[str], bytes]):
        self._conn = conn
        self._notifies = conn.notifies()
        self._decoder = decoder

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Optional[bool]:
        await self._notifies.aclose()
        return False

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> Event:
        try:
            notification = await anext(self._notifies)
        except StopAsyncIteration:
            # reconfigure notifies generator if it had been stopped
            self._notifies = self._conn.notifies()
            notification = await anext(self._notifies)

        decoded = self._decoder(notification.payload)
        event = Event.deserialize(decoded)
        logger.debug("received event: %s", event)

        return event


class PsycopgEventReceiver(EventReceiver):
    """
    Psycopg event receiver.
    """

    def __init__(
        self,
        pool: ppg_pool.AsyncConnectionPool,
        channel: str = "schedex_events",
        decoder: Callable[[str], bytes] = base64.b64decode,
    ):
        self._pool = pool
        self._channel = channel
        self._decoder = decoder

    @cl.asynccontextmanager
    async def connect(self) -> AsyncGenerator[EventSource, None]:
        logger.info("connecting to event source ...")

        async with self._pool.connection() as conn:
            await conn.set_autocommit(True)

            await conn.execute(sql.SQL("LISTEN {0}").format(sql.Identifier(self._channel)))
            try:
                async with EventSource(conn, self._decoder) as source:
                    logger.info("event source configured")
                    yield source

            finally:
                await conn.execute(sql.SQL("UNLISTEN {0}").format(sql.Identifier(self._channel)))

        logger.info("event source connection closed")
