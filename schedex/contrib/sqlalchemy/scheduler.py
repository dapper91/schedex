import weakref as wr
from collections import deque
from types import TracebackType
from typing import Iterable, Optional, Self

import sqlalchemy as sa
import sqlalchemy.event
import sqlalchemy.ext.asyncio as aiosa
import sqlalchemy.orm

import schedex as sx

from .storage import SqlAlchemyJobManager, SqlAlchemyTransactionManager


class SessionBoundEventSender(sx.EventSender):
    def __init__(self, sa_session: aiosa.AsyncSession, event_sender_ref: wr.ref[sx.BufferedEventSender]):
        self._sa_session = sa_session
        self._event_sender_ref = event_sender_ref

    async def send(self, event: sx.Event) -> None:
        event_queue: deque[sx.Event] = self._sa_session.info.setdefault("schedex_delayed_events", deque())
        event_queue.append(event)

        if not sa.event.contains(self._sa_session.sync_session, "after_commit", self._after_commit):
            sa.event.listen(self._sa_session.sync_session, "after_commit", self._after_commit)
        if not sa.event.contains(self._sa_session.sync_session, "after_rollback", self._after_rollback):
            sa.event.listen(self._sa_session.sync_session, "after_rollback", self._after_rollback)

    def _after_commit(self, session: sa.orm.Session) -> None:
        event_queue: Optional[deque[sx.Event]] = session.info.get("schedex_delayed_events")
        if (event_sender := self._event_sender_ref()) is not None and event_queue is not None:
            while event_queue:
                event_sender.append(event_queue.popleft())

    def _after_rollback(self, session: sa.orm.Session) -> None:
        event_queue: Optional[deque[sx.Event]] = session.info.get("schedex_delayed_events")
        if event_queue is not None:
            event_queue.clear()


class SqlAlchemyScheduler[SchT: sx.Schedule](sx.Scheduler[SchT]):
    """
    SqlAlchemy job scheduler.

    :param session_maker: sqlalchemy session maker
    :param event_sender: event sender
    :param schedule_type: schedule type
    """

    def __init__(
        self,
        session_maker: aiosa.async_sessionmaker[aiosa.AsyncSession],
        schedule_type: type[SchT],
        event_sender: Optional[sx.EventSender] = None,
        middlewares: Iterable[sx.SchedulerMiddleware[SchT]] = (),
    ):
        super().__init__(SqlAlchemyJobManager(session_maker), schedule_type, event_sender, middlewares)
        self._schedule_type = schedule_type
        self._event_sender = event_sender


class SqlAlchemyTransactionalScheduler[SchT: sx.Schedule](sx.TransactionalScheduler[aiosa.AsyncSession, SchT]):
    """
    SqlAlchemy transactional job scheduler.

    :param session_maker: sqlalchemy session maker
    :param event_sender: event sender
    :param schedule_type: schedule type
    """

    def __init__(
        self,
        session_maker: aiosa.async_sessionmaker[aiosa.AsyncSession],
        schedule_type: type[SchT],
        event_sender: Optional[sx.EventSender] = None,
        middlewares: Iterable[sx.SchedulerMiddleware[SchT]] = (),
        graceful_timeout: float = 0,
    ):
        super().__init__(
            SqlAlchemyTransactionManager(session_maker),
            SqlAlchemyJobManager(session_maker),
            schedule_type,
            event_sender,
            middlewares,
        )
        # since sqlalchemy doesn't support asynchronous event handlers, buffered event sender
        # is used to send an event on transaction commit from synchronous method
        self._buffered_event_sender = (
            sx.BufferedEventSender(event_sender, background_sender=True, graceful_timeout=graceful_timeout)
            if event_sender
            else None
        )

    async def __aenter__(self) -> Self:
        if event_sender := self._buffered_event_sender:
            await event_sender.__aenter__()

        return self

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> Optional[bool]:
        if event_sender := self._buffered_event_sender:
            await event_sender.__aexit__(exc_type, exc_val, exc_tb)

        return False

    async def shutdown(self, graceful_timeout: float = 0) -> None:
        if event_sender := self._buffered_event_sender:
            await event_sender.shutdown(graceful_timeout)

        return None

    def within_transaction(self, outer_tx: aiosa.AsyncSession) -> sx.SchedulerTransaction[aiosa.AsyncSession, SchT]:
        return sx.SchedulerTransaction(
            self._transaction_manager.within_transaction(outer_tx),
            self._schedule_type,
            SessionBoundEventSender(
                outer_tx,
                # wrap event sender in ref in case of transaction outlives it
                wr.ref(self._buffered_event_sender),
            )
            if self._buffered_event_sender
            else None,
            self._middlewares,
        )
