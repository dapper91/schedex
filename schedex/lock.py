import abc
import contextlib as cl
import datetime as dt
import logging
from collections import deque
from types import TracebackType
from typing import Never, Optional

from schedex.eventbus import Event, EventSender
from schedex.storage import StoredJob, StoredTask

logger = logging.getLogger(__name__)


class LockFailedError(Exception):
    """
    Entity locking failed.
    """


class BreakTask(BaseException):
    """
    Breaks the task execution without updating the status.
    """


class Lock(cl.AbstractAsyncContextManager["Lock"], abc.ABC):
    """
    Scheduler common lock.
    """

    @abc.abstractmethod
    async def release(self) -> None:
        """
        Releases the lock.
        """


class JobLock(Lock, abc.ABC):
    """
    Scheduler job lock.
    """

    @property
    @abc.abstractmethod
    def job(self) -> StoredJob:
        """
        Returns locked job.
        """

    @abc.abstractmethod
    async def remove(self) -> None:
        """
        Removes the job and releases the lock.
        """

    @abc.abstractmethod
    async def release_with_error(self) -> None:
        """
        Changes the job status to Error and releases the lock.
        """

    @abc.abstractmethod
    async def create_task(self, next_run_at: Optional[dt.datetime]) -> None:
        """
        Creates a task from the job and releases the lock.
        The next job run will be scheduled at `next_run_at`.

        :param next_run_at: next job run time. If `None` the job is considered as completed.
        """


class TaskLock(Lock, abc.ABC):
    """
    Scheduler task lock.
    """

    @property
    @abc.abstractmethod
    def task(self) -> StoredTask:
        """
        Returns locked task.
        """

    @abc.abstractmethod
    async def remove(self) -> None:
        """
        Removes the task and releases the lock.
        """

    @abc.abstractmethod
    async def release_with_error(self) -> None:
        """
        Changes the task status to Error and releases the lock.
        """

    @abc.abstractmethod
    async def succeed(self) -> None:
        """
        Changes the task status to Succeeded and releases the lock.
        """

    @abc.abstractmethod
    async def postpone(self, delay: dt.timedelta) -> Never:
        """
        Postpones the task for a future execution.
        """


class LeasingTaskLock(TaskLock, abc.ABC):
    """
    Scheduler leasing task lock.
    """

    def __init__(
        self,
        period: dt.timedelta,
        locker: str,
    ):
        super().__init__()

        self._acquired_at = dt.datetime.now(dt.timezone.utc)
        self._period = period
        self._locker = locker

    @property
    def period(self) -> dt.timedelta:
        return self._period

    @property
    def locker(self) -> str:
        return self._locker

    @property
    def acquired_at(self) -> dt.datetime:
        return self._acquired_at

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Optional[bool]:
        return False

    @abc.abstractmethod
    async def is_locked(self) -> bool:
        pass

    @abc.abstractmethod
    async def remain(self) -> Optional[dt.timedelta]:
        pass

    @abc.abstractmethod
    async def extend(self, period: dt.timedelta) -> bool:
        pass


class LockManager[JlkT: JobLock, TlkT: TaskLock](cl.AbstractAsyncContextManager["LockManager[JlkT, TlkT]"], abc.ABC):
    """
    Scheduler lock manager.
    """

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Optional[bool]:
        return False

    @abc.abstractmethod
    async def lock_upcoming_job(self, locker: str) -> tuple[Optional[JlkT], dt.timedelta]:
        """
        Creates a job lock.

        :param locker: locker identifier
        :return: job lock, delay pair where job is a locked job or `None` if there is no job to lock and
                 delay indicates how much time to wait for the next try
        """

    @abc.abstractmethod
    async def lock_next_task(self, locker: str) -> tuple[Optional[TlkT], dt.timedelta]:
        """
        Creates a task lock.

        :param locker: locker identifier
        :return: task lock, delay pair where task is a locked task or `None` if there is no task to lock and
                 delay indicates how much time to wait for the next try
        """


class EventManagerMixin:
    """
    Lock event manager mix-in.

    :param event_sender: Event sender.
    """

    def __init__(self, event_sender: Optional[EventSender] = None):
        self._event_sender = event_sender
        self._events = deque[Event]()

    def _emit_event(self, event: Event, replace: bool = False) -> None:
        if self._event_sender:
            if replace:
                self._events.clear()

            self._events.append(event)

    def _clear_events(self) -> None:
        self._events.clear()

    async def _flush_events(self) -> None:
        if self._event_sender:
            while len(self._events) != 0:
                await self._event_sender.send(self._events.popleft())
