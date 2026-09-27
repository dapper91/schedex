import abc
import contextlib as cl
import datetime as dt
from collections import deque
from typing import Optional

from schedex.eventbus import Event, EventSender
from schedex.storage import StoredJob, StoredTask


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


class LockManager[JlkT: JobLock, TlkT: TaskLock](abc.ABC):
    @abc.abstractmethod
    async def lock_upcoming_job(self) -> tuple[Optional[JlkT], dt.timedelta]:
        """
        Creates a job lock.

        :return: job lock or `None` if there is no job to lock.
        """

    @abc.abstractmethod
    async def lock_next_task(self) -> tuple[Optional[TlkT], dt.timedelta]:
        """
        Creates a task lock.

        :return: task lock or `None` if there is no task to lock.
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
