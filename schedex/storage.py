import abc
import contextlib as cl
import dataclasses as dc
import datetime as dt
import enum
from types import TracebackType
from typing import Optional, Self


class JobAlreadyExists(Exception):
    """
    Raised when a job already exists.
    """


class JobStatus(enum.IntEnum):
    """
    Job status.
    """

    # Job is active
    Active = 0
    # Job is suspended
    Suspended = 1
    # Job can't be processed (schedule or metadata deserialization failed)
    Error = 2
    # Job completed
    Completed = 3


@dc.dataclass
class StoredJob:
    """
    Stored job data.
    """

    id: str
    created_at: dt.datetime
    status: JobStatus
    schedule: bytes
    count: int
    task_name: str
    task_args: bytes
    meta: bytes
    run_at: dt.datetime


class JobManager(abc.ABC):
    """
    Job manager.
    """

    @abc.abstractmethod
    async def get_job(self, job_id: str) -> Optional[StoredJob]:
        """
        Returns a job by the identifier.

        :param job_id: job identifier.
        :return: job
        """

    @abc.abstractmethod
    async def add_job(self, job: StoredJob) -> None:
        """
        Creates a new job with provided parameters.

        :param job: job to be created
        """

    @abc.abstractmethod
    async def cancel_job(self, job_id: str) -> bool:
        """
        Removes a job by the identifier.

        :param job_id: job identifier.
        """


class TaskStatus(enum.IntEnum):
    """
    Task status.
    """

    # Task is waiting for executor.
    Pending = 0
    # Task is being executed.
    Executing = 1
    # Task finished successfully.
    Succeeded = 2
    # Task failed during execution, but may succeed after the next try.
    Failed = 3
    # Task can't be executed (task not found, task arguments or metadata deserialization failed).
    # All subsequent tries will fail too.
    Error = 4


@dc.dataclass
class StoredTask:
    """
    Stored task data.
    """

    id: str
    job_id: str
    sequence_number: int
    created_at: dt.datetime
    status: TaskStatus
    attempts: int
    task_name: str
    task_args: bytes
    meta: bytes
    run_at: dt.datetime
    acquired_by: Optional[str]
    acquired_until: Optional[dt.datetime]


class TaskManager(abc.ABC):
    """
    Task manager.
    """

    @abc.abstractmethod
    async def add_task(self, task: StoredTask) -> None:
        """
        Creates a new task with provided parameters.
        """

    @abc.abstractmethod
    async def remove_task(self, task_id: str) -> None:
        """
        Removes a task by the identifier.
        """


class Transaction[TxT](JobManager, abc.ABC):
    """
    Scheduler transaction.
    """

    @property
    @abc.abstractmethod
    def outer(self) -> TxT:
        """
        Outer transaction.
        """


class OwnedTransaction[TxT](Transaction[TxT], cl.AbstractAsyncContextManager["OwnedTransaction[TxT]"], abc.ABC):
    """
    Scheduler owned transaction.
    """

    async def __aenter__(self) -> Self:
        await self.begin()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Optional[bool]:
        await self.commit() if exc_value is None else await self.rollback()
        return False

    @abc.abstractmethod
    async def begin(self) -> None:
        """
        Begins the transaction.
        """

    @abc.abstractmethod
    async def commit(self) -> None:
        """
        Commits the transaction.
        """

    @abc.abstractmethod
    async def rollback(self) -> None:
        """
        Rollbacks the transaction.
        """


class TransactionManager[TxT](abc.ABC):
    """
    Scheduler transaction manager.
    """

    @abc.abstractmethod
    def begin_transactional(self) -> OwnedTransaction[TxT]:
        """
        Begins an owned transaction.
        """

    @abc.abstractmethod
    def within_transaction(self, outer_tx: TxT) -> Transaction[TxT]:
        """
        Returns a transaction inheriting an outer transaction.

        :param outer_tx: outer transaction to be inherited
        """
