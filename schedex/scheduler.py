import dataclasses as dc
import datetime as dt
import functools as ft
import logging
import uuid
from types import TracebackType
from typing import Iterable, Optional, Protocol, Self

from .eventbus import Event, EventKind, EventSender
from .metadata import Metadata, metadata_decoder, metadata_encoder
from .schedule import Schedule
from .storage import JobManager, JobStatus, OwnedTransaction, StoredJob, Transaction, TransactionManager
from .task import BoundTask

logger = logging.getLogger(__name__)


@dc.dataclass
class JobData[SchT: Schedule]:
    """
    Scheduler job data.

    :param id: job identifier
    :param schedule: job schedule
    :param meta: job metadata
    """

    schedule: SchT
    meta: Metadata = dc.field(default_factory=dict, kw_only=True)
    id: str = dc.field(default_factory=lambda: str(uuid.uuid4()), kw_only=True)


@dc.dataclass
class Job[SchT: Schedule](JobData[SchT]):
    """
    Scheduler job.

    :param id: job identifier
    :param schedule: job schedule
    :param task: job task
    :param meta: job metadata
    """

    task: BoundTask


class SchedulerMiddlewareWrappedFunc[SchT: Schedule](Protocol):
    """
    Scheduler middleware wrapped function.
    """

    async def __call__(self, job: Job[SchT], /) -> Optional[str]:
        pass


class SchedulerMiddleware[SchT: Schedule](Protocol):
    """
    Scheduler middleware.
    """

    async def __call__(self, job: Job[SchT], /, *, wrapped: SchedulerMiddlewareWrappedFunc[SchT]) -> Optional[str]:
        """
        Calls the middleware.

        :param job: job is being scheduled
        :param wrapped: function the middleware is wrapping
        :return: wrapped function result
        """


class Scheduler[SchT: Schedule]:
    """
    Job scheduler.

    :param job_manager: job manager
    :param event_sender: event sender
    :param schedule_type: schedule type
    :param middlewares: scheduler middlewares
    """

    def __init__(
        self,
        job_manager: JobManager,
        schedule_type: type[SchT],
        event_sender: Optional[EventSender] = None,
        middlewares: Iterable[SchedulerMiddleware[SchT]] = (),
    ):
        self._job_manager = job_manager
        self._schedule_type = schedule_type
        self._event_sender = event_sender
        self._middlewares = middlewares

        wrapped_add_job = self._add_job
        for middleware in reversed(list(middlewares)):
            wrapped_add_job = ft.partial(middleware, wrapped=wrapped_add_job)

        self._wrapped_add_job = wrapped_add_job

    async def add_job(self, job: Job[SchT], /) -> Optional[str]:
        """
        Schedules a new job.

        :param job: job to be scheduled
        """

        return await self._wrapped_add_job(job)

    async def _add_job(self, job: Job[SchT], /) -> Optional[str]:
        logger.debug(f"scheduling job '{job.id}'")

        now = dt.datetime.now(tz=dt.timezone.utc)

        if (run_at := job.schedule.next_run(now=now, prev=None, count=0)) is not None:
            job = StoredJob(
                id=job.id or uuid.uuid4().hex,
                status=JobStatus.Active,
                created_at=now,
                run_at=run_at,
                count=0,
                schedule=job.schedule.serialize(),
                task_name=job.task.name,
                task_args=job.task.serialize(),
                meta=metadata_encoder.encode(job.meta),
            )
            await self._job_manager.add_job(job)
            await self._send_event(EventKind.JobReady)

            return job.id

        return None

    async def cancel_job(self, job_id: str) -> bool:
        """
        Cancels a job by identifier.

        :param job_id: job identifier
        """

        logger.debug(f"cancelling job '{job_id}'")

        if cancelled := await self._job_manager.cancel_job(job_id):
            await self._send_event(EventKind.JobCanceled)

        return cancelled

    async def get_job(self, job_id: str) -> Optional[JobData[SchT]]:
        """
        Returns a job by identifier.

        :param job_id: job identifier
        """

        if (stored_job := await self._job_manager.get_job(job_id)) is not None:
            return JobData(
                id=stored_job.id,
                schedule=self._schedule_type.deserialize(stored_job.schedule),
                meta=metadata_decoder.decode(stored_job.meta),
            )

        return None

    async def _send_event(self, kind: EventKind) -> None:
        if self._event_sender:
            event = Event(kind, timestamp=dt.datetime.now(tz=dt.timezone.utc))
            logger.debug(f"emitting event {event}")
            await self._event_sender.send(event)


class SchedulerTransaction[TxT, SchT: Schedule](Scheduler[SchT]):
    """
    Scheduler transaction.

    :param transaction: transaction
    :param schedule_type: schedule type
    :param event_sender: event sender
    :param middlewares: scheduler middlewares
    """

    def __init__(
        self,
        transaction: Transaction[TxT],
        schedule_type: type[SchT],
        event_sender: Optional[EventSender] = None,
        middlewares: Iterable[SchedulerMiddleware[SchT]] = (),
    ):
        super().__init__(transaction, schedule_type, event_sender, middlewares)


class OwnedSchedulerTransaction[TxT, SchT: Schedule](SchedulerTransaction[TxT, SchT]):
    """
    Scheduler owned transaction.

    :param owned_transaction: transaction
    :param schedule_type: schedule type
    :param event_sender: event sender
    :param middlewares: scheduler middlewares
    """

    def __init__(
        self,
        owned_transaction: OwnedTransaction[TxT],
        schedule_type: type[SchT],
        event_sender: Optional[EventSender] = None,
        middlewares: Iterable[SchedulerMiddleware[SchT]] = (),
    ):
        super().__init__(owned_transaction, schedule_type, event_sender, middlewares)
        self._owned_transaction = owned_transaction

    async def __aenter__(self) -> Self:
        await self.begin()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> bool:
        if exc_val is None:
            await self.commit()
        else:
            await self.rollback()

        return False

    async def begin(self) -> None:
        """
        Begins a transaction.
        """

        await self._owned_transaction.begin()

    async def commit(self) -> None:
        """
        Commits a transaction.
        """

        await self._owned_transaction.commit()
        await self._send_event(EventKind.JobReady)

    async def rollback(self) -> None:
        """
        Rollbacks a transaction.
        """

        await self._owned_transaction.rollback()


class TransactionalScheduler[TxT, SchT: Schedule](Scheduler[SchT]):
    """
    Transactional job scheduler.

    :param transaction_manager: transaction manager
    :param job_manager: job manager
    :param schedule_type: schedule type
    :param event_sender: event sender
    :param middlewares: scheduler middlewares
    """

    def __init__(
        self,
        transaction_manager: TransactionManager[TxT],
        job_manager: JobManager,
        schedule_type: type[SchT],
        event_sender: Optional[EventSender] = None,
        middlewares: Iterable[SchedulerMiddleware[SchT]] = (),
    ):
        super().__init__(job_manager, schedule_type, event_sender, middlewares)
        self._transaction_manager = transaction_manager

    def transactional(self) -> OwnedSchedulerTransaction[TxT, SchT]:
        """
        Begins a scheduler transaction.
        """

        return OwnedSchedulerTransaction(
            self._transaction_manager.begin_transactional(),
            self._schedule_type,
            self._event_sender,
            self._middlewares,
        )

    def within_transaction(self, outer_tx: TxT) -> SchedulerTransaction[TxT, SchT]:
        """
        Returns a scheduler transaction inheriting an outer transaction.

        :param outer_tx: outer transaction to be inherited
        """

        return SchedulerTransaction(
            self._transaction_manager.within_transaction(outer_tx),
            self._schedule_type,
            # event sender is disabled since event must be sent after transaction commit
            # but outer transaction is commited or rolled back by caller afterward
            None,
            self._middlewares,
        )
