import asyncio as aio
import datetime as dt
import logging
import uuid
from types import TracebackType
from typing import Any, Optional, Self, cast

import sqlalchemy as sa
import sqlalchemy.ext.asyncio as aiosa
import sqlalchemy.orm

import schedex as sx

from . import tables

logger = logging.getLogger(__name__)


class LockFailedError(Exception):
    """
    Entity locking failed.
    """


class SqlAlchemySfuJobLock(sx.EventManagerMixin, sx.JobLock):
    """
    SqlAlchemy select-for-update job lock.
    """

    @classmethod
    async def try_lock(
        cls,
        session_maker: aiosa.async_sessionmaker[aiosa.AsyncSession],
        event_sender: Optional[sx.EventSender] = None,
    ) -> tuple[Optional[Self], dt.timedelta]:
        logger.debug("locking a job...")

        session = session_maker()
        try:
            await session.begin()

            job = await session.scalar(
                sa.select(tables.Job)
                .with_for_update(skip_locked=True)
                .where(
                    tables.Job.status == sx.JobStatus.Active,
                )
                .order_by(
                    tables.Job.run_at.asc(),
                )
                .limit(1)
            )
            if job is not None:
                if (delay := job.run_at - dt.datetime.now(tz=dt.timezone.utc)) <= dt.timedelta(0):
                    logger.debug("job %s locked", job.id)
                    return cls(session, job, event_sender), dt.timedelta(0)
                else:
                    logger.debug("no upcoming jobs found")
                    await session.close()
                    return None, delay
            else:
                logger.debug("job timeline is empty")
                await session.close()
                return None, dt.timedelta.max

        except:
            await session.close()
            raise

    def __init__(self, session: aiosa.AsyncSession, job: tables.Job, event_sender: Optional[sx.EventSender] = None):
        assert session.in_transaction(), "transaction is not started"

        super().__init__(event_sender)

        self._session = session
        self._job = job

    @property
    def job(self) -> sx.StoredJob:
        return sx.StoredJob(
            id=self._job.id,
            created_at=self._job.created_at,
            status=self._job.status,
            schedule=self._job.schedule,
            count=self._job.count,
            task_name=self._job.task_name,
            task_args=self._job.task_args,
            meta=self._job.meta,
            run_at=self._job.run_at,
        )

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Optional[bool]:
        if exc_value is None:
            await self.release()
        elif isinstance(exc_value, aio.CancelledError):
            await self.release(rollback=True)
        else:
            await self.release_with_error()

        return False

    async def release(self, rollback: bool = False) -> None:
        self._session.expunge(self._job)
        try:
            if rollback:
                await self._session.rollback()
                self._emit_event(sx.Event(sx.EventKind.JobReady), replace=True)
            else:
                await self._session.commit()
        finally:
            await self._session.close()

        await self._flush_events()
        logger.debug("job %s released", self._job.id)

    async def release_with_error(self) -> None:
        await self._session.execute(
            sa.update(tables.Job).values(status=sx.JobStatus.Error).where(tables.Job.id == self._job.id)
        )
        self._emit_event(sx.Event(sx.EventKind.JobCanceled))

        await self.release()

        logger.debug("job %s released with error", self._job.id)

    async def remove(self) -> None:
        await self._session.execute(sa.delete(tables.Job).where(tables.Job.id == self._job.id))
        self._emit_event(sx.Event(sx.EventKind.JobCompleted))

        await self.release()
        logger.debug("job %s removed", self._job.id)

    async def complete(self) -> None:
        await self._session.execute(
            sa.update(tables.Job).values(status=sx.JobStatus.Completed).where(tables.Job.id == self._job.id)
        )
        self._emit_event(sx.Event(sx.EventKind.JobCompleted))

        await self.release()
        logger.debug("job %s completed", self._job.id)

    async def create_task(self, next_run_at: Optional[dt.datetime] = None) -> None:
        logger.debug("job %s spawning a task...", self._job.id)

        if next_run_at is None:
            await self._session.execute(
                sa.update(tables.Job).values(status=sx.JobStatus.Completed).where(tables.Job.id == self._job.id)
            )
            self._emit_event(sx.Event(sx.EventKind.JobCompleted))
        else:
            await self._session.execute(
                sa.update(tables.Job)
                .values(run_at=next_run_at, count=self.job.count + 1)
                .where(tables.Job.id == self._job.id)
            )
            self._emit_event(sx.Event(sx.EventKind.JobReady))

        task_id = uuid.uuid4().hex
        self._session.add(
            tables.Task(
                id=task_id,
                created_at=dt.datetime.now(tz=dt.timezone.utc),
                status=sx.TaskStatus.Pending,
                job_id=self._job.id,
                sequence_number=self._job.count,
                task_name=self._job.task_name,
                task_args=self._job.task_args,
                meta=self._job.meta,
                run_at=dt.datetime.now(tz=dt.timezone.utc),
            )
        )
        self._emit_event(sx.Event(sx.EventKind.TaskReady))
        logger.debug("job %s spawned a task %s", self._job.id, task_id)


class SqlAlchemySfuTaskLock(sx.EventManagerMixin, sx.TaskLock):
    """
    SqlAlchemy select-for-update task lock.
    """

    @classmethod
    async def try_lock(
        cls,
        session_maker: aiosa.async_sessionmaker[aiosa.AsyncSession],
        event_sender: Optional[sx.EventSender] = None,
    ) -> tuple[Optional[Self], dt.timedelta]:
        logger.debug("locking a task...")

        session = session_maker()
        try:
            await session.begin()

            task = await session.scalar(
                sa.select(tables.Task)
                .with_for_update(skip_locked=True)
                .where(
                    tables.Task.status.in_((sx.TaskStatus.Pending, sx.TaskStatus.Failed)),
                )
                .order_by(
                    tables.Task.run_at.asc(),
                )
                .limit(1)
            )
            if task is not None:
                if (delay := task.run_at - dt.datetime.now(tz=dt.timezone.utc)) <= dt.timedelta(0):
                    logger.debug("task %s locked", task.id)
                    return cls(session, task, event_sender), dt.timedelta(0)
                else:
                    logger.debug("no upcoming tasks found")
                    await session.close()
                    return None, delay
            else:
                logger.debug("task timeline is empty")
                await session.close()
                return None, dt.timedelta.max

        except:
            await session.close()
            raise

    def __init__(self, session: aiosa.AsyncSession, task: tables.Task, event_sender: Optional[sx.EventSender] = None):
        assert session.in_transaction(), "transaction is not started"

        super().__init__(event_sender)

        self._session = session
        self._task = task

    @property
    def task(self) -> sx.StoredTask:
        return sx.StoredTask(
            id=self._task.id,
            job_id=self._task.job_id,
            sequence_number=self._task.sequence_number,
            created_at=self._task.created_at,
            status=self._task.status,
            attempts=self._task.attempts,
            task_name=self._task.task_name,
            task_args=self._task.task_args,
            meta=self._task.meta,
            run_at=self._task.run_at,
            acquired_by=self._task.acquired_by,
            acquired_until=self._task.acquired_until,
        )

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Optional[bool]:
        if exc_value is None:
            await self.succeed()
        elif isinstance(exc_value, aio.CancelledError):
            await self.release(rollback=True)
        else:
            await self.release_with_error()

        return False

    async def release(self, rollback: bool = False) -> None:
        self._session.expunge(self._task)

        try:
            if rollback:
                await self._session.rollback()
                self._emit_event(sx.Event(sx.EventKind.JobReady), replace=True)
            else:
                await self._session.commit()
        finally:
            await self._session.close()

        await self._flush_events()
        logger.debug("task %s released", self._task.id)

    async def release_with_error(self) -> None:
        await self._session.execute(
            sa.update(tables.Task)
            .values(status=sx.TaskStatus.Error, attempts=self._task.attempts + 1)
            .where(tables.Task.id == self._task.id)
        )
        self._emit_event(sx.Event(sx.EventKind.TaskFailed))

        await self.release()
        logger.debug("task %s released with error", self._task.id)

    async def succeed(self) -> None:
        await self._session.execute(
            sa.update(tables.Task).values(status=sx.TaskStatus.Succeeded).where(tables.Task.id == self._task.id)
        )
        self._emit_event(sx.Event(sx.EventKind.TaskSucceeded))

        await self.release()
        logger.debug("task %s released as succeeded", self._task.id)

    async def remove(self) -> None:
        await self._session.execute(sa.delete(tables.Task).where(tables.Task.id == self._task.id))
        self._emit_event(sx.Event(sx.EventKind.TaskSucceeded))

        await self.release()
        logger.debug("task %s removed", self._task.id)


class SqlAlchemySfuLockManager(sx.LockManager[SqlAlchemySfuJobLock, SqlAlchemySfuTaskLock]):
    """
    SqlAlchemy select-for-update lock manager.
    """

    def __init__(self, engine: aiosa.AsyncEngine, event_sender: Optional[sx.EventSender] = None):
        self._session_maker = aiosa.async_sessionmaker(engine)
        self._event_sender = event_sender

    async def lock_upcoming_job(self) -> tuple[Optional[SqlAlchemySfuJobLock], dt.timedelta]:
        return await SqlAlchemySfuJobLock.try_lock(self._session_maker, self._event_sender)

    async def lock_next_task(self) -> tuple[Optional[SqlAlchemySfuTaskLock], dt.timedelta]:
        return await SqlAlchemySfuTaskLock.try_lock(self._session_maker, self._event_sender)


class SqlAlchemyLeasingJobLock(sx.EventManagerMixin, sx.JobLock):
    """
    SqlAlchemy leasing job lock.
    """

    @classmethod
    async def try_lock(
        cls,
        session_maker: aiosa.async_sessionmaker[aiosa.AsyncSession],
        period: dt.timedelta,
        identifier: str,
        event_sender: Optional[sx.EventSender] = None,
    ) -> tuple[Optional[Self], dt.timedelta]:
        """
        Tries to lock the next job.

        :return: task lock or `None` if there is no task to lock.
        """

        logger.debug("locking a job...")

        async with session_maker() as session:
            async with session.begin():
                job = await session.scalar(
                    sa.select(
                        sa.orm.aliased(
                            tables.Job,
                            sa.union_all(
                                # select the earliest unacquired job
                                sa.select(tables.Job, tables.Job.run_at.label("check_at"))
                                .where(
                                    tables.Job.status == sx.JobStatus.Active,
                                    tables.Job.acquired_by.is_(None),
                                    tables.Job.acquired_until.is_(None),
                                )
                                .order_by(
                                    tables.Job.run_at.asc(),
                                )
                                .limit(1),
                                # select acquired job that may be released first
                                # in case the worker acquired it terminated without releasing
                                sa.select(tables.Job, tables.Job.acquired_until.label("check_at"))
                                .where(
                                    tables.Job.status == sx.JobStatus.Active,
                                    tables.Job.acquired_by.is_not(None),
                                    tables.Job.acquired_until.is_not(None),
                                )
                                .order_by(
                                    tables.Job.acquired_until.asc(),
                                )
                                .limit(1),
                            )
                            .order_by("check_at")
                            .limit(1)
                            .subquery(),
                        )
                    )
                )
                if job is not None:
                    if job.acquired_until is not None:
                        delay = job.acquired_until - dt.datetime.now(tz=dt.timezone.utc)
                    else:
                        delay = job.run_at - dt.datetime.now(tz=dt.timezone.utc)

                    if delay <= dt.timedelta(0):
                        result = cast(
                            sa.CursorResult[Any],
                            await session.execute(
                                sa.update(tables.Job)
                                .where(
                                    tables.Job.id == job.id,
                                    # check that job is not reacquired by another worker
                                    tables.Job.acquired_by == job.acquired_by,
                                    tables.Job.acquired_until == job.acquired_until,
                                )
                                .values(
                                    acquired_by=identifier,
                                    acquired_until=dt.datetime.now(tz=dt.timezone.utc) + period,
                                )
                            ),
                        )
                        if result.rowcount:
                            session.expunge(job)
                            logger.debug("job %s locked", job.id)
                            lock = cls(session_maker, job, period, identifier, event_sender)
                            return lock, dt.timedelta(0)
                        else:
                            logger.debug("job acquired by another worker")
                            return None, dt.timedelta(0)
                    else:
                        logger.debug("no upcoming jobs found")
                        return None, delay

                else:
                    logger.debug("job timeline is empty")
                    return None, dt.timedelta.max

    def __init__(
        self,
        session_maker: aiosa.async_sessionmaker[aiosa.AsyncSession],
        job: tables.Job,
        period: dt.timedelta,
        identifier: str,
        event_sender: Optional[sx.EventSender] = None,
    ):
        super().__init__(event_sender)

        self._session_maker = session_maker
        self._job = job
        self._period = period
        self._identifier = identifier

    @property
    def period(self) -> dt.timedelta:
        return self._period

    @property
    def identifier(self) -> str:
        return self._identifier

    @property
    def job(self) -> sx.StoredJob:
        return sx.StoredJob(
            id=self._job.id,
            created_at=self._job.created_at,
            status=self._job.status,
            schedule=self._job.schedule,
            count=self._job.count,
            task_name=self._job.task_name,
            task_args=self._job.task_args,
            meta=self._job.meta,
            run_at=self._job.run_at,
        )

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Optional[bool]:
        async with self._session_maker() as session:
            async with session.begin():
                if exc_value is None:
                    await self._update_and_release(session)
                elif isinstance(exc_value, aio.CancelledError):
                    await self.release()
                else:
                    await self._update_and_release(session, status=sx.JobStatus.Error)

        return False

    async def remove(self) -> None:
        async with self._session_maker() as session:
            async with session.begin():
                result = cast(
                    sa.CursorResult[Any],
                    await session.execute(
                        sa.delete(tables.Job).where(
                            tables.Job.id == self._job.id,
                            tables.Job.acquired_by == self._identifier,
                            tables.Job.acquired_until >= dt.datetime.now(tz=dt.timezone.utc),
                        )
                    ),
                )
                self._emit_event(sx.Event(sx.EventKind.JobCanceled))

        if result.rowcount:
            logger.debug("job %s removed", self._job.id)
            await self._flush_events()
        else:
            raise LockFailedError("job lock expired")

    async def release(self) -> None:
        async with self._session_maker() as session:
            async with session.begin():
                await self._update_and_release(session)
                self._emit_event(sx.Event(sx.EventKind.JobReady))

        logger.debug("job %s released", self._job.id)
        await self._flush_events()

    async def complete(self) -> None:
        async with self._session_maker() as session:
            async with session.begin():
                await self._update_and_release(session, status=sx.JobStatus.Completed)
                self._emit_event(sx.Event(sx.EventKind.JobCompleted))

        logger.debug("job %s completed", self._job.id)
        await self._flush_events()

    async def release_with_error(self) -> None:
        async with self._session_maker() as session:
            async with session.begin():
                await self._update_and_release(session, status=sx.JobStatus.Error)
                self._emit_event(sx.Event(sx.EventKind.JobCanceled))

        logger.debug("job %s released with error", self._job.id)
        await self._flush_events()

    async def create_task(self, next_run_at: Optional[dt.datetime]) -> None:
        logger.debug("job %s spawning a task...", self._job.id)

        async with self._session_maker() as session:
            async with session.begin():
                if next_run_at is None:
                    await self._update(session, status=sx.JobStatus.Completed)
                    self._emit_event(sx.Event(sx.EventKind.JobCompleted))

                else:
                    await self._update(session, run_at=next_run_at, count=self.job.count + 1)
                    self._emit_event(sx.Event(sx.EventKind.JobReady))

                task_id = uuid.uuid4().hex
                session.add(
                    tables.Task(
                        id=task_id,
                        created_at=dt.datetime.now(tz=dt.timezone.utc),
                        status=sx.TaskStatus.Pending,
                        job_id=self._job.id,
                        sequence_number=self._job.count,
                        task_name=self._job.task_name,
                        task_args=self._job.task_args,
                        meta=self._job.meta,
                        run_at=dt.datetime.now(tz=dt.timezone.utc),
                    )
                )
                await session.flush()
                self._emit_event(sx.Event(sx.EventKind.TaskReady))

        logger.debug("job %s spawned a task %s", self._job.id, task_id)
        if next_run_at is None:
            logger.debug("job %s completed", self._job.id)
        else:
            logger.debug("job %s rescheduled", self._job.id)

        await self._flush_events()

    async def _update_and_release(self, session: aiosa.AsyncSession, **value: Any) -> None:
        await self._update(session, acquired_by=None, acquired_until=None, **value)

    async def _update(self, session: aiosa.AsyncSession, **value: Any) -> None:
        result = cast(
            sa.CursorResult[Any],
            await session.execute(
                sa.update(tables.Job)
                .values(**value)
                .where(
                    tables.Job.id == self._job.id,
                    tables.Job.acquired_by == self._identifier,
                    tables.Job.acquired_until >= dt.datetime.now(tz=dt.timezone.utc),
                )
            ),
        )
        if not result.rowcount:
            raise LockFailedError("job lock expired")


class SqlAlchemyLeasingTaskLock(sx.EventManagerMixin, sx.TaskLock):
    """
    SqlAlchemy leasing task lock.
    """

    @classmethod
    async def try_lock(
        cls,
        session_maker: aiosa.async_sessionmaker[aiosa.AsyncSession],
        period: dt.timedelta,
        identifier: str,
        event_sender: Optional[sx.EventSender] = None,
    ) -> tuple[Optional[Self], dt.timedelta]:
        """
        Tries to lock the next task.

        :return: task lock or `None` if there is no task to lock.
        """

        logger.debug("locking a task...")

        async with session_maker() as session:
            async with session.begin():
                task = await session.scalar(
                    sa.select(
                        sa.orm.aliased(
                            tables.Task,
                            sa.union_all(
                                # select the earliest unacquired task
                                sa.select(tables.Task, tables.Task.run_at.label("check_at"))
                                .where(
                                    tables.Task.status.in_((sx.TaskStatus.Pending, sx.TaskStatus.Failed)),
                                    tables.Task.acquired_by.is_(None),
                                    tables.Task.acquired_until.is_(None),
                                )
                                .order_by(
                                    tables.Task.run_at.asc(),
                                )
                                .limit(1),
                                # select acquired task that may be released first
                                # in case the worker acquired it terminated without releasing
                                sa.select(tables.Task, tables.Task.acquired_until.label("check_at"))
                                .where(
                                    tables.Task.status == sx.TaskStatus.Executing,
                                    tables.Task.acquired_by.is_not(None),
                                    tables.Task.acquired_until.is_not(None),
                                )
                                .order_by(
                                    tables.Task.acquired_until.asc(),
                                )
                                .limit(1),
                            )
                            .order_by("check_at")
                            .limit(1)
                            .subquery(),
                        )
                    )
                )
                if task is not None:
                    if task.acquired_until is not None:
                        delay = task.acquired_until - dt.datetime.now(tz=dt.timezone.utc)
                    else:
                        delay = task.run_at - dt.datetime.now(tz=dt.timezone.utc)

                    if delay <= dt.timedelta(0):
                        result = cast(
                            sa.CursorResult[Any],
                            await session.execute(
                                sa.update(tables.Task)
                                .where(
                                    tables.Task.id == task.id,
                                    # check that task is not reacquired by another worker
                                    tables.Task.acquired_by == task.acquired_by,
                                    tables.Task.acquired_until == task.acquired_until,
                                )
                                .values(
                                    acquired_by=identifier,
                                    acquired_until=dt.datetime.now(tz=dt.timezone.utc) + period,
                                )
                            ),
                        )
                        if result.rowcount:
                            logger.debug("task %s locked", task.id)
                            session.expunge(task)
                            lock = cls(session_maker, task, period, identifier, event_sender)
                            return lock, dt.timedelta(0)
                        else:
                            logger.debug("task acquired by another worker")
                            return None, dt.timedelta(0)
                    else:
                        logger.debug("no upcoming tasks found")
                        return None, delay

                else:
                    logger.debug("task timeline is empty")
                    return None, dt.timedelta.max

    def __init__(
        self,
        session_maker: aiosa.async_sessionmaker[aiosa.AsyncSession],
        task: tables.Task,
        period: dt.timedelta,
        identifier: str,
        event_sender: Optional[sx.EventSender] = None,
    ):
        super().__init__(event_sender)

        self._session_maker = session_maker
        self._task = task
        self._period = period
        self._identifier = identifier

    @property
    def period(self) -> dt.timedelta:
        return self._period

    @property
    def identifier(self) -> str:
        return self._identifier

    @property
    def task(self) -> sx.StoredTask:
        return sx.StoredTask(
            id=self._task.id,
            job_id=self._task.job_id,
            sequence_number=self._task.sequence_number,
            created_at=self._task.created_at,
            status=self._task.status,
            attempts=self._task.attempts,
            task_name=self._task.task_name,
            task_args=self._task.task_args,
            meta=self._task.meta,
            run_at=self._task.run_at,
            acquired_by=self._task.acquired_by,
            acquired_until=self._task.acquired_until,
        )

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Optional[bool]:
        if exc_value is None:
            await self.succeed()
        elif isinstance(exc_value, aio.CancelledError):
            await self.release()
        else:
            await self.release_with_error()

        return False

    async def release(self) -> None:
        async with self._session_maker() as session:
            async with session.begin():
                await self._update_and_release(session)
                self._emit_event(sx.Event(sx.EventKind.TaskReady))

        logger.debug("task %s released", self._task.id)

        await self._flush_events()

    async def remove(self) -> None:
        async with self._session_maker() as session:
            async with session.begin():
                result = cast(
                    sa.CursorResult[Any],
                    await session.execute(
                        sa.delete(tables.Task).where(
                            tables.Task.id == self._task.id,
                            tables.Task.acquired_by == self._identifier,
                            tables.Task.acquired_until >= dt.datetime.now(tz=dt.timezone.utc),
                        )
                    ),
                )
                if not result.rowcount:
                    raise LockFailedError("task lock expired")

                self._emit_event(sx.Event(sx.EventKind.TaskSucceeded))

        logger.debug("task %s removed", self._task.id)

        await self._flush_events()

    async def release_with_error(self) -> None:
        async with self._session_maker() as session:
            async with session.begin():
                await self._update_and_release(session, status=sx.TaskStatus.Error, attempts=self._task.attempts + 1)
                self._emit_event(sx.Event(sx.EventKind.TaskFailed))

        logger.debug("task %s released with error", self._task.id)

        await self._flush_events()

    async def succeed(self) -> None:
        async with self._session_maker() as session:
            async with session.begin():
                await self._update_and_release(session, status=sx.TaskStatus.Succeeded)
                self._emit_event(sx.Event(sx.EventKind.TaskSucceeded))

        logger.debug("task %s released as succeeded", self._task.id)

        await self._flush_events()

    async def is_locked(self) -> bool:
        return (remain := await self.remain()) is not None and remain > dt.timedelta(0)

    async def remain(self) -> Optional[dt.timedelta]:
        async with self._session_maker() as session:
            async with session.begin():
                acquired_until = await session.scalar(
                    sa.select(tables.Task.acquired_until).where(
                        tables.Task.id == self._task.id,
                        tables.Task.acquired_by == self._identifier,
                        tables.Task.acquired_until >= dt.datetime.now(tz=dt.timezone.utc),
                    )
                )
                if acquired_until is not None:
                    return acquired_until - dt.datetime.now(tz=dt.timezone.utc)

        return None

    async def extend(self, period: dt.timedelta) -> bool:
        async with self._session_maker() as session:
            async with session.begin():
                result = cast(
                    sa.CursorResult[Any],
                    await session.execute(
                        sa.update(tables.Task)
                        .values(
                            acquired_until=dt.datetime.now(tz=dt.timezone.utc) + period,
                        )
                        .where(
                            tables.Task.id == self._task.id,
                            tables.Task.acquired_by == self._identifier,
                            tables.Task.acquired_until >= dt.datetime.now(tz=dt.timezone.utc),
                        )
                    ),
                )

        logger.debug("task %s lock extended by %s", self._task.id, period)

        return bool(result.rowcount)

    async def _update_and_release(self, session: aiosa.AsyncSession, **value: Any) -> None:
        result = cast(
            sa.CursorResult[Any],
            await session.execute(
                sa.update(tables.Task)
                .values(acquired_by=None, acquired_until=None, **value)
                .where(
                    tables.Task.id == self._task.id,
                    tables.Task.acquired_by == self._identifier,
                    tables.Task.acquired_until >= dt.datetime.now(tz=dt.timezone.utc),
                )
            ),
        )
        if not result.rowcount:
            raise LockFailedError("task lock expired")


class SqlAlchemyLeasingLockManager(sx.LockManager[SqlAlchemyLeasingJobLock, SqlAlchemyLeasingTaskLock]):
    """
    SqlAlchemy leasing lock manager.

    :param engine: SQLAlchemy engine instance.
    :param event_sender: scheduler event sender
    :param period: task lock period.
    :param identifier: lock identifier tasks will be acquired by.
    """

    def __init__(
        self,
        engine: aiosa.AsyncEngine,
        period: dt.timedelta,
        identifier: str,
        event_sender: Optional[sx.EventSender] = None,
    ):
        self._session_maker = aiosa.async_sessionmaker(engine)
        self._event_sender = event_sender
        self._period = period
        self._identifier = identifier

    async def lock_upcoming_job(self) -> tuple[Optional[SqlAlchemyLeasingJobLock], dt.timedelta]:
        return await SqlAlchemyLeasingJobLock.try_lock(
            self._session_maker, self._period, self._identifier, self._event_sender
        )

    async def lock_next_task(self) -> tuple[Optional[SqlAlchemyLeasingTaskLock], dt.timedelta]:
        return await SqlAlchemyLeasingTaskLock.try_lock(
            self._session_maker, self._period, self._identifier, self._event_sender
        )
