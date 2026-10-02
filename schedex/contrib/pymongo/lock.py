import asyncio as aio
import datetime as dt
import logging
import uuid
from types import TracebackType
from typing import Any, Optional, Self, cast

import pymongo.asynchronous.client_session as pmses
import pymongo.asynchronous.collection as pmcol
import pymongo.asynchronous.mongo_client as pmcli

import schedex as sx

from . import utils
from .schema import JOBS_COLLECTION, TASKS_COLLECTION, DocumentType, Job, Task

logger = logging.getLogger(__name__)


class LockFailedError(Exception):
    """
    Entity locking failed.
    """


class PyMongoLeasingJobLock(sx.EventManagerMixin, sx.JobLock):
    """
    PyMongo leasing job lock.
    """

    @classmethod
    async def try_lock(
        cls,
        client: pmcli.AsyncMongoClient[DocumentType],
        dbname: Optional[str],
        period: dt.timedelta,
        identifier: str,
        event_sender: Optional[sx.EventSender] = None,
    ) -> tuple[Optional[Self], dt.timedelta]:
        """
        Tries to lock the next job.

        :return: task lock or `None` if there is no task to lock.
        """

        logger.debug("locking a job...")

        async with client.start_session() as session:
            jobs = cast(pmcol.AsyncCollection[Job], utils.get_collection(session, dbname, JOBS_COLLECTION))
            async with await session.start_transaction():
                closest_jobs: list[tuple[dt.timedelta, Job]] = []
                # finds the earliest unacquired job
                job = await jobs.find_one(
                    {
                        "status": sx.JobStatus.Active,
                        "acquired_by": None,
                        "acquired_until": None,
                    },
                    sort=[("run_at", 1)],
                )
                if job is not None:
                    delay = job["run_at"] - dt.datetime.now(tz=dt.timezone.utc)
                    closest_jobs.append((delay, job))

                # finds acquired job that may be released first
                # in case the worker acquired it terminated without releasing
                job = await jobs.find_one(
                    {
                        "status": sx.JobStatus.Active,
                        "acquired_by": {"$ne": None},
                        "acquired_until": {"$ne": None},
                    },
                    sort=[("acquired_until", 1)],
                )
                if job is not None:
                    assert job["acquired_until"] is not None
                    delay = job["acquired_until"] - dt.datetime.now(tz=dt.timezone.utc)
                    closest_jobs.append((delay, job))

                if (next_job := next(iter(sorted(closest_jobs, key=lambda j: j[0])), None)) is not None:
                    delay, job = next_job
                    if delay <= dt.timedelta(0):
                        result = await jobs.update_one(
                            {
                                "id": job["id"],
                                # check that job is not reacquired by another worker
                                "acquired_by": job.get("acquired_by"),
                                "acquired_until": job.get("acquired_until"),
                            },
                            {
                                "$set": {
                                    "acquired_by": identifier,
                                    "acquired_until": dt.datetime.now(tz=dt.timezone.utc) + period,
                                }
                            },
                        )
                        if result.modified_count:
                            logger.debug("job %s locked", job["id"])
                            lock = cls(client, dbname, job, period, identifier, event_sender)
                            return lock, dt.timedelta(0)
                        else:
                            # job has been reacquired by another worker
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
        client: pmcli.AsyncMongoClient[DocumentType],
        dbname: Optional[str],
        job: Job,
        period: dt.timedelta,
        identifier: str,
        event_sender: Optional[sx.EventSender] = None,
    ):
        super().__init__(event_sender)

        self._client = client
        self._dbname = dbname
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
            id=self._job["id"],
            created_at=self._job["created_at"],
            status=self._job["status"],
            schedule=self._job["schedule"],
            count=self._job["count"],
            task_name=self._job["task_name"],
            task_args=self._job["task_args"],
            meta=self._job["meta"],
            run_at=self._job["run_at"],
        )

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Optional[bool]:
        async with self._client.start_session() as session:
            async with await session.start_transaction():
                if exc_value is None:
                    await self._update_and_release(session)
                elif isinstance(exc_value, aio.CancelledError):
                    await self.release()
                else:
                    await self._update_and_release(session, status=sx.JobStatus.Error)

        return False

    async def remove(self) -> None:
        async with self._client.start_session() as session:
            jobs = cast(pmcol.AsyncCollection[Job], utils.get_collection(session, self._dbname, JOBS_COLLECTION))

            async with await session.start_transaction():
                result = await jobs.delete_one(
                    {
                        "id": self._job["id"],
                        "acquired_by": self._identifier,
                        "acquired_until": {"$gte": dt.datetime.now(tz=dt.timezone.utc)},
                    },
                    session=session,
                )
                self._emit_event(sx.Event(sx.EventKind.JobCanceled))

        if result.deleted_count:
            logger.debug("job %s removed", self._job["id"])
            await self._flush_events()
        else:
            raise LockFailedError("job lock expired")

    async def release(self) -> None:
        async with self._client.start_session() as session:
            async with await session.start_transaction():
                await self._update_and_release(session)
                self._emit_event(sx.Event(sx.EventKind.JobReady))

        logger.debug("job %s released", self._job["id"])
        await self._flush_events()

    async def complete(self) -> None:
        async with self._client.start_session() as session:
            async with await session.start_transaction():
                await self._update_and_release(session, status=sx.JobStatus.Completed)
                self._emit_event(sx.Event(sx.EventKind.JobCompleted))

        logger.debug("job %s completed", self._job["id"])
        await self._flush_events()

    async def release_with_error(self) -> None:
        async with self._client.start_session() as session:
            async with await session.start_transaction():
                await self._update_and_release(session, status=sx.JobStatus.Error)
                self._emit_event(sx.Event(sx.EventKind.JobCanceled))

        logger.debug("job %s released with error", self._job["id"])
        await self._flush_events()

    async def create_task(self, next_run_at: Optional[dt.datetime]) -> None:
        logger.debug("job %s spawning a task...", self._job["id"])

        async with self._client.start_session() as session:
            async with await session.start_transaction():
                if next_run_at is None:
                    await self._update(session, status=sx.JobStatus.Completed)
                    self._emit_event(sx.Event(sx.EventKind.JobCompleted))

                else:
                    await self._update(session, run_at=next_run_at, count=self.job.count + 1)
                    self._emit_event(sx.Event(sx.EventKind.JobReady))

                tasks = cast(pmcol.AsyncCollection[Task], utils.get_collection(session, self._dbname, TASKS_COLLECTION))
                task_id = uuid.uuid4().hex
                await tasks.insert_one(
                    Task(
                        id=task_id,
                        created_at=dt.datetime.now(tz=dt.timezone.utc),
                        status=sx.TaskStatus.Pending,
                        job_id=self._job["id"],
                        sequence_number=self._job["count"],
                        task_name=self._job["task_name"],
                        task_args=self._job["task_args"],
                        meta=self._job["meta"],
                        run_at=dt.datetime.now(tz=dt.timezone.utc),
                        attempts=0,
                    )
                )
                self._emit_event(sx.Event(sx.EventKind.TaskReady))

        logger.debug("job %s spawned a task %s", self._job["id"], task_id)
        if next_run_at is None:
            logger.debug("job %s completed", self._job["id"])
        else:
            logger.debug("job %s rescheduled", self._job["id"])

        await self._flush_events()

    async def _update_and_release(self, session: pmses.AsyncClientSession, **value: Any) -> None:
        await self._update(session, acquired_by=None, acquired_until=None, **value)

    async def _update(self, session: pmses.AsyncClientSession, **value: Any) -> None:
        jobs = cast(pmcol.AsyncCollection[Job], utils.get_collection(session, self._dbname, JOBS_COLLECTION))

        result = await jobs.update_one(
            {
                "id": self._job["id"],
                "acquired_by": self._identifier,
                "acquired_until": {"$gte": dt.datetime.now(tz=dt.timezone.utc)},
            },
            {"$set": value},
            session=session,
        )
        if not result.modified_count:
            raise LockFailedError("job lock expired")


class PyMongoLeasingTaskLock(sx.EventManagerMixin, sx.TaskLock):
    """
    PyMongo leasing task lock.
    """

    @classmethod
    async def try_lock(
        cls,
        client: pmcli.AsyncMongoClient[DocumentType],
        dbname: Optional[str],
        period: dt.timedelta,
        identifier: str,
        event_sender: Optional[sx.EventSender] = None,
    ) -> tuple[Optional[Self], dt.timedelta]:
        """
        Tries to lock the next task.

        :return: task lock or `None` if there is no task to lock.
        """

        logger.debug("locking a task...")

        async with client.start_session() as session:
            tasks = cast(pmcol.AsyncCollection[Task], utils.get_collection(session, dbname, TASKS_COLLECTION))

            async with await session.start_transaction():
                closest_tasks: list[tuple[dt.timedelta, Task]] = []
                # finds the earliest unacquired task
                task = await tasks.find_one(
                    {
                        "status": {"$in": [sx.TaskStatus.Pending, sx.TaskStatus.Failed]},
                        "acquired_by": None,
                        "acquired_until": None,
                    },
                    sort=[("run_at", 1)],
                )
                if task is not None:
                    delay = task["run_at"] - dt.datetime.now(tz=dt.timezone.utc)
                    closest_tasks.append((delay, task))

                # finds acquired task that may be released first
                # in case the worker acquired it terminated without releasing
                task = await tasks.find_one(
                    {
                        "status": sx.TaskStatus.Executing,
                        "acquired_by": {"$ne": None},
                        "acquired_until": {"$ne": None},
                    },
                    sort=[("acquired_until", 1)],
                )
                if task is not None:
                    assert task["acquired_until"] is not None
                    delay = task["acquired_until"] - dt.datetime.now(tz=dt.timezone.utc)
                    closest_tasks.append((delay, task))

                if (next_task := next(iter(sorted(closest_tasks, key=lambda j: j[0])), None)) is not None:
                    delay, task = next_task
                    if delay <= dt.timedelta(0):
                        result = await tasks.update_one(
                            {
                                "id": task["id"],
                                # check that job is not reacquired by another worker
                                "acquired_by": task.get("acquired_by"),
                                "acquired_until": task.get("acquired_until"),
                            },
                            {
                                "$set": {
                                    "acquired_by": identifier,
                                    "acquired_until": dt.datetime.now(tz=dt.timezone.utc) + period,
                                }
                            },
                        )
                        if result.modified_count:
                            logger.debug("task %s locked", task["id"])
                            lock = cls(client, dbname, task, period, identifier, event_sender)
                            return lock, dt.timedelta(0)
                        else:
                            logger.debug("task acquired by another worker")
                            # task has been reacquired by another worker
                            return None, dt.timedelta(0)
                    else:
                        logger.debug("no upcoming tasks found")
                        return None, delay

                else:
                    logger.debug("task timeline is empty")
                    return None, dt.timedelta.max

    def __init__(
        self,
        client: pmcli.AsyncMongoClient[DocumentType],
        dbname: Optional[str],
        task: Task,
        period: dt.timedelta,
        identifier: str,
        event_sender: Optional[sx.EventSender] = None,
    ):
        super().__init__(event_sender)

        self._client = client
        self._dbname = dbname
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
            id=self._task["id"],
            job_id=self._task["job_id"],
            sequence_number=self._task["sequence_number"],
            created_at=self._task["created_at"],
            status=self._task["status"],
            attempts=self._task["attempts"],
            task_name=self._task["task_name"],
            task_args=self._task["task_args"],
            meta=self._task["meta"],
            run_at=self._task["run_at"],
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
        async with self._client.start_session() as session:
            async with await session.start_transaction():
                await self._update_and_release(session)
                self._emit_event(sx.Event(sx.EventKind.TaskReady))

        logger.debug("task %s released", self._task["id"])

        await self._flush_events()

    async def remove(self) -> None:
        async with self._client.start_session() as session:
            tasks = cast(pmcol.AsyncCollection[Task], utils.get_collection(session, self._dbname, TASKS_COLLECTION))

            async with await session.start_transaction():
                result = await tasks.delete_one(
                    {
                        "id": self._task["id"],
                        "acquired_by": self._identifier,
                        "acquired_until": {"$gte": dt.datetime.now(tz=dt.timezone.utc)},
                    },
                    session=session,
                )
                self._emit_event(sx.Event(sx.EventKind.JobCanceled))
                if not result.deleted_count:
                    raise LockFailedError("task lock expired")

            self._emit_event(sx.Event(sx.EventKind.TaskSucceeded))

        logger.debug("task %s removed", self._task["id"])

        await self._flush_events()

    async def release_with_error(self) -> None:
        async with self._client.start_session() as session:
            async with await session.start_transaction():
                await self._update_and_release(session, status=sx.TaskStatus.Error, attempts=self._task["attempts"] + 1)
                self._emit_event(sx.Event(sx.EventKind.TaskFailed))

        logger.debug("task %s released with error", self._task["id"])

        await self._flush_events()

    async def succeed(self) -> None:
        async with self._client.start_session() as session:
            async with await session.start_transaction():
                await self._update_and_release(session, status=sx.TaskStatus.Succeeded)
                self._emit_event(sx.Event(sx.EventKind.TaskSucceeded))

        logger.debug("task %s released as succeeded", self._task["id"])

        await self._flush_events()

    # async def postpone(self, delay: dt.timedelta) -> None:
    #     async with self._client.start_session() as session:
    #         async with await session.start_transaction():
    #             await self._update_and_release(
    #                 session,
    #                 status=sx.TaskStatus.Failed,
    #                 attempts=self._task.attempts + 1,
    #                 run_at=dt.datetime.now(tz=dt.timezone.utc) + delay,
    #             )
    #             self._emit_event(sx.Event(sx.EventKind.TaskFailed))
    #
    #     await self._flush_events()

    async def is_locked(self) -> bool:
        return (remain := await self.remain()) is not None and remain > dt.timedelta(0)

    async def remain(self) -> Optional[dt.timedelta]:
        async with self._client.start_session() as session:
            tasks = cast(pmcol.AsyncCollection[Task], utils.get_collection(session, self._dbname, TASKS_COLLECTION))

            async with await session.start_transaction():
                task = await tasks.find_one(
                    {
                        "id": self._task["id"],
                        "acquired_by": self._identifier,
                        "acquired_until": {"$gte": dt.datetime.now(tz=dt.timezone.utc)},
                    }
                )
                if task and (acquired_until := task["acquired_until"]) is not None:
                    return acquired_until - dt.datetime.now(tz=dt.timezone.utc)

        return None

    async def extend(self, period: dt.timedelta) -> bool:
        async with self._client.start_session() as session:
            tasks = cast(pmcol.AsyncCollection[Task], utils.get_collection(session, self._dbname, TASKS_COLLECTION))

            async with await session.start_transaction():
                result = await tasks.update_one(
                    {
                        "id": self._task["id"],
                        "acquired_by": self._identifier,
                        "acquired_until": {"$gte": dt.datetime.now(tz=dt.timezone.utc)},
                    },
                    {
                        "$set": {
                            "acquired_until": dt.datetime.now(tz=dt.timezone.utc) + period,
                        }
                    },
                )

        logger.debug("task %s lock extended by %s", self._task["id"], period)

        return bool(result.modified_count)

    async def _update_and_release(self, session: pmses.AsyncClientSession, **value: Any) -> None:
        tasks = cast(pmcol.AsyncCollection[Task], utils.get_collection(session, self._dbname, TASKS_COLLECTION))
        result = await tasks.update_one(
            {
                "id": self._task["id"],
                "acquired_by": self._identifier,
                "acquired_until": {"$gte": dt.datetime.now(tz=dt.timezone.utc)},
            },
            {"$set": {"acquired_by": None, "acquired_until": None, **value}},
            session=session,
        )
        if not result.modified_count:
            raise LockFailedError("task lock expired")


class PyMongoLeasingLockManager(sx.LockManager[PyMongoLeasingJobLock, PyMongoLeasingTaskLock]):
    """
    PyMongo leasing lock manager.

    :param client: PyMongo client
    :param dbname: database name
    :param event_sender: scheduler event sender
    :param period: task lock period.
    :param identifier: lock identifier tasks will be acquired by.
    """

    def __init__(
        self,
        client: pmcli.AsyncMongoClient[DocumentType],
        dbname: Optional[str],
        period: dt.timedelta,
        identifier: str,
        event_sender: Optional[sx.EventSender] = None,
    ):
        self._client = client
        self._dbname = dbname
        self._event_sender = event_sender
        self._period = period
        self._identifier = identifier

    async def lock_upcoming_job(self) -> tuple[Optional[PyMongoLeasingJobLock], dt.timedelta]:
        return await PyMongoLeasingJobLock.try_lock(
            self._client, self._dbname, self._period, self._identifier, self._event_sender
        )

    async def lock_next_task(self) -> tuple[Optional[PyMongoLeasingTaskLock], dt.timedelta]:
        return await PyMongoLeasingTaskLock.try_lock(
            self._client, self._dbname, self._period, self._identifier, self._event_sender
        )
