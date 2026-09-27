import asyncio as aio
import datetime as dt
import logging
from typing import AsyncIterator, Optional

from schedex.lock import JobLock, LockManager, TaskLock
from schedex.schedule import Schedule

logger = logging.getLogger(__name__)


class TaskFetcher[SchT: Schedule, JlkT: JobLock, TlkT: TaskLock]:
    """
    Scheduler task fetcher.

    :param polling_interval: task polling interval
    :param lock_manager: lock manager
    :param schedule_type: schedule type
    """

    def __init__(
        self,
        polling_interval: dt.timedelta,
        lock_manager: LockManager[JlkT, TlkT],
        schedule_type: type[SchT],
    ):
        self._polling_interval = polling_interval
        self._schedule_type = schedule_type
        self._lock_manager = lock_manager

    def __aiter__(self) -> "TaskStream[SchT, JlkT, TlkT]":
        return TaskStream(self)

    @property
    def polling_interval(self) -> dt.timedelta:
        return self._polling_interval

    async def fetch_next_task(self) -> tuple[Optional[TlkT], dt.timedelta]:
        """
        Waits for an upcoming task.

        :return: locked task
        """

        logger.debug("looking for upcoming task...")

        task_lock, next_task_delay = await self._lock_manager.lock_next_task()
        if task_lock is not None:
            logger.debug("task %s locked", task_lock.task.id)
            return task_lock, dt.timedelta(0)

        logger.debug("processing jobs...")
        job_lock, next_job_delay = await self._lock_manager.lock_upcoming_job()
        if job_lock is not None:
            logger.debug("job %s locked", job_lock.job.id)
            await self._process_job(job_lock)
            return None, dt.timedelta(0)
        else:
            return None, min(next_task_delay, next_job_delay)

    async def _process_job(self, job_lock: JobLock) -> None:
        async with job_lock:
            now = dt.datetime.now(dt.timezone.utc)
            schedule = self._schedule_type.deserialize(job_lock.job.schedule)
            next_job_run = schedule.next_run(now=now, prev=job_lock.job.run_at, count=job_lock.job.count + 1)

            await job_lock.create_task(next_run_at=next_job_run)
            logger.debug("job %s spawned a task", job_lock.job.id)


class TaskStream[SchT: Schedule, JlkT: JobLock, TlkT: TaskLock](AsyncIterator[TlkT]):
    """
    Scheduled task stream.

    :param task_fetcher: task fetcher
    """

    def __init__(self, task_fetcher: TaskFetcher[SchT, JlkT, TlkT]):
        self._task_fetcher = task_fetcher

    async def __anext__(self) -> TlkT:
        while True:
            task_lock, delay = await self._task_fetcher.fetch_next_task()
            if task_lock is None:
                await aio.sleep(min(delay, self._task_fetcher.polling_interval).total_seconds())
            else:
                return task_lock
