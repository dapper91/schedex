import datetime as dt

import pytest
import sqlalchemy.ext.asyncio as aiosa

from schedex import JobStatus, TaskStatus
from schedex.contrib.sqlalchemy.lock import SqlAlchemySfuLockManager
from tests.sqlalchemy.factory import JobFactory, TaskFactory
from tests.sqlalchemy.tables import JobTable, TaskTable


class CustomException(Exception):
    pass


async def test_sqlalchemy_sfu_job_lock_manager_no_jobs(sa_database: aiosa.AsyncEngine):
    lock_mgr = SqlAlchemySfuLockManager(sa_database)

    lock, delay = await lock_mgr.lock_upcoming_job()
    assert lock is None


@pytest.mark.time_machine("2026-09-06 12:00 +0000", tick=False)
async def test_sqlalchemy_sfu_job_lock_manager_no_upcoming_jobs(
    sa_database: aiosa.AsyncEngine,
    job_factory: type[JobFactory],
    job_table: JobTable,
):
    now = dt.datetime.now(dt.timezone.utc)
    await job_table.insert(job_factory.build(run_at=now + dt.timedelta(microseconds=1)))

    lock_mgr = SqlAlchemySfuLockManager(sa_database)

    lock, delay = await lock_mgr.lock_upcoming_job()
    assert lock is None


@pytest.mark.time_machine("2026-09-06 12:00 +0000", tick=False)
async def test_sqlalchemy_sfu_job_lock_manager_job_order(
    sa_database: aiosa.AsyncEngine,
    job_factory: type[JobFactory],
    job_table: JobTable,
):
    now = dt.datetime.now(dt.timezone.utc)
    await job_table.insert(job_factory.build(run_at=now + dt.timedelta(microseconds=1)))
    expected_job = await job_table.insert(job_factory.build(run_at=now))

    lock_mgr = SqlAlchemySfuLockManager(sa_database)

    lock, delay = await lock_mgr.lock_upcoming_job()
    assert lock is not None
    assert lock.job.id == expected_job.id

    await lock.release()


@pytest.mark.parametrize("status", [JobStatus.Suspended, JobStatus.Error])
async def test_sqlalchemy_sfu_job_lock_manager_job_skip(
    sa_database: aiosa.AsyncEngine,
    job_factory: type[JobFactory],
    job_table: JobTable,
    status: JobStatus,
):
    await job_table.insert(job_factory.build(status=status))

    lock_mgr = SqlAlchemySfuLockManager(sa_database)
    lock, delay = await lock_mgr.lock_upcoming_job()
    assert lock is None


async def test_sqlalchemy_sfu_job_lock_release(
    sa_database: aiosa.AsyncEngine, job_factory: type[JobFactory], job_table: JobTable
):
    await job_table.insert(job_factory.build())

    lock_mgr = SqlAlchemySfuLockManager(sa_database)

    lock, delay = await lock_mgr.lock_upcoming_job()
    assert lock is not None
    assert await job_table.count(skip_locked=True) == 0

    await lock.release()
    assert await job_table.count() == 1


async def test_sqlalchemy_sfu_job_lock_context_manager_success(
    sa_database: aiosa.AsyncEngine, job_factory: type[JobFactory], job_table: JobTable
):
    job = await job_table.insert(job_factory.build())

    lock_mgr = SqlAlchemySfuLockManager(sa_database)

    lock, delay = await lock_mgr.lock_upcoming_job()
    assert lock is not None
    async with lock:
        assert await job_table.count(skip_locked=True) == 0

    job = await job_table.select(job.id)
    assert job.status is JobStatus.Active


async def test_sqlalchemy_sfu_job_lock_context_manager_exception(
    sa_database: aiosa.AsyncEngine, job_factory: type[JobFactory], job_table: JobTable
):
    job = await job_table.insert(job_factory.build())

    lock_mgr = SqlAlchemySfuLockManager(sa_database)

    lock, delay = await lock_mgr.lock_upcoming_job()
    assert lock is not None
    with pytest.raises(CustomException):
        async with lock:
            raise CustomException()

    job = await job_table.select(job.id)
    assert job.status is JobStatus.Error


async def test_sqlalchemy_sfu_job_lock_release_with_status(
    sa_database: aiosa.AsyncEngine, job_factory: type[JobFactory], job_table: JobTable
):
    job = await job_table.insert(job_factory.build())

    lock_mgr = SqlAlchemySfuLockManager(sa_database)

    lock, delay = await lock_mgr.lock_upcoming_job()
    assert lock is not None
    await lock.release_with_error()
    assert await job_table.count() == 1

    job = await job_table.select(job.id)
    assert job.status is JobStatus.Error


@pytest.mark.time_machine("2026-09-06 12:00 +0000", tick=False)
@pytest.mark.parametrize("completed", [True, False])
async def test_sqlalchemy_sfu_job_lock_create_task(
    sa_database: aiosa.AsyncEngine,
    job_factory: type[JobFactory],
    job_table: JobTable,
    task_table: TaskTable,
    completed: bool,
):
    now = dt.datetime.now(dt.timezone.utc)
    if completed:
        next_run_at = None
    else:
        next_run_at = now + dt.timedelta(seconds=1)

    job = await job_table.insert(job_factory.build())

    lock_mgr = SqlAlchemySfuLockManager(sa_database)

    lock, delay = await lock_mgr.lock_upcoming_job()
    assert lock is not None
    await lock.create_task(next_run_at=next_run_at)
    await lock.release()

    job = await job_table.select(job.id)
    if completed:
        assert job.status is JobStatus.Completed
        assert job.count == 0
        assert job.run_at == now
    else:
        assert job.status is JobStatus.Active
        assert job.count == 1
        assert job.run_at == next_run_at

    tasks = await task_table.select_all()
    assert len(tasks) == 1

    task = tasks[0]
    assert task is not None
    assert task.attempts == 0
    assert task.task_name == job.task_name
    assert task.task_args == job.task_args
    assert task.meta == job.meta
    assert task.run_at == now


async def test_sqlalchemy_sfu_task_lock_manager_no_tasks(sa_database: aiosa.AsyncEngine):
    lock_mgr = SqlAlchemySfuLockManager(sa_database)

    lock, delay = await lock_mgr.lock_next_task()
    assert lock is None


@pytest.mark.time_machine("2026-09-06 12:00 +0000", tick=False)
async def test_sqlalchemy_sfu_job_lock_manager_no_upcoming_tasks(
    sa_database: aiosa.AsyncEngine,
    task_factory: type[TaskFactory],
    task_table: TaskTable,
):
    now = dt.datetime.now(dt.timezone.utc)
    await task_table.insert(task_factory.build(run_at=now + dt.timedelta(microseconds=1)))

    lock_mgr = SqlAlchemySfuLockManager(sa_database)

    lock, delay = await lock_mgr.lock_next_task()
    assert lock is None


@pytest.mark.parametrize("status", [TaskStatus.Pending, TaskStatus.Failed])
async def test_sqlalchemy_sfu_task_lock_manager_task_order(
    sa_database: aiosa.AsyncEngine,
    task_factory: type[TaskFactory],
    task_table: TaskTable,
    status: TaskStatus,
):
    if status == TaskStatus.Pending:
        task_factory = task_factory.pending()
    elif status == TaskStatus.Failed:
        task_factory = task_factory.failed()
    else:
        raise AssertionError("unreachable")

    now = dt.datetime.now(dt.timezone.utc)
    await task_table.insert(task_factory.build(run_at=now + dt.timedelta(microseconds=1)))
    expected_task = await task_table.insert(task_factory.build(run_at=now))

    lock_mgr = SqlAlchemySfuLockManager(sa_database)

    lock, delay = await lock_mgr.lock_next_task()
    assert lock is not None
    assert lock.task.id == expected_task.id
    await lock.release()


@pytest.mark.parametrize("status", [TaskStatus.Succeeded, TaskStatus.Error])
async def test_sqlalchemy_sfu_task_lock_manager_task_skip(
    sa_database: aiosa.AsyncEngine,
    task_factory: type[TaskFactory],
    task_table: TaskTable,
    status: TaskStatus,
):
    if status == TaskStatus.Succeeded:
        task_factory = task_factory.succeeded()
    elif status == TaskStatus.Error:
        task_factory = task_factory.error()
    else:
        raise AssertionError("unreachable")

    await task_table.insert(task_factory.build())

    lock_mgr = SqlAlchemySfuLockManager(sa_database)
    lock, delay = await lock_mgr.lock_next_task()
    assert lock is None


async def test_sqlalchemy_sfu_task_lock_release(
    sa_database: aiosa.AsyncEngine, task_factory: type[TaskFactory], task_table: TaskTable
):
    await task_table.insert(task_factory.pending().build())

    lock_mgr = SqlAlchemySfuLockManager(sa_database)

    lock, delay = await lock_mgr.lock_next_task()
    assert lock is not None
    assert await task_table.count(skip_locked=True) == 0

    await lock.release()
    assert await task_table.count() == 1


async def test_sqlalchemy_sfu_task_lock_context_manager_success(
    sa_database: aiosa.AsyncEngine, task_factory: type[TaskFactory], task_table: TaskTable
):
    task = await task_table.insert(task_factory.pending().build())

    lock_mgr = SqlAlchemySfuLockManager(sa_database)

    lock, delay = await lock_mgr.lock_next_task()
    assert lock is not None
    async with lock:
        assert await task_table.count(skip_locked=True) == 0

    task = await task_table.select(task.id)
    assert task.status is TaskStatus.Succeeded


async def test_sqlalchemy_sfu_task_lock_context_manager_exception(
    sa_database: aiosa.AsyncEngine, task_factory: type[TaskFactory], task_table: TaskTable
):
    task = await task_table.insert(task_factory.pending().build())

    lock_mgr = SqlAlchemySfuLockManager(sa_database)

    lock, delay = await lock_mgr.lock_next_task()
    assert lock is not None
    with pytest.raises(CustomException):
        async with lock:
            raise CustomException()

    task = await task_table.select(task.id)
    assert task.status is TaskStatus.Error


async def test_sqlalchemy_sfu_task_lock_release_with_status(
    sa_database: aiosa.AsyncEngine, task_factory: type[TaskFactory], task_table: TaskTable
):
    task = await task_table.insert(task_factory.pending().build())

    lock_mgr = SqlAlchemySfuLockManager(sa_database)

    lock, delay = await lock_mgr.lock_next_task()
    assert lock is not None
    await lock.release_with_error()
    assert await task_table.count() == 1

    task = await task_table.select(task.id)
    assert task.status is TaskStatus.Error
