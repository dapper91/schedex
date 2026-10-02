import dataclasses as dc
import datetime as dt

import pytest
import sqlalchemy.ext.asyncio as aiosa

import schedex as sx
from schedex.contrib.sqlalchemy.lock import SqlAlchemySfuLockManager
from tests.factories import StoredJobFactory, StoredTaskFactory
from tests.sqlalchemy.storage import JobSqlAlchemyStorage, TaskSqlAlchemyStorage


class CustomException(Exception):
    pass


async def test_sqlalchemy_sfu_job_lock_manager_no_jobs(sa_db_engine: aiosa.AsyncEngine) -> None:
    lock_mgr = SqlAlchemySfuLockManager(sa_db_engine)

    lock, delay = await lock_mgr.lock_upcoming_job()
    assert lock is None


@pytest.mark.time_machine("2026-09-06 12:00 +0000", tick=False)
async def test_sqlalchemy_sfu_job_lock_manager_no_upcoming_jobs(
    stored_job_factory: type[StoredJobFactory],
    sa_db_engine: aiosa.AsyncEngine,
    sa_job_storage: JobSqlAlchemyStorage,
) -> None:
    now = dt.datetime.now(dt.timezone.utc)
    stored_job = stored_job_factory.build(run_at=now + dt.timedelta(microseconds=1))
    await sa_job_storage.insert(dc.asdict(stored_job))

    lock_mgr = SqlAlchemySfuLockManager(sa_db_engine)

    lock, delay = await lock_mgr.lock_upcoming_job()
    assert lock is None


@pytest.mark.time_machine("2026-09-06 12:00 +0000", tick=False)
async def test_sqlalchemy_sfu_job_lock_manager_job_order(
    stored_job_factory: type[StoredJobFactory],
    sa_db_engine: aiosa.AsyncEngine,
    sa_job_storage: JobSqlAlchemyStorage,
) -> None:
    now = dt.datetime.now(dt.timezone.utc)
    stored_job = stored_job_factory.build(run_at=now + dt.timedelta(microseconds=1))
    await sa_job_storage.insert(dc.asdict(stored_job))
    expected_stored_job = stored_job_factory.build(run_at=now)
    await sa_job_storage.insert(dc.asdict(expected_stored_job))

    lock_mgr = SqlAlchemySfuLockManager(sa_db_engine)

    lock, delay = await lock_mgr.lock_upcoming_job()
    assert lock is not None
    assert lock.job.id == expected_stored_job.id

    await lock.release()


@pytest.mark.parametrize("status", [sx.JobStatus.Suspended, sx.JobStatus.Error])
async def test_sqlalchemy_sfu_job_lock_manager_job_skip(
    stored_job_factory: type[StoredJobFactory],
    sa_db_engine: aiosa.AsyncEngine,
    sa_job_storage: JobSqlAlchemyStorage,
    status: sx.JobStatus,
) -> None:
    stored_job = stored_job_factory.build(status=status)
    await sa_job_storage.insert(dc.asdict(stored_job))

    lock_mgr = SqlAlchemySfuLockManager(sa_db_engine)
    lock, delay = await lock_mgr.lock_upcoming_job()
    assert lock is None


async def test_sqlalchemy_sfu_job_lock_release(
    stored_job_factory: type[StoredJobFactory],
    sa_db_engine: aiosa.AsyncEngine,
    sa_job_storage: JobSqlAlchemyStorage,
) -> None:
    stored_job = stored_job_factory.build()
    await sa_job_storage.insert(dc.asdict(stored_job))

    lock_mgr = SqlAlchemySfuLockManager(sa_db_engine)

    lock, delay = await lock_mgr.lock_upcoming_job()
    assert lock is not None
    await sa_job_storage.assert_has_exactly({}, 0)

    await lock.release()
    await sa_job_storage.assert_has_exactly(
        dict(
            id=stored_job.id,
            created_at=stored_job.created_at,
            status=stored_job.status,
            schedule=stored_job.schedule,
            count=stored_job.count,
            task_name=stored_job.task_name,
            task_args=stored_job.task_args,
            meta=stored_job.meta,
            run_at=stored_job.run_at,
            acquired_by=None,
            acquired_until=None,
        ),
        1,
    )


async def test_sqlalchemy_sfu_job_lock_context_manager_success(
    stored_job_factory: type[StoredJobFactory],
    sa_db_engine: aiosa.AsyncEngine,
    sa_job_storage: JobSqlAlchemyStorage,
) -> None:
    stored_job = stored_job_factory.build()
    await sa_job_storage.insert(dc.asdict(stored_job))

    lock_mgr = SqlAlchemySfuLockManager(sa_db_engine)

    lock, delay = await lock_mgr.lock_upcoming_job()
    assert lock is not None
    async with lock:
        await sa_job_storage.assert_has_exactly({}, 0)

    await sa_job_storage.assert_has_exactly(
        dict(
            id=stored_job.id,
            created_at=stored_job.created_at,
            status=stored_job.status,
            schedule=stored_job.schedule,
            count=stored_job.count,
            task_name=stored_job.task_name,
            task_args=stored_job.task_args,
            meta=stored_job.meta,
            run_at=stored_job.run_at,
            acquired_by=None,
            acquired_until=None,
        ),
        1,
    )


async def test_sqlalchemy_sfu_job_lock_context_manager_exception(
    stored_job_factory: type[StoredJobFactory],
    sa_db_engine: aiosa.AsyncEngine,
    sa_job_storage: JobSqlAlchemyStorage,
) -> None:
    stored_job = stored_job_factory.build()
    await sa_job_storage.insert(dc.asdict(stored_job))

    lock_mgr = SqlAlchemySfuLockManager(sa_db_engine)

    lock, delay = await lock_mgr.lock_upcoming_job()
    assert lock is not None
    with pytest.raises(CustomException):
        async with lock:
            raise CustomException()

    await sa_job_storage.assert_has_exactly(
        dict(
            id=stored_job.id,
            created_at=stored_job.created_at,
            status=sx.JobStatus.Error,
            schedule=stored_job.schedule,
            count=stored_job.count,
            task_name=stored_job.task_name,
            task_args=stored_job.task_args,
            meta=stored_job.meta,
            run_at=stored_job.run_at,
            acquired_by=None,
            acquired_until=None,
        ),
        1,
    )


async def test_sqlalchemy_sfu_job_lock_release_with_status(
    stored_job_factory: type[StoredJobFactory],
    sa_db_engine: aiosa.AsyncEngine,
    sa_job_storage: JobSqlAlchemyStorage,
) -> None:
    stored_job = stored_job_factory.build()
    await sa_job_storage.insert(dc.asdict(stored_job))

    lock_mgr = SqlAlchemySfuLockManager(sa_db_engine)

    lock, delay = await lock_mgr.lock_upcoming_job()
    assert lock is not None
    await lock.release_with_error()

    await sa_job_storage.assert_has_exactly(
        dict(
            id=stored_job.id,
            created_at=stored_job.created_at,
            status=sx.JobStatus.Error,
            schedule=stored_job.schedule,
            count=stored_job.count,
            task_name=stored_job.task_name,
            task_args=stored_job.task_args,
            meta=stored_job.meta,
            run_at=stored_job.run_at,
            acquired_by=None,
            acquired_until=None,
        ),
        1,
    )


@pytest.mark.time_machine("2026-09-06 12:00 +0000", tick=False)
@pytest.mark.parametrize("completed", [True, False])
async def test_sqlalchemy_sfu_job_lock_create_task(
    stored_job_factory: type[StoredJobFactory],
    sa_db_engine: aiosa.AsyncEngine,
    sa_job_storage: JobSqlAlchemyStorage,
    sa_task_storage: TaskSqlAlchemyStorage,
    completed: bool,
) -> None:
    now = dt.datetime.now(dt.timezone.utc)
    if completed:
        next_run_at = None
    else:
        next_run_at = now + dt.timedelta(seconds=1)

    stored_job = stored_job_factory.build()
    await sa_job_storage.insert(dc.asdict(stored_job))

    lock_mgr = SqlAlchemySfuLockManager(sa_db_engine)

    lock, delay = await lock_mgr.lock_upcoming_job()
    assert lock is not None
    await lock.create_task(next_run_at=next_run_at)
    await lock.release()

    if completed:
        await sa_job_storage.assert_has_exactly(
            dict(
                id=stored_job.id,
                created_at=stored_job.created_at,
                status=sx.JobStatus.Completed,
                schedule=stored_job.schedule,
                count=0,
                task_name=stored_job.task_name,
                task_args=stored_job.task_args,
                meta=stored_job.meta,
                run_at=now,
                acquired_by=None,
                acquired_until=None,
            ),
            1,
        )
    else:
        await sa_job_storage.assert_has_exactly(
            dict(
                id=stored_job.id,
                created_at=stored_job.created_at,
                status=sx.JobStatus.Active,
                schedule=stored_job.schedule,
                count=1,
                task_name=stored_job.task_name,
                task_args=stored_job.task_args,
                meta=stored_job.meta,
                run_at=next_run_at,
                acquired_by=None,
                acquired_until=None,
            ),
            1,
        )

    await sa_task_storage.assert_has_exactly({}, 1)
    await sa_task_storage.assert_has_exactly(
        dict(
            attempts=0,
            task_name=stored_job.task_name,
            task_args=stored_job.task_args,
            meta=stored_job.meta,
            run_at=now,
        ),
        1,
    )


async def test_sqlalchemy_sfu_task_lock_manager_no_tasks(sa_db_engine: aiosa.AsyncEngine) -> None:
    lock_mgr = SqlAlchemySfuLockManager(sa_db_engine)

    lock, delay = await lock_mgr.lock_next_task()
    assert lock is None


@pytest.mark.time_machine("2026-09-06 12:00 +0000", tick=False)
async def test_sqlalchemy_sfu_job_lock_manager_no_upcoming_tasks(
    stored_task_factory: type[StoredTaskFactory],
    sa_db_engine: aiosa.AsyncEngine,
    sa_task_storage: TaskSqlAlchemyStorage,
) -> None:
    now = dt.datetime.now(dt.timezone.utc)
    stored_task = stored_task_factory.build(run_at=now + dt.timedelta(microseconds=1))
    await sa_task_storage.insert(dc.asdict(stored_task))

    lock_mgr = SqlAlchemySfuLockManager(sa_db_engine)

    lock, delay = await lock_mgr.lock_next_task()
    assert lock is None


@pytest.mark.parametrize("status", [sx.TaskStatus.Pending, sx.TaskStatus.Failed])
async def test_sqlalchemy_sfu_task_lock_manager_task_order(
    stored_task_factory: type[StoredTaskFactory],
    sa_db_engine: aiosa.AsyncEngine,
    sa_task_storage: TaskSqlAlchemyStorage,
    status: sx.TaskStatus,
) -> None:
    if status == sx.TaskStatus.Pending:
        task_factory = stored_task_factory.pending()
    elif status == sx.TaskStatus.Failed:
        task_factory = stored_task_factory.failed()
    else:
        raise AssertionError("unreachable")

    now = dt.datetime.now(dt.timezone.utc)
    task = task_factory.build(run_at=now + dt.timedelta(microseconds=1))
    await sa_task_storage.insert(dc.asdict(task))
    expected_task = task_factory.build(run_at=now)
    await sa_task_storage.insert(dc.asdict(expected_task))

    lock_mgr = SqlAlchemySfuLockManager(sa_db_engine)

    lock, delay = await lock_mgr.lock_next_task()
    assert lock is not None
    assert lock.task.id == expected_task.id
    await lock.release()


@pytest.mark.parametrize("status", [sx.TaskStatus.Succeeded, sx.TaskStatus.Error])
async def test_sqlalchemy_sfu_task_lock_manager_task_skip(
    stored_task_factory: type[StoredTaskFactory],
    sa_db_engine: aiosa.AsyncEngine,
    sa_task_storage: TaskSqlAlchemyStorage,
    status: sx.TaskStatus,
) -> None:
    if status == sx.TaskStatus.Succeeded:
        task_factory = stored_task_factory.succeeded()
    elif status == sx.TaskStatus.Error:
        task_factory = stored_task_factory.error()
    else:
        raise AssertionError("unreachable")

    await sa_task_storage.insert(dc.asdict(task_factory.build()))

    lock_mgr = SqlAlchemySfuLockManager(sa_db_engine)
    lock, delay = await lock_mgr.lock_next_task()
    assert lock is None


async def test_sqlalchemy_sfu_task_lock_release(
    stored_task_factory: type[StoredTaskFactory],
    sa_db_engine: aiosa.AsyncEngine,
    sa_task_storage: TaskSqlAlchemyStorage,
) -> None:
    stored_task = stored_task_factory.pending().build()
    await sa_task_storage.insert(dc.asdict(stored_task))

    lock_mgr = SqlAlchemySfuLockManager(sa_db_engine)

    lock, delay = await lock_mgr.lock_next_task()
    assert lock is not None
    await sa_task_storage.assert_has_exactly({}, 0)

    await lock.release()
    await sa_task_storage.assert_has_exactly(
        dict(
            id=stored_task.id,
            job_id=stored_task.job_id,
            sequence_number=stored_task.sequence_number,
            created_at=stored_task.created_at,
            status=stored_task.status,
            attempts=stored_task.attempts,
            task_name=stored_task.task_name,
            task_args=stored_task.task_args,
            meta=stored_task.meta,
            run_at=stored_task.run_at,
            acquired_by=None,
            acquired_until=None,
        ),
        1,
    )


async def test_sqlalchemy_sfu_task_lock_context_manager_success(
    stored_task_factory: type[StoredTaskFactory],
    sa_db_engine: aiosa.AsyncEngine,
    sa_task_storage: TaskSqlAlchemyStorage,
) -> None:
    stored_task = stored_task_factory.pending().build()
    await sa_task_storage.insert(dc.asdict(stored_task))

    lock_mgr = SqlAlchemySfuLockManager(sa_db_engine)

    lock, delay = await lock_mgr.lock_next_task()
    assert lock is not None
    async with lock:
        await sa_task_storage.assert_has_exactly({}, 0)

    await sa_task_storage.assert_has_exactly(
        dict(
            id=stored_task.id,
            job_id=stored_task.job_id,
            sequence_number=stored_task.sequence_number,
            created_at=stored_task.created_at,
            status=sx.TaskStatus.Succeeded,
            attempts=stored_task.attempts,
            task_name=stored_task.task_name,
            task_args=stored_task.task_args,
            meta=stored_task.meta,
            run_at=stored_task.run_at,
            acquired_by=None,
            acquired_until=None,
        ),
        1,
    )


async def test_sqlalchemy_sfu_task_lock_context_manager_exception(
    stored_task_factory: type[StoredTaskFactory],
    sa_db_engine: aiosa.AsyncEngine,
    sa_task_storage: TaskSqlAlchemyStorage,
) -> None:
    stored_task = stored_task_factory.pending().build()
    await sa_task_storage.insert(dc.asdict(stored_task))

    lock_mgr = SqlAlchemySfuLockManager(sa_db_engine)

    lock, delay = await lock_mgr.lock_next_task()
    assert lock is not None
    with pytest.raises(CustomException):
        async with lock:
            raise CustomException()

    await sa_task_storage.assert_has_exactly(
        dict(
            id=stored_task.id,
            job_id=stored_task.job_id,
            sequence_number=stored_task.sequence_number,
            created_at=stored_task.created_at,
            status=sx.TaskStatus.Error,
            attempts=stored_task.attempts + 1,
            task_name=stored_task.task_name,
            task_args=stored_task.task_args,
            meta=stored_task.meta,
            run_at=stored_task.run_at,
            acquired_by=None,
            acquired_until=None,
        ),
        1,
    )


async def test_sqlalchemy_sfu_task_lock_release_with_status(
    stored_task_factory: type[StoredTaskFactory],
    sa_db_engine: aiosa.AsyncEngine,
    sa_task_storage: TaskSqlAlchemyStorage,
) -> None:
    stored_task = stored_task_factory.pending().build()
    await sa_task_storage.insert(dc.asdict(stored_task))

    lock_mgr = SqlAlchemySfuLockManager(sa_db_engine)

    lock, delay = await lock_mgr.lock_next_task()
    assert lock is not None
    await lock.release_with_error()

    await sa_task_storage.assert_has_exactly({}, 1)
    await sa_task_storage.assert_has_exactly(
        dict(
            id=stored_task.id,
            job_id=stored_task.job_id,
            sequence_number=stored_task.sequence_number,
            created_at=stored_task.created_at,
            status=sx.TaskStatus.Error,
            attempts=stored_task.attempts + 1,
            task_name=stored_task.task_name,
            task_args=stored_task.task_args,
            meta=stored_task.meta,
            run_at=stored_task.run_at,
            acquired_by=None,
            acquired_until=None,
        ),
        1,
    )
