import dataclasses as dc
import datetime as dt
from typing import Any

import pytest
import time_machine as tm
from pytest_lazy_fixtures import lf

import schedex as sx
from tests.conftest import StoredJobFactory
from tests.factories import StoredTaskFactory
from tests.types import BaseStorage


@pytest.fixture(
    params=[
        pytest.param(
            (
                lf("sa_job_storage"),
                lf("sa_task_storage"),
                lf("sa_leasing_lock_manager"),
            ),
            id="sqlalchemy-leasing",
        ),
        pytest.param(
            (
                lf("pymongo_job_storage"),
                lf("pymongo_task_storage"),
                lf("pymongo_lock_manager"),
            ),
            id="pymongo",
        ),
    ],
)
async def test_bundle(request: pytest.FixtureRequest) -> tuple[BaseStorage, BaseStorage, sx.LockManager[Any, Any]]:
    job_storage: BaseStorage = request.param[0]
    task_storage: BaseStorage = request.param[1]
    lock_manager: sx.LockManager[Any, Any] = request.param[2]
    return job_storage, task_storage, lock_manager


@pytest.fixture
def job_storage(test_bundle: tuple[BaseStorage, BaseStorage, sx.TransactionManager[Any]]) -> BaseStorage:
    return test_bundle[0]


@pytest.fixture
def task_storage(test_bundle: tuple[BaseStorage, BaseStorage, sx.TransactionManager[Any]]) -> BaseStorage:
    return test_bundle[1]


@pytest.fixture
def lock_manager(test_bundle: tuple[BaseStorage, BaseStorage, sx.LockManager[Any, Any]]) -> sx.LockManager[Any, Any]:
    return test_bundle[2]


class CustomException(Exception):
    pass


async def test_leasing_job_lock_manager_no_jobs(
    stored_job_factory: type[StoredJobFactory],
    job_storage: BaseStorage,
    lock_manager: sx.LockManager[Any, Any],
) -> None:
    lock, delay = await lock_manager.lock_upcoming_job()
    assert lock is None


@pytest.mark.time_machine("2026-09-06 12:00 +0000", tick=False)
async def test_leasing_job_lock_manager_no_upcoming_jobs(
    stored_job_factory: type[StoredJobFactory],
    job_storage: BaseStorage,
    lock_manager: sx.LockManager[Any, Any],
) -> None:
    now = dt.datetime.now(dt.timezone.utc)
    stored_job = stored_job_factory.build(run_at=now + dt.timedelta(milliseconds=1))
    await job_storage.insert(dc.asdict(stored_job))

    lock, delay = await lock_manager.lock_upcoming_job()
    assert lock is None


@pytest.mark.time_machine("2026-09-06 12:00 +0000", tick=False)
async def test_leasing_job_lock_manager_job_order(
    stored_job_factory: type[StoredJobFactory],
    job_storage: BaseStorage,
    lock_manager: sx.LockManager[Any, Any],
) -> None:
    now = dt.datetime.now(dt.timezone.utc)

    stored_job = stored_job_factory.build(run_at=now + dt.timedelta(milliseconds=1))
    await job_storage.insert(dc.asdict(stored_job))

    expected_stored_job = stored_job_factory.build(run_at=now)
    await job_storage.insert(dc.asdict(expected_stored_job))

    lock, delay = await lock_manager.lock_upcoming_job()
    assert lock is not None
    assert lock.job.id == expected_stored_job.id

    await lock.release()


@pytest.mark.parametrize("status", [sx.JobStatus.Suspended, sx.JobStatus.Error])
async def test_leasing_job_lock_manager_job_skip(
    stored_job_factory: type[StoredJobFactory],
    job_storage: BaseStorage,
    lock_manager: sx.LockManager[Any, Any],
    status: sx.JobStatus,
) -> None:
    stored_job = stored_job_factory.build(status=status)
    await job_storage.insert(dc.asdict(stored_job))

    lock, delay = await lock_manager.lock_upcoming_job()
    assert lock is None


@pytest.mark.time_machine("2026-09-06 12:00 +0000", tick=False)
async def test_leasing_job_lock_release(
    stored_job_factory: type[StoredJobFactory],
    job_storage: BaseStorage,
    lock_manager: sx.LockManager[Any, Any],
) -> None:
    now = dt.datetime.now(dt.timezone.utc)
    acquire_period = dt.timedelta(seconds=1)
    lock_identifier = "lock-id"

    stored_job = stored_job_factory.build()
    await job_storage.insert(dc.asdict(stored_job))

    lock, delay = await lock_manager.lock_upcoming_job()
    assert lock is not None
    await job_storage.assert_has_exactly(
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
            acquired_by=lock_identifier,
            acquired_until=now + acquire_period,
        ),
        count=1,
    )

    await lock.release()
    await job_storage.assert_has_exactly(
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
        count=1,
    )


@pytest.mark.time_machine("2026-09-06 12:00 +0000", tick=False)
async def test_leasing_job_lock_context_manager_success(
    stored_job_factory: type[StoredJobFactory],
    job_storage: BaseStorage,
    lock_manager: sx.LockManager[Any, Any],
) -> None:
    now = dt.datetime.now(dt.timezone.utc)
    acquire_period = dt.timedelta(seconds=1)
    lock_identifier = "lock-id"

    stored_job = stored_job_factory.build()
    await job_storage.insert(dc.asdict(stored_job))

    lock, delay = await lock_manager.lock_upcoming_job()
    assert lock is not None
    async with lock:
        await job_storage.assert_has_exactly(
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
                acquired_by=lock_identifier,
                acquired_until=now + acquire_period,
            ),
            count=1,
        )

    await job_storage.assert_has_exactly(
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
        count=1,
    )


async def test_leasing_job_lock_context_manager_exception(
    stored_job_factory: type[StoredJobFactory],
    job_storage: BaseStorage,
    lock_manager: sx.LockManager[Any, Any],
) -> None:
    stored_job = stored_job_factory.build()
    await job_storage.insert(dc.asdict(stored_job))

    lock, delay = await lock_manager.lock_upcoming_job()
    assert lock is not None
    with pytest.raises(CustomException):
        async with lock:
            raise CustomException()

    await job_storage.assert_has_exactly(
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
        count=1,
    )


async def test_leasing_job_lock_release_with_error(
    stored_job_factory: type[StoredJobFactory],
    job_storage: BaseStorage,
    lock_manager: sx.LockManager[Any, Any],
) -> None:
    stored_job = stored_job_factory.build()
    await job_storage.insert(dc.asdict(stored_job))

    lock, delay = await lock_manager.lock_upcoming_job()
    assert lock is not None
    await lock.release_with_error()
    await job_storage.assert_has_exactly(
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
        count=1,
    )


async def test_leasing_job_lock_remove(
    stored_job_factory: type[StoredJobFactory],
    job_storage: BaseStorage,
    lock_manager: sx.LockManager[Any, Any],
) -> None:
    stored_job = stored_job_factory.build()
    await job_storage.insert(dc.asdict(stored_job))

    lock, delay = await lock_manager.lock_upcoming_job()
    assert lock is not None
    await lock.remove()
    await job_storage.assert_has_exactly({}, count=0)


async def test_leasing_job_lock_complete(
    stored_job_factory: type[StoredJobFactory],
    job_storage: BaseStorage,
    lock_manager: sx.LockManager[Any, Any],
) -> None:
    stored_job = stored_job_factory.build()
    await job_storage.insert(dc.asdict(stored_job))

    lock, delay = await lock_manager.lock_upcoming_job()
    assert lock is not None
    await lock.complete()
    await job_storage.assert_has_exactly(
        dict(
            id=stored_job.id,
            created_at=stored_job.created_at,
            status=sx.JobStatus.Completed,
            schedule=stored_job.schedule,
            count=stored_job.count,
            task_name=stored_job.task_name,
            task_args=stored_job.task_args,
            meta=stored_job.meta,
            run_at=stored_job.run_at,
            acquired_by=None,
            acquired_until=None,
        ),
        count=1,
    )


@pytest.mark.time_machine("2026-09-06 12:00 +0000", tick=False)
@pytest.mark.parametrize("completed", [True, False])
async def test_leasing_job_lock_create_task(
    stored_job_factory: type[StoredJobFactory],
    job_storage: BaseStorage,
    task_storage: BaseStorage,
    lock_manager: sx.LockManager[Any, Any],
    completed: bool,
) -> None:
    now = dt.datetime.now(dt.timezone.utc)
    if completed:
        next_run_at = None
    else:
        next_run_at = now + dt.timedelta(seconds=1)

    stored_job = stored_job_factory.build()
    await job_storage.insert(dc.asdict(stored_job))

    lock, delay = await lock_manager.lock_upcoming_job()
    assert lock is not None
    await lock.create_task(next_run_at=next_run_at)
    await lock.release()

    if completed:
        await job_storage.assert_has_exactly(
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
        await job_storage.assert_has_exactly(
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

    await task_storage.assert_has_exactly({}, 1)
    await task_storage.assert_has_exactly(
        dict(
            job_id=stored_job.id,
            sequence_number=0,
            created_at=now,
            status=sx.TaskStatus.Pending,
            attempts=0,
            task_name=stored_job.task_name,
            task_args=stored_job.task_args,
            meta=stored_job.meta,
            run_at=now,
        ),
        1,
    )


async def test_leasing_task_lock_manager_no_tasks(lock_manager: sx.LockManager[Any, Any]) -> None:
    lock, delay = await lock_manager.lock_next_task()
    assert lock is None


@pytest.mark.time_machine("2026-09-06 12:00 +0000", tick=False)
async def test_leasing_job_lock_manager_no_upcoming_tasks(
    stored_task_factory: type[StoredTaskFactory],
    task_storage: BaseStorage,
    lock_manager: sx.LockManager[Any, Any],
):
    now = dt.datetime.now(dt.timezone.utc)
    stored_task = stored_task_factory.build(run_at=now + dt.timedelta(milliseconds=1))
    await task_storage.insert(dc.asdict(stored_task))

    lock, delay = await lock_manager.lock_next_task()
    assert lock is None


@pytest.mark.parametrize("status", [sx.TaskStatus.Pending, sx.TaskStatus.Failed])
async def test_leasing_task_lock_manager_task_order(
    stored_task_factory: type[StoredTaskFactory],
    task_storage: BaseStorage,
    lock_manager: sx.LockManager[Any, Any],
    status: sx.TaskStatus,
) -> None:
    if status == sx.TaskStatus.Pending:
        stored_task_factory = stored_task_factory.pending()
    elif status == sx.TaskStatus.Failed:
        stored_task_factory = stored_task_factory.failed()
    else:
        raise AssertionError("unreachable")

    now = dt.datetime.now(dt.timezone.utc)
    stored_task = stored_task_factory.build(run_at=now + dt.timedelta(milliseconds=1))
    await task_storage.insert(dc.asdict(stored_task))

    expected_stored_task = stored_task_factory.build(run_at=now)
    await task_storage.insert(dc.asdict(expected_stored_task))

    lock, delay = await lock_manager.lock_next_task()
    assert lock is not None
    assert lock.task.id == expected_stored_task.id
    await lock.release()


@pytest.mark.parametrize("status", [sx.TaskStatus.Succeeded, sx.TaskStatus.Error])
async def test_leasing_task_lock_manager_task_skip(
    stored_task_factory: type[StoredTaskFactory],
    task_storage: BaseStorage,
    lock_manager: sx.LockManager[Any, Any],
    status: sx.TaskStatus,
) -> None:
    if status == sx.TaskStatus.Succeeded:
        stored_task_factory = stored_task_factory.succeeded()
    elif status == sx.TaskStatus.Error:
        stored_task_factory = stored_task_factory.error()
    else:
        raise AssertionError("unreachable")

    stored_task = stored_task_factory.build()
    await task_storage.insert(dc.asdict(stored_task))

    lock, delay = await lock_manager.lock_next_task()
    assert lock is None


@pytest.mark.time_machine("2026-09-06 12:00 +0000", tick=False)
async def test_leasing_task_lock_release(
    stored_task_factory: type[StoredTaskFactory],
    task_storage: BaseStorage,
    lock_manager: sx.LockManager[Any, Any],
) -> None:
    now = dt.datetime.now(dt.timezone.utc)
    acquire_period = dt.timedelta(seconds=1)
    lock_identifier = "lock-id"

    stored_task = stored_task_factory.pending().build()
    await task_storage.insert(dc.asdict(stored_task))

    lock, delay = await lock_manager.lock_next_task()
    assert lock is not None

    await task_storage.assert_has_exactly(
        dict(
            created_at=stored_task.created_at,
            status=stored_task.status,
            sequence_number=stored_task.sequence_number,
            attempts=stored_task.attempts,
            task_name=stored_task.task_name,
            task_args=stored_task.task_args,
            meta=stored_task.meta,
            run_at=stored_task.run_at,
            acquired_by=lock_identifier,
            acquired_until=now + acquire_period,
        ),
        1,
    )

    await lock.release()
    await task_storage.assert_has_exactly(
        dict(
            created_at=stored_task.created_at,
            status=stored_task.status,
            sequence_number=stored_task.sequence_number,
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


@pytest.mark.time_machine("2026-09-06 12:00 +0000", tick=False)
async def test_leasing_task_lock_context_manager_success(
    stored_task_factory: type[StoredTaskFactory],
    task_storage: BaseStorage,
    lock_manager: sx.LockManager[Any, Any],
) -> None:
    now = dt.datetime.now(dt.timezone.utc)
    acquire_period = dt.timedelta(seconds=1)
    lock_identifier = "lock-id"

    stored_task = stored_task_factory.pending().build()
    await task_storage.insert(dc.asdict(stored_task))

    lock, delay = await lock_manager.lock_next_task()
    assert lock is not None
    async with lock:
        await task_storage.assert_has_exactly(
            dict(
                created_at=stored_task.created_at,
                status=stored_task.status,
                sequence_number=stored_task.sequence_number,
                attempts=stored_task.attempts,
                task_name=stored_task.task_name,
                task_args=stored_task.task_args,
                meta=stored_task.meta,
                run_at=stored_task.run_at,
                acquired_by=lock_identifier,
                acquired_until=now + acquire_period,
            ),
            1,
        )

    await task_storage.assert_has_exactly(
        dict(
            created_at=stored_task.created_at,
            status=sx.TaskStatus.Succeeded,
            sequence_number=stored_task.sequence_number,
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


async def test_leasing_task_lock_context_manager_exception(
    stored_task_factory: type[StoredTaskFactory],
    task_storage: BaseStorage,
    lock_manager: sx.LockManager[Any, Any],
) -> None:
    stored_task = stored_task_factory.pending().build()
    await task_storage.insert(dc.asdict(stored_task))

    lock, delay = await lock_manager.lock_next_task()
    assert lock is not None
    with pytest.raises(CustomException):
        async with lock:
            raise CustomException()

    await task_storage.assert_has_exactly(
        dict(
            created_at=stored_task.created_at,
            status=sx.TaskStatus.Error,
            sequence_number=stored_task.sequence_number,
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


async def test_leasing_task_lock_release_with_status(
    stored_task_factory: type[StoredTaskFactory],
    task_storage: BaseStorage,
    lock_manager: sx.LockManager[Any, Any],
) -> None:
    stored_task = stored_task_factory.pending().build()
    await task_storage.insert(dc.asdict(stored_task))

    lock, delay = await lock_manager.lock_next_task()
    assert lock is not None
    await lock.release_with_error()
    await task_storage.assert_has_exactly(
        dict(
            created_at=stored_task.created_at,
            status=sx.TaskStatus.Error,
            sequence_number=stored_task.sequence_number,
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


async def test_leasing_task_lock_remove(
    stored_task_factory: type[StoredTaskFactory],
    task_storage: BaseStorage,
    lock_manager: sx.LockManager[Any, Any],
) -> None:
    stored_task = stored_task_factory.pending().build()
    await task_storage.insert(dc.asdict(stored_task))

    lock, delay = await lock_manager.lock_next_task()
    assert lock is not None
    await lock.remove()
    await task_storage.assert_has_exactly({}, 0)


@pytest.mark.time_machine("2026-09-06 12:00 +0000", tick=False)
async def test_leasing_task_lock_lease_expired(
    time_machine: tm.TimeMachineFixture,
    stored_task_factory: type[StoredTaskFactory],
    task_storage: BaseStorage,
    lock_manager: sx.LockManager[Any, Any],
) -> None:
    now = dt.datetime.now(dt.timezone.utc)
    acquire_period = dt.timedelta(seconds=1)
    lock_identifier = "lock-id"

    stored_task = stored_task_factory.executing(
        acquired_by=lock_identifier, acquired_until=now + acquire_period
    ).build()
    await task_storage.insert(dc.asdict(stored_task))

    lock, delay = await lock_manager.lock_next_task()
    assert lock is None

    time_machine.shift(acquire_period)
    now = dt.datetime.now(dt.timezone.utc)

    lock, delay = await lock_manager.lock_next_task()
    assert lock is not None
    await task_storage.assert_has_exactly(
        dict(
            created_at=stored_task.created_at,
            status=sx.TaskStatus.Executing,
            sequence_number=stored_task.sequence_number,
            attempts=stored_task.attempts,
            task_name=stored_task.task_name,
            task_args=stored_task.task_args,
            meta=stored_task.meta,
            run_at=stored_task.run_at,
            acquired_by=lock_identifier,
            acquired_until=now + acquire_period,
        ),
        1,
    )


@pytest.mark.time_machine("2026-09-06 12:00 +0000", tick=False)
async def test__leasing_task_lock_extend(
    time_machine: tm.TimeMachineFixture,
    stored_task_factory: type[StoredTaskFactory],
    task_storage: BaseStorage,
    lock_manager: sx.LockManager[Any, Any],
) -> None:
    now = dt.datetime.now(dt.timezone.utc)
    acquire_period = dt.timedelta(seconds=1)
    lock_identifier = "lock-id"

    stored_task = stored_task_factory.pending().build()
    await task_storage.insert(dc.asdict(stored_task))

    lock, delay = await lock_manager.lock_next_task()
    assert lock is not None
    assert await lock.is_locked() is True
    assert await lock.remain() == acquire_period

    await task_storage.assert_has_exactly(
        dict(
            created_at=stored_task.created_at,
            status=sx.TaskStatus.Pending,
            sequence_number=stored_task.sequence_number,
            attempts=stored_task.attempts,
            task_name=stored_task.task_name,
            task_args=stored_task.task_args,
            meta=stored_task.meta,
            run_at=stored_task.run_at,
            acquired_by=lock_identifier,
            acquired_until=now + acquire_period,
        ),
        1,
    )

    time_machine.shift(acquire_period / 2)
    now = dt.datetime.now(dt.timezone.utc)

    assert await lock.is_locked() is True
    assert await lock.remain() == acquire_period / 2

    result = await lock.extend(acquire_period)
    assert result is True
    assert await lock.is_locked() is True
    assert await lock.remain() == acquire_period

    await task_storage.assert_has_exactly(
        dict(
            created_at=stored_task.created_at,
            status=sx.TaskStatus.Pending,
            sequence_number=stored_task.sequence_number,
            attempts=stored_task.attempts,
            task_name=stored_task.task_name,
            task_args=stored_task.task_args,
            meta=stored_task.meta,
            run_at=stored_task.run_at,
            acquired_by=lock_identifier,
            acquired_until=now + acquire_period,
        ),
        1,
    )

    time_machine.shift(acquire_period)
    assert await lock.is_locked() is False
    assert await lock.remain() == dt.timedelta(0)
