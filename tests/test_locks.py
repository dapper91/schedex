import dataclasses as dc
import datetime as dt
import random as rnd
import unittest.mock as mk
from typing import Any, Callable

import pytest
from pytest_lazy_fixtures import lf

import schedex as sx
from tests.conftest import StoredJobFactory
from tests.factories import StoredTaskFactory
from tests.types import BaseStorage


@pytest.fixture(
    params=[
        pytest.param(
            (
                lf("sa_leasing_job_storage"),
                lf("sa_leasing_task_storage"),
                lf("sa_leasing_lock_manager_builder"),
            ),
            id="sqlalchemy-leasing",
        ),
        pytest.param(
            (
                lf("sa_sfu_job_storage"),
                lf("sa_sfu_task_storage"),
                lf("sa_sfu_lock_manager_builder"),
            ),
            id="sqlalchemy-sfu",
        ),
        pytest.param(
            (
                lf("pymongo_job_storage"),
                lf("pymongo_task_storage"),
                lf("pymongo_lock_manager_builder"),
            ),
            id="mongodb",
        ),
    ],
)
async def test_bundle(
    request: pytest.FixtureRequest,
) -> tuple[BaseStorage, BaseStorage, Callable[..., sx.LockManager[Any, Any]]]:
    job_storage: BaseStorage = request.param[0]
    task_storage: BaseStorage = request.param[1]
    lock_manager_builder: Callable[..., sx.LockManager[Any, Any]] = request.param[2]
    return job_storage, task_storage, lock_manager_builder


@pytest.fixture
def job_storage(test_bundle: tuple[BaseStorage, BaseStorage, Callable[..., sx.LockManager[Any, Any]]]) -> BaseStorage:
    return test_bundle[0]


@pytest.fixture
def task_storage(test_bundle: tuple[BaseStorage, BaseStorage, Callable[..., sx.LockManager[Any, Any]]]) -> BaseStorage:
    return test_bundle[1]


@pytest.fixture
def lock_manager_builder(
    test_bundle: tuple[BaseStorage, BaseStorage, Callable[..., sx.LockManager[Any, Any]]],
) -> Callable[..., sx.LockManager[Any, Any]]:
    return test_bundle[2]


@pytest.fixture
def lock_manager(
    test_bundle: tuple[BaseStorage, BaseStorage, Callable[..., sx.LockManager[Any, Any]]],
) -> sx.LockManager[Any, Any]:
    return test_bundle[2]()


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

    stored_jobs = [
        stored_job_factory.build(run_at=now + dt.timedelta(milliseconds=rnd.randint(1, 10))) for _ in range(10)
    ]
    stored_jobs.append(upcoming_job := stored_job_factory.build(run_at=now))
    rnd.shuffle(stored_jobs)

    for stored_job in stored_jobs:
        await job_storage.insert(dc.asdict(stored_job))

    lock, delay = await lock_manager.lock_upcoming_job()
    assert lock is not None
    assert lock.job.id == upcoming_job.id

    await lock.release()


@pytest.mark.parametrize("status", [sx.JobStatus.Suspended, sx.JobStatus.Error])
async def test_leasing_job_lock_manager_job_status_filter(
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
    stored_job = stored_job_factory.build()
    await job_storage.insert(dc.asdict(stored_job))

    lock, delay = await lock_manager.lock_upcoming_job()
    assert lock is not None
    await job_storage.assert_is_locked(
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
        ),
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
    stored_job = stored_job_factory.build()
    await job_storage.insert(dc.asdict(stored_job))

    lock, delay = await lock_manager.lock_upcoming_job()
    assert lock is not None
    async with lock:
        await job_storage.assert_is_locked(
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
            ),
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


@pytest.mark.time_machine("2026-09-06 12:00 +0000", tick=False)
async def test_leasing_job_lock_manager_event_on_release(
    stored_job_factory: type[StoredJobFactory],
    job_storage: BaseStorage,
    lock_manager_builder: Callable[..., sx.LockManager[Any, Any]],
) -> None:
    await job_storage.insert(dc.asdict(stored_job_factory.build()))

    event_sender_mock = mk.MagicMock(spec=sx.EventSender)
    lock_manager = lock_manager_builder(event_sender=event_sender_mock)

    lock, delay = await lock_manager.lock_upcoming_job()
    assert lock is not None
    await lock.release()
    event_sender_mock.send.assert_called_with(
        sx.Event(
            kind=sx.EventKind.JobReady,
            timestamp=dt.datetime.now(dt.timezone.utc),
            id=mk.ANY,
        )
    )


@pytest.mark.time_machine("2026-09-06 12:00 +0000", tick=False)
async def test_leasing_job_lock_manager_event_on_remove(
    stored_job_factory: type[StoredJobFactory],
    job_storage: BaseStorage,
    lock_manager_builder: Callable[..., sx.LockManager[Any, Any]],
) -> None:
    await job_storage.insert(dc.asdict(stored_job_factory.build()))

    event_sender_mock = mk.MagicMock(spec=sx.EventSender)
    lock_manager = lock_manager_builder(event_sender=event_sender_mock)

    lock, delay = await lock_manager.lock_upcoming_job()
    assert lock is not None
    await lock.remove()
    event_sender_mock.send.assert_called_with(
        sx.Event(
            kind=sx.EventKind.JobCompleted,
            timestamp=dt.datetime.now(dt.timezone.utc),
            id=mk.ANY,
        )
    )


@pytest.mark.time_machine("2026-09-06 12:00 +0000", tick=False)
async def test_leasing_job_lock_manager_event_on_complete(
    stored_job_factory: type[StoredJobFactory],
    job_storage: BaseStorage,
    lock_manager_builder: Callable[..., sx.LockManager[Any, Any]],
) -> None:
    await job_storage.insert(dc.asdict(stored_job_factory.build()))

    event_sender_mock = mk.MagicMock(spec=sx.EventSender)
    lock_manager = lock_manager_builder(event_sender=event_sender_mock)

    lock, delay = await lock_manager.lock_upcoming_job()
    assert lock is not None
    await lock.complete()
    event_sender_mock.send.assert_called_with(
        sx.Event(
            kind=sx.EventKind.JobCompleted,
            timestamp=dt.datetime.now(dt.timezone.utc),
            id=mk.ANY,
        )
    )


@pytest.mark.time_machine("2026-09-06 12:00 +0000", tick=False)
async def test_leasing_job_lock_manager_event_on_error(
    stored_job_factory: type[StoredJobFactory],
    job_storage: BaseStorage,
    lock_manager_builder: Callable[..., sx.LockManager[Any, Any]],
) -> None:
    await job_storage.insert(dc.asdict(stored_job_factory.build()))

    event_sender_mock = mk.MagicMock(spec=sx.EventSender)
    lock_manager = lock_manager_builder(event_sender=event_sender_mock)

    lock, delay = await lock_manager.lock_upcoming_job()
    assert lock is not None
    await lock.release_with_error()
    event_sender_mock.send.assert_called_with(
        sx.Event(
            kind=sx.EventKind.JobCanceled,
            timestamp=dt.datetime.now(dt.timezone.utc),
            id=mk.ANY,
        )
    )


@pytest.mark.time_machine("2026-09-06 12:00 +0000", tick=False)
@pytest.mark.parametrize("next_run_at", [True, False])
async def test_leasing_job_lock_manager_event_on_create_task(
    stored_job_factory: type[StoredJobFactory],
    job_storage: BaseStorage,
    lock_manager_builder: Callable[..., sx.LockManager[Any, Any]],
    next_run_at: bool,
) -> None:
    now = dt.datetime.now(dt.timezone.utc)
    await job_storage.insert(dc.asdict(stored_job_factory.build(run_at=now)))

    event_sender_mock = mk.MagicMock(spec=sx.EventSender)
    lock_manager = lock_manager_builder(event_sender=event_sender_mock)

    lock, delay = await lock_manager.lock_upcoming_job()
    assert lock is not None
    await lock.create_task(now if next_run_at else None)
    await lock.release()
    event_sender_mock.send.assert_has_calls(
        [
            mk.call(
                sx.Event(
                    kind=sx.EventKind.JobReady if next_run_at else sx.EventKind.JobCompleted,
                    timestamp=dt.datetime.now(dt.timezone.utc),
                    id=mk.ANY,
                )
            ),
            mk.call(
                sx.Event(
                    kind=sx.EventKind.TaskReady,
                    timestamp=dt.datetime.now(dt.timezone.utc),
                    id=mk.ANY,
                )
            ),
        ]
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

    stored_tasks = [
        stored_task_factory.build(run_at=now + dt.timedelta(milliseconds=rnd.randint(1, 10))) for _ in range(10)
    ]
    stored_tasks.append(upcoming_task := stored_task_factory.build(run_at=now))
    rnd.shuffle(stored_tasks)

    for stored_task in stored_tasks:
        await task_storage.insert(dc.asdict(stored_task))

    lock, delay = await lock_manager.lock_next_task()
    assert lock is not None
    assert lock.task.id == upcoming_task.id
    await lock.release()


@pytest.mark.parametrize("status", [sx.TaskStatus.Succeeded, sx.TaskStatus.Error])
async def test_leasing_task_lock_manager_task_status_filter(
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
    stored_task = stored_task_factory.pending().build()
    await task_storage.insert(dc.asdict(stored_task))

    lock, delay = await lock_manager.lock_next_task()
    assert lock is not None

    await task_storage.assert_is_locked(
        dict(
            created_at=stored_task.created_at,
            status=stored_task.status,
            sequence_number=stored_task.sequence_number,
            attempts=stored_task.attempts,
            task_name=stored_task.task_name,
            task_args=stored_task.task_args,
            meta=stored_task.meta,
            run_at=stored_task.run_at,
        ),
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
    stored_task = stored_task_factory.pending().build()
    await task_storage.insert(dc.asdict(stored_task))

    lock, delay = await lock_manager.lock_next_task()
    assert lock is not None
    async with lock:
        await task_storage.assert_is_locked(
            dict(
                created_at=stored_task.created_at,
                status=stored_task.status,
                sequence_number=stored_task.sequence_number,
                attempts=stored_task.attempts,
                task_name=stored_task.task_name,
                task_args=stored_task.task_args,
                meta=stored_task.meta,
                run_at=stored_task.run_at,
            ),
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
async def test_leasing_task_lock_manager_event_on_release(
    stored_task_factory: type[StoredTaskFactory],
    task_storage: BaseStorage,
    lock_manager_builder: Callable[..., sx.LockManager[Any, Any]],
) -> None:
    await task_storage.insert(dc.asdict(stored_task_factory.pending().build()))

    event_sender_mock = mk.MagicMock(spec=sx.EventSender)
    lock_manager = lock_manager_builder(event_sender=event_sender_mock)

    lock, delay = await lock_manager.lock_next_task()
    assert lock is not None

    await lock.release()
    event_sender_mock.send.assert_called_with(
        sx.Event(
            kind=sx.EventKind.TaskReady,
            timestamp=dt.datetime.now(dt.timezone.utc),
            id=mk.ANY,
        )
    )


@pytest.mark.time_machine("2026-09-06 12:00 +0000", tick=False)
async def test_leasing_task_lock_manager_event_on_remove(
    stored_task_factory: type[StoredTaskFactory],
    task_storage: BaseStorage,
    lock_manager_builder: Callable[..., sx.LockManager[Any, Any]],
) -> None:
    await task_storage.insert(dc.asdict(stored_task_factory.pending().build()))

    event_sender_mock = mk.MagicMock(spec=sx.EventSender)
    lock_manager = lock_manager_builder(event_sender=event_sender_mock)

    lock, delay = await lock_manager.lock_next_task()
    assert lock is not None

    await lock.remove()
    event_sender_mock.send.assert_called_with(
        sx.Event(
            kind=sx.EventKind.TaskSucceeded,
            timestamp=dt.datetime.now(dt.timezone.utc),
            id=mk.ANY,
        )
    )


@pytest.mark.time_machine("2026-09-06 12:00 +0000", tick=False)
async def test_leasing_task_lock_manager_event_on_error(
    stored_task_factory: type[StoredTaskFactory],
    task_storage: BaseStorage,
    lock_manager_builder: Callable[..., sx.LockManager[Any, Any]],
) -> None:
    await task_storage.insert(dc.asdict(stored_task_factory.pending().build()))

    event_sender_mock = mk.MagicMock(spec=sx.EventSender)
    lock_manager = lock_manager_builder(event_sender=event_sender_mock)

    lock, delay = await lock_manager.lock_next_task()
    assert lock is not None

    await lock.release_with_error()
    event_sender_mock.send.assert_called_with(
        sx.Event(
            kind=sx.EventKind.TaskFailed,
            timestamp=dt.datetime.now(dt.timezone.utc),
            id=mk.ANY,
        )
    )


@pytest.mark.time_machine("2026-09-06 12:00 +0000", tick=False)
async def test_leasing_task_lock_manager_event_on_success(
    stored_task_factory: type[StoredTaskFactory],
    task_storage: BaseStorage,
    lock_manager_builder: Callable[..., sx.LockManager[Any, Any]],
) -> None:
    await task_storage.insert(dc.asdict(stored_task_factory.pending().build()))

    event_sender_mock = mk.MagicMock(spec=sx.EventSender)
    lock_manager = lock_manager_builder(event_sender=event_sender_mock)

    lock, delay = await lock_manager.lock_next_task()
    assert lock is not None

    await lock.succeed()
    event_sender_mock.send.assert_called_with(
        sx.Event(
            kind=sx.EventKind.TaskSucceeded,
            timestamp=dt.datetime.now(dt.timezone.utc),
            id=mk.ANY,
        )
    )
