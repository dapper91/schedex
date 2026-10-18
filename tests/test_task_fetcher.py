import dataclasses as dc
import datetime as dt
from typing import Any

import pytest
from pytest_lazy_fixtures import lf

import schedex as sx
from tests.factories import StoredJobFactory, StoredTaskFactory
from tests.types import BaseStorage


@pytest.fixture(
    params=[
        pytest.param(
            (
                lf("sa_leasing_job_storage"),
                lf("sa_leasing_task_storage"),
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
            id="mongodb",
        ),
    ],
)
async def test_bundle(request: pytest.FixtureRequest) -> tuple[BaseStorage, BaseStorage, sx.LockManager[Any, Any]]:
    job_storage: BaseStorage = request.param[0]
    task_storage: BaseStorage = request.param[1]
    lock_manager: sx.LockManager[Any, Any] = request.param[2]
    return job_storage, task_storage, lock_manager


@pytest.fixture
def job_storage(test_bundle: tuple[BaseStorage, BaseStorage, sx.LockManager[Any, Any]]) -> BaseStorage:
    return test_bundle[0]


@pytest.fixture
def task_storage(test_bundle: tuple[BaseStorage, BaseStorage, sx.LockManager[Any, Any]]) -> BaseStorage:
    return test_bundle[1]


@pytest.fixture
def lock_manager(test_bundle: tuple[BaseStorage, BaseStorage, sx.LockManager[Any, Any]]) -> sx.LockManager[Any, Any]:
    return test_bundle[2]


async def test_task_fetcher_no_tasks(
    stored_job_factory: type[StoredJobFactory],
    job_storage: BaseStorage,
    lock_manager: sx.LockManager[Any, Any],
) -> None:
    fetcher = sx.TaskFetcher(
        polling_interval=dt.timedelta(seconds=1),
        lock_manager=lock_manager,
        schedule_type=sx.PeriodicSchedule,
    )

    task, delay = await fetcher.fetch_next_task()
    assert task is None
    assert delay == dt.timedelta.max


@pytest.mark.time_machine("2026-09-06 12:00 +0000", tick=False)
async def test_task_fetcher_no_pending_tasks(
    stored_job_factory: type[StoredJobFactory],
    stored_task_factory: type[StoredTaskFactory],
    job_storage: BaseStorage,
    task_storage: BaseStorage,
    lock_manager: sx.LockManager[Any, Any],
) -> None:
    now = dt.datetime.now(dt.timezone.utc)
    stored_task = stored_task_factory.pending().build(run_at=now + dt.timedelta(milliseconds=1))
    await task_storage.insert(dc.asdict(stored_task))

    fetcher = sx.TaskFetcher(
        polling_interval=dt.timedelta(seconds=1),
        lock_manager=lock_manager,
        schedule_type=sx.PeriodicSchedule,
    )

    task, delay = await fetcher.fetch_next_task()
    assert task is None
    assert delay == dt.timedelta(milliseconds=1)


@pytest.mark.time_machine("2026-09-06 12:00 +0000", tick=False)
async def test_task_fetcher_no_pending_jobs(
    stored_job_factory: type[StoredJobFactory],
    stored_task_factory: type[StoredTaskFactory],
    job_storage: BaseStorage,
    task_storage: BaseStorage,
    lock_manager: sx.LockManager[Any, Any],
) -> None:
    now = dt.datetime.now(dt.timezone.utc)
    stored_job = stored_job_factory.build(run_at=now + dt.timedelta(milliseconds=1))
    await job_storage.insert(dc.asdict(stored_job))

    fetcher = sx.TaskFetcher(
        polling_interval=dt.timedelta(seconds=1),
        lock_manager=lock_manager,
        schedule_type=sx.PeriodicSchedule,
    )

    task, delay = await fetcher.fetch_next_task()
    assert task is None
    assert delay == dt.timedelta(milliseconds=1)


@pytest.mark.time_machine("2026-09-06 12:00 +0000", tick=False)
async def test_task_fetcher_pending_task(
    stored_job_factory: type[StoredJobFactory],
    stored_task_factory: type[StoredTaskFactory],
    job_storage: BaseStorage,
    task_storage: BaseStorage,
    lock_manager: sx.LockManager[Any, Any],
) -> None:
    now = dt.datetime.now(dt.timezone.utc)
    stored_task = stored_task_factory.pending().build(run_at=now)
    await task_storage.insert(dc.asdict(stored_task))

    fetcher = sx.TaskFetcher(
        polling_interval=dt.timedelta(seconds=1),
        lock_manager=lock_manager,
        schedule_type=sx.PeriodicSchedule,
    )

    task, delay = await fetcher.fetch_next_task()
    assert task is not None
    assert delay == dt.timedelta(0)


@pytest.mark.time_machine("2026-09-06 12:00 +0000", tick=False)
async def test_task_fetcher_pending_job(
    stored_job_factory: type[StoredJobFactory],
    stored_task_factory: type[StoredTaskFactory],
    job_storage: BaseStorage,
    task_storage: BaseStorage,
    lock_manager: sx.LockManager[Any, Any],
) -> None:
    now = dt.datetime.now(dt.timezone.utc)
    schedule = sx.PeriodicSchedule.periodically(start_at=now, delay=dt.timedelta(minutes=1))
    stored_job = stored_job_factory.build(run_at=now, schedule=schedule.serialize())
    await job_storage.insert(dc.asdict(stored_job))

    fetcher = sx.TaskFetcher(
        polling_interval=dt.timedelta(seconds=1),
        lock_manager=lock_manager,
        schedule_type=sx.PeriodicSchedule,
    )

    task, delay = await fetcher.fetch_next_task()
    assert task is None
    assert delay == dt.timedelta(0)

    await task_storage.assert_has_exactly(
        dict(
            attempts=0,
            task_name=stored_job.task_name,
            task_args=stored_job.task_args,
            meta=stored_job.meta,
            run_at=now,
        ),
        1,
    )
    await job_storage.assert_has_exactly(
        dict(
            id=stored_job.id,
            created_at=stored_job.created_at,
            status=sx.JobStatus.Active,
            schedule=stored_job.schedule,
            count=stored_job.count + 1,
            task_name=stored_job.task_name,
            task_args=stored_job.task_args,
            meta=stored_job.meta,
            run_at=now + dt.timedelta(minutes=1),
            acquired_by=None,
            acquired_until=None,
        ),
        1,
    )


@pytest.mark.time_machine("2026-09-06 12:00 +0000", tick=False)
@pytest.mark.parametrize("expected_first", ["task", "job"])
async def test_task_fetcher_pending_job_task_order(
    stored_job_factory: type[StoredJobFactory],
    stored_task_factory: type[StoredTaskFactory],
    job_storage: BaseStorage,
    task_storage: BaseStorage,
    lock_manager: sx.LockManager[Any, Any],
    expected_first: str,
) -> None:
    now = dt.datetime.now(dt.timezone.utc)
    if expected_first == "task":
        stored_job = stored_job_factory.build(run_at=now + dt.timedelta(seconds=2))
        stored_task = stored_task_factory.pending().build(run_at=now + dt.timedelta(seconds=1))
    elif expected_first == "job":
        stored_job = stored_job_factory.build(run_at=now + dt.timedelta(seconds=1))
        stored_task = stored_task_factory.pending().build(run_at=now + dt.timedelta(seconds=2))
    else:
        raise AssertionError("unreachable")

    await job_storage.insert(dc.asdict(stored_job))
    await task_storage.insert(dc.asdict(stored_task))

    fetcher = sx.TaskFetcher(
        polling_interval=dt.timedelta(seconds=1),
        lock_manager=lock_manager,
        schedule_type=sx.PeriodicSchedule,
    )

    task, delay = await fetcher.fetch_next_task()
    assert task is None
    assert delay == dt.timedelta(seconds=1)
