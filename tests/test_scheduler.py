import dataclasses as dc
import datetime as dt
from typing import Any

import msgspec.json
import pytest
from pytest_lazy_fixtures import lf

import schedex as sx
from tests.factories import JobFactory, StoredJobFactory
from tests.types import BaseStorage


@pytest.fixture(
    params=[
        pytest.param((lf("sa_job_storage"), lf("sa_scheduler")), id="sqlalchemy"),
        pytest.param((lf("pymongo_job_storage"), lf("pymongo_scheduler")), id="pymongo"),
    ],
)
async def test_bundle(
    request: pytest.FixtureRequest,
) -> tuple[BaseStorage, sx.Scheduler[sx.PeriodicSchedule]]:
    storage: BaseStorage = request.param[0]
    transaction_manager: sx.Scheduler[sx.PeriodicSchedule] = request.param[1]
    return storage, transaction_manager


@pytest.fixture
def job_storage(test_bundle: tuple[BaseStorage, sx.TransactionManager[Any]]) -> BaseStorage:
    return test_bundle[0]


@pytest.fixture
def scheduler(
    test_bundle: tuple[BaseStorage, sx.Scheduler[sx.PeriodicSchedule]],
) -> sx.Scheduler[sx.PeriodicSchedule]:
    return test_bundle[1]


@pytest.mark.time_machine("2026-10-10 22:30 +0500", tick=False)
async def test_scheduler_add_job(
    stored_job_factory: type[StoredJobFactory],
    job_storage: BaseStorage,
    scheduler: sx.Scheduler[sx.PeriodicSchedule],
) -> None:
    job = JobFactory.build()
    await scheduler.add_job(job)

    await job_storage.assert_has_exactly(
        dict(
            id=job.id,
            created_at=dt.datetime.now(dt.timezone.utc),
            status=sx.JobStatus.Active,
            schedule=job.schedule.serialize(),
            count=0,
            task_name=job.task.name,
            task_args=job.task.serialize(),
            meta=msgspec.json.encode(job.meta),
            run_at=job.schedule.next_run(dt.datetime.now(dt.timezone.utc), None, 0),
            acquired_by=None,
            acquired_until=None,
        ),
        count=1,
    )


async def test_scheduler_add_job_duplicate(
    stored_job_factory: type[StoredJobFactory],
    job_storage: BaseStorage,
    scheduler: sx.Scheduler[sx.PeriodicSchedule],
) -> None:
    job = JobFactory.build()
    await scheduler.add_job(job)

    with pytest.raises(sx.JobAlreadyExists):
        await scheduler.add_job(job)

    await job_storage.assert_has_exactly({}, count=1)


async def test_scheduler_cancel_job(
    stored_job_factory: type[StoredJobFactory],
    job_storage: BaseStorage,
    scheduler: sx.Scheduler[sx.PeriodicSchedule],
) -> None:
    stored_job = stored_job_factory.build()
    await job_storage.insert(dc.asdict(stored_job))

    await scheduler.cancel_job(stored_job.id)
    await job_storage.assert_empty()


async def test_scheduler_cancel_non_existing_job(
    stored_job_factory: type[StoredJobFactory],
    job_storage: BaseStorage,
    scheduler: sx.Scheduler[sx.PeriodicSchedule],
) -> None:
    await scheduler.cancel_job("test-id")
    await job_storage.assert_empty()


async def test_scheduler_get_job(
    stored_job_factory: type[StoredJobFactory],
    job_storage: BaseStorage,
    scheduler: sx.Scheduler[sx.PeriodicSchedule],
):
    jobs = JobFactory.batch(size=3)
    job = jobs[0]

    await job_storage.insert(
        dict(
            id=job.id,
            created_at=dt.datetime.now(dt.timezone.utc),
            status=sx.JobStatus.Active,
            schedule=job.schedule.serialize(),
            count=0,
            task_name=job.task.name,
            task_args=job.task.serialize(),
            meta=msgspec.json.encode(job.meta),
            run_at=job.schedule.next_run(dt.datetime.now(dt.timezone.utc), None, 0),
            acquired_by=None,
            acquired_until=None,
        )
    )

    job = await scheduler.get_job(job.id)
    assert job is not None

    assert job.id == job.id
    assert job.schedule == job.schedule
    assert job.meta == job.meta
