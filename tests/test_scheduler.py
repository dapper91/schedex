import dataclasses as dc
import datetime as dt
from typing import Any, Callable, Optional
from unittest import mock as mk

import msgspec.json
import pytest
from pytest_lazy_fixtures import lf

import schedex as sx
from tests.factories import JobFactory, StoredJobFactory
from tests.types import BaseStorage, StorageTransaction


class CustomException(Exception):
    pass


@pytest.fixture(
    params=[
        pytest.param(
            (
                lf("sa_leasing_job_storage"),
                lf("sa_storage_transaction"),
                lf("sa_scheduler_builder"),
                lf("sa_transactional_scheduler_builder"),
            ),
            id="sqlalchemy",
        ),
        pytest.param(
            (
                lf("pymongo_job_storage"),
                lf("pymongo_storage_transaction"),
                lf("pymongo_scheduler_builder"),
                lf("pymongo_transactional_scheduler_builder"),
            ),
            id="mongodb",
        ),
    ],
)
async def test_bundle(
    request: pytest.FixtureRequest,
) -> tuple[
    BaseStorage,
    StorageTransaction,
    Callable[..., sx.Scheduler[sx.PeriodicSchedule]],
    Callable[..., sx.TransactionalScheduler[Any, sx.PeriodicSchedule]],
]:
    storage: BaseStorage = request.param[0]
    storage_transaction: StorageTransaction = request.param[1]
    scheduler_builder: Callable[..., sx.Scheduler[sx.PeriodicSchedule]] = request.param[2]
    transactional_scheduler_builder: Callable[..., sx.TransactionalScheduler[Any, sx.PeriodicSchedule]] = request.param[
        3
    ]
    return storage, storage_transaction, scheduler_builder, transactional_scheduler_builder


@pytest.fixture
def job_storage(
    test_bundle: tuple[
        BaseStorage,
        StorageTransaction,
        Callable[..., sx.Scheduler[sx.PeriodicSchedule]],
        Callable[..., sx.TransactionalScheduler[Any, sx.PeriodicSchedule]],
    ],
) -> BaseStorage:
    return test_bundle[0]


@pytest.fixture
def storage_transaction(
    test_bundle: tuple[
        BaseStorage,
        StorageTransaction,
        Callable[..., sx.Scheduler[sx.PeriodicSchedule]],
        Callable[..., sx.TransactionalScheduler[Any, sx.PeriodicSchedule]],
    ],
) -> StorageTransaction[Any]:
    return test_bundle[1]


@pytest.fixture
def scheduler_builder(
    test_bundle: tuple[
        BaseStorage,
        StorageTransaction,
        Callable[..., sx.Scheduler[sx.PeriodicSchedule]],
        Callable[..., sx.TransactionalScheduler[Any, sx.PeriodicSchedule]],
    ],
) -> Callable[..., sx.Scheduler[sx.PeriodicSchedule]]:
    return test_bundle[2]


@pytest.fixture
def scheduler(
    test_bundle: tuple[
        BaseStorage,
        StorageTransaction,
        Callable[..., sx.Scheduler[sx.PeriodicSchedule]],
        Callable[..., sx.TransactionalScheduler[Any, sx.PeriodicSchedule]],
    ],
) -> sx.Scheduler[sx.PeriodicSchedule]:
    return test_bundle[2]()


@pytest.fixture
def transactional_scheduler_builder(
    test_bundle: tuple[
        BaseStorage,
        StorageTransaction,
        Callable[..., sx.Scheduler[sx.PeriodicSchedule]],
        Callable[..., sx.TransactionalScheduler[Any, sx.PeriodicSchedule]],
    ],
) -> Callable[..., sx.TransactionalScheduler[Any, sx.PeriodicSchedule]]:
    return test_bundle[3]


@pytest.fixture
def transactional_scheduler(
    test_bundle: tuple[
        BaseStorage,
        StorageTransaction,
        Callable[..., sx.Scheduler[sx.PeriodicSchedule]],
        Callable[..., sx.TransactionalScheduler[Any, sx.PeriodicSchedule]],
    ],
) -> sx.TransactionalScheduler[Any, sx.PeriodicSchedule]:
    return test_bundle[3]()


@pytest.mark.time_machine("2026-10-10 22:30 +0500", tick=False)
async def test_scheduler_add_job(
    stored_job_factory: type[StoredJobFactory],
    job_storage: BaseStorage,
    scheduler: sx.Scheduler[sx.PeriodicSchedule],
) -> None:
    job = JobFactory.build(executor_tag="gpu")
    job_id = await scheduler.add_job(job)

    assert job_id == job.id

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
            executor_tag="gpu",
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
    stored_jobs = stored_job_factory.batch(size=3)
    for job in stored_jobs:
        await job_storage.insert(dc.asdict(job))

    cancelled_job, untouched_jobs = stored_jobs[0], stored_jobs[1:]

    await scheduler.cancel_job(cancelled_job.id)
    await job_storage.assert_has_exactly({}, count=2)
    for job in untouched_jobs:
        await job_storage.assert_has_exactly(dict(id=job.id), count=1)


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
            executor_tag=None,
        )
    )

    job = await scheduler.get_job(job.id)
    assert job is not None

    assert job.id == job.id
    assert job.schedule == job.schedule
    assert job.meta == job.meta


@pytest.mark.time_machine("2026-10-10 22:30 +0500", tick=False)
async def test_transactional_scheduler_add_job_succeeded(
    stored_job_factory: type[StoredJobFactory],
    job_storage: BaseStorage,
    transactional_scheduler: sx.TransactionalScheduler[Any, sx.PeriodicSchedule],
) -> None:
    job = JobFactory.build(executor_tag="gpu")

    async with transactional_scheduler.transactional() as tx:
        job_id = await tx.add_job(job)

    assert job_id == job.id

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
            executor_tag="gpu",
            acquired_by=None,
            acquired_until=None,
        ),
        count=1,
    )


@pytest.mark.time_machine("2026-10-10 22:30 +0500", tick=False)
async def test_transactional_scheduler_add_job_failed(
    stored_job_factory: type[StoredJobFactory],
    job_storage: BaseStorage,
    transactional_scheduler: sx.TransactionalScheduler[Any, sx.PeriodicSchedule],
) -> None:
    job = JobFactory.build()

    with pytest.raises(CustomException):
        async with transactional_scheduler.transactional() as tx:
            job_id = await tx.add_job(job)
            raise CustomException()

    assert job_id == job.id

    await job_storage.assert_has_exactly({}, count=0)


async def test_transactional_scheduler_add_job_duplicate(
    stored_job_factory: type[StoredJobFactory],
    job_storage: BaseStorage,
    transactional_scheduler: sx.TransactionalScheduler[Any, sx.PeriodicSchedule],
) -> None:
    job = JobFactory.build()

    with pytest.raises(sx.JobAlreadyExists):
        async with transactional_scheduler.transactional() as tx:
            await tx.add_job(job)
            await tx.add_job(job)

    await job_storage.assert_has_exactly({}, count=0)


async def test_transactional_scheduler_cancel_job_succeeded(
    stored_job_factory: type[StoredJobFactory],
    job_storage: BaseStorage,
    transactional_scheduler: sx.TransactionalScheduler[Any, sx.PeriodicSchedule],
) -> None:
    stored_jobs = stored_job_factory.batch(size=3)
    for job in stored_jobs:
        await job_storage.insert(dc.asdict(job))

    cancelled_job, untouched_jobs = stored_jobs[0], stored_jobs[1:]

    async with transactional_scheduler.transactional() as tx:
        await tx.cancel_job(cancelled_job.id)

    await job_storage.assert_has_exactly({}, count=2)
    for job in untouched_jobs:
        await job_storage.assert_has_exactly(dict(id=job.id), count=1)


async def test_transactional_scheduler_cancel_job_failed(
    stored_job_factory: type[StoredJobFactory],
    job_storage: BaseStorage,
    transactional_scheduler: sx.TransactionalScheduler[Any, sx.PeriodicSchedule],
) -> None:
    job = stored_job_factory.build()
    await job_storage.insert(dc.asdict(job))

    with pytest.raises(CustomException):
        async with transactional_scheduler.transactional() as tx:
            await tx.cancel_job(job.id)
            raise CustomException()

    await job_storage.assert_has_exactly({}, count=1)


@pytest.mark.time_machine("2026-10-10 22:30 +0500", tick=False)
async def test_transactional_scheduler_outer_transaction_commit(
    stored_job_factory: type[StoredJobFactory],
    job_storage: BaseStorage,
    storage_transaction: StorageTransaction[Any],
    transactional_scheduler: sx.TransactionalScheduler[Any, sx.PeriodicSchedule],
) -> None:
    job = JobFactory.build()

    outer_tx = await storage_transaction.begin()
    tx = transactional_scheduler.within_transaction(outer_tx)
    job_id = await tx.add_job(job)
    assert job_id == job.id

    await storage_transaction.commit()

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
            executor_tag=None,
            acquired_by=None,
            acquired_until=None,
        ),
        count=1,
    )


async def test_transactional_scheduler_outer_transaction_rollback(
    stored_job_factory: type[StoredJobFactory],
    job_storage: BaseStorage,
    storage_transaction: StorageTransaction[Any],
    transactional_scheduler: sx.TransactionalScheduler[Any, sx.PeriodicSchedule],
) -> None:
    job = JobFactory.build()

    outer_tx = await storage_transaction.begin()
    tx = transactional_scheduler.within_transaction(outer_tx)
    job_id = await tx.add_job(job)
    assert job_id == job.id

    await storage_transaction.rollback()

    await job_storage.assert_has_exactly({}, count=0)


@pytest.mark.time_machine("2026-10-10 22:30 +0500", tick=False)
async def test_scheduler_middleware(
    job_storage: BaseStorage,
    scheduler_builder: Callable[..., sx.Scheduler[sx.PeriodicSchedule]],
) -> None:
    middleware_job = None

    async def test_middleware(
        job: sx.Job[sx.PeriodicSchedule],
        /,
        *,
        wrapped: sx.SchedulerMiddlewareWrappedFunc[sx.PeriodicSchedule],
    ) -> Optional[str]:
        nonlocal middleware_job
        middleware_job = job
        return await wrapped(job)

    scheduler = scheduler_builder(middlewares=[test_middleware])

    job = JobFactory.build()
    job_id = await scheduler.add_job(job)

    assert job_id == job.id
    assert middleware_job is job

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
            executor_tag=None,
            acquired_by=None,
            acquired_until=None,
        ),
        count=1,
    )


@pytest.mark.time_machine("2026-10-10 22:30 +0500", tick=False)
async def test_transactional_scheduler_middleware(
    job_storage: BaseStorage,
    transactional_scheduler_builder: Callable[..., sx.TransactionalScheduler[Any, sx.PeriodicSchedule]],
) -> None:
    middleware_job = None

    async def test_middleware(
        job: sx.Job[sx.PeriodicSchedule],
        /,
        *,
        wrapped: sx.SchedulerMiddlewareWrappedFunc[sx.PeriodicSchedule],
    ) -> Optional[str]:
        nonlocal middleware_job
        middleware_job = job
        return await wrapped(job)

    scheduler = transactional_scheduler_builder(middlewares=[test_middleware])

    job = JobFactory.build()
    async with scheduler.transactional() as tx:
        job_id = await tx.add_job(job)

    assert job_id == job.id
    assert middleware_job is job

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
            executor_tag=None,
            acquired_by=None,
            acquired_until=None,
        ),
        count=1,
    )


@pytest.mark.time_machine("2026-10-10 22:30 +0500", tick=False)
async def test_scheduler_middleware_fail(
    job_storage: BaseStorage,
    scheduler_builder: Callable[..., sx.Scheduler[sx.PeriodicSchedule]],
) -> None:
    async def test_middleware(
        job: sx.Job[sx.PeriodicSchedule],
        /,
        *,
        wrapped: sx.SchedulerMiddlewareWrappedFunc[sx.PeriodicSchedule],
    ) -> Optional[str]:
        raise CustomException()

    scheduler = scheduler_builder(middlewares=[test_middleware])

    job = JobFactory.build()
    with pytest.raises(CustomException):
        await scheduler.add_job(job)

    await job_storage.assert_has_exactly({}, count=0)


@pytest.mark.time_machine("2026-10-10 22:30 +0500", tick=False)
async def test_scheduler_add_job_events(
    stored_job_factory: type[StoredJobFactory],
    scheduler_builder: Callable[..., sx.Scheduler[sx.PeriodicSchedule]],
) -> None:
    event_sender_mock = mk.MagicMock(spec=sx.EventSender)

    scheduler = scheduler_builder(event_sender=event_sender_mock)

    job = JobFactory.build()
    await scheduler.add_job(job)

    event_sender_mock.send.assert_called_with(
        sx.Event(
            kind=sx.EventKind.JobReady,
            timestamp=dt.datetime.now(dt.timezone.utc),
            id=mk.ANY,
        )
    )


@pytest.mark.time_machine("2026-10-10 22:30 +0500", tick=False)
async def test_scheduler_cancel_job_events(
    stored_job_factory: type[StoredJobFactory],
    job_storage: BaseStorage,
    scheduler_builder: Callable[..., sx.Scheduler[sx.PeriodicSchedule]],
) -> None:
    event_sender_mock = mk.MagicMock(spec=sx.EventSender)

    scheduler = scheduler_builder(event_sender=event_sender_mock)

    job = stored_job_factory.build()
    await job_storage.insert(dc.asdict(job))
    await scheduler.cancel_job(job.id)

    event_sender_mock.send.assert_called_with(
        sx.Event(
            kind=sx.EventKind.JobCanceled,
            timestamp=dt.datetime.now(dt.timezone.utc),
            id=mk.ANY,
        )
    )
