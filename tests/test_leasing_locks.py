import dataclasses as dc
import datetime as dt
from typing import Any, Callable

import pytest
import time_machine as tm
from pytest_lazy_fixtures import lf

import schedex as sx
from tests.factories import StoredTaskFactory
from tests.types import BaseStorage


@pytest.fixture(
    params=[
        pytest.param(
            (
                lf("sa_leasing_task_storage"),
                lf("sa_leasing_lock_manager_builder"),
            ),
            id="sqlalchemy-leasing",
        ),
        pytest.param(
            (
                lf("pymongo_task_storage"),
                lf("pymongo_lock_manager_builder"),
            ),
            id="mongodb",
        ),
    ],
)
async def test_bundle(
    request: pytest.FixtureRequest,
) -> tuple[BaseStorage, Callable[..., sx.LockManager[Any, Any]]]:
    task_storage: BaseStorage = request.param[0]
    lock_manager_builder: Callable[..., sx.LockManager[Any, Any]] = request.param[1]
    return task_storage, lock_manager_builder


@pytest.fixture
def task_storage(test_bundle: tuple[BaseStorage, Callable[..., sx.LockManager[Any, Any]]]) -> BaseStorage:
    return test_bundle[0]


@pytest.fixture
def lock_manager_builder(
    test_bundle: tuple[BaseStorage, Callable[..., sx.LockManager[Any, Any]]],
) -> Callable[..., sx.LockManager[Any, Any]]:
    return test_bundle[1]


@pytest.mark.time_machine("2026-09-06 12:00 +0000", tick=False)
async def test_leasing_task_lock_lease_expired(
    time_machine: tm.TimeMachineFixture,
    lock_id: str,
    lock_period: dt.timedelta,
    stored_task_factory: type[StoredTaskFactory],
    task_storage: BaseStorage,
    lock_manager_builder: Callable[..., sx.LockManager[Any, Any]],
) -> None:
    now = dt.datetime.now(dt.timezone.utc)

    lock_manager = lock_manager_builder(period=lock_period, identifier=lock_id)

    stored_task = stored_task_factory.executing(acquired_by=lock_id, acquired_until=now + lock_period).build()
    await task_storage.insert(dc.asdict(stored_task))

    lock, delay = await lock_manager.lock_next_task()
    assert lock is None

    time_machine.shift(lock_period)
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
            acquired_by=lock_id,
            acquired_until=now + lock_period,
            executor_tag=None,
        ),
        1,
    )


@pytest.mark.time_machine("2026-09-06 12:00 +0000", tick=False)
async def test_leasing_task_lock_extend(
    time_machine: tm.TimeMachineFixture,
    lock_id: str,
    lock_period: dt.timedelta,
    stored_task_factory: type[StoredTaskFactory],
    task_storage: BaseStorage,
    lock_manager_builder: Callable[..., sx.LockManager[Any, Any]],
) -> None:
    now = dt.datetime.now(dt.timezone.utc)

    lock_manager = lock_manager_builder(period=lock_period, identifier=lock_id)

    stored_task = stored_task_factory.pending().build()
    await task_storage.insert(dc.asdict(stored_task))

    lock, delay = await lock_manager.lock_next_task()
    assert lock is not None
    assert await lock.is_locked() is True
    assert await lock.remain() == lock_period

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
            acquired_by=lock_id,
            acquired_until=now + lock_period,
            executor_tag=None,
        ),
        1,
    )

    time_machine.shift(lock_period / 2)
    now = dt.datetime.now(dt.timezone.utc)

    assert await lock.is_locked() is True
    assert await lock.remain() == lock_period / 2

    result = await lock.extend(lock_period)
    assert result is True
    assert await lock.is_locked() is True
    assert await lock.remain() == lock_period

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
            acquired_by=lock_id,
            acquired_until=now + lock_period,
            executor_tag=None,
        ),
        1,
    )

    time_machine.shift(lock_period)
    assert await lock.is_locked() is False
    assert await lock.remain() == dt.timedelta(0)
