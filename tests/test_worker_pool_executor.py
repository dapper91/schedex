import asyncio as aio
import datetime as dt
import unittest.mock as mk
from typing import Any

import msgspec.json
import pytest

import schedex as sx
from schedex.executor.worker_pool import WorkerPool, WorkerPoolExecutor
from tests.factories import StoredTaskFactory

pytestmark = pytest.mark.unit


async def test_worker_pool_acquire():
    called = aio.Event()

    async def task():
        called.set()

    async with WorkerPool(max_workers=1) as pool:
        worker = await pool.acquire()
        assert pool.size == 1
        worker.submit(task())

    assert called.is_set()


async def test_worker_pool_max_size():
    async with WorkerPool(max_workers=1) as pool:
        worker = await pool.acquire()
        with pytest.raises(aio.TimeoutError):
            await aio.wait_for(pool.acquire(), timeout=0)
        pool.release(worker)
        assert pool.size == 0

        worker = await pool.acquire()
        pool.release(worker)
        assert pool.size == 0


@pytest.mark.no_leaks
@pytest.mark.timeout(1.0)
async def test_worker_pool_executor(wait_delay: float, stored_task_factory: type[StoredTaskFactory]) -> None:
    event_sender, event_receiver = sx.mem.eventbus.build()

    app_state = object()
    stored_task = stored_task_factory.pending().build(
        task_name="task_name",
        task_args=msgspec.json.encode(
            {
                "name": "task_name",
                "args": [1],
                "kwargs": {"string": "text"},
            }
        ),
    )
    # TODO: use in-memory lock manager
    task_lock_mock = mk.MagicMock(spec=sx.TaskLock)
    type(task_lock_mock).task = mk.PropertyMock(return_value=stored_task)

    lock_manager = mk.MagicMock(spec=sx.LockManager)
    lock_manager.lock_next_task.return_value = (task_lock_mock, dt.timedelta.max)

    registry = sx.TaskRegistry()
    task_call = aio.Future()

    @registry.task(name="task_name")
    async def task(context: sx.Context[Any, Any], number: int, /, *, string: str) -> None:
        task_call.set_result((context, number, string))

    executor = WorkerPoolExecutor(
        state=app_state,
        max_workers=1,
        polling_interval=dt.timedelta(seconds=1),
        schedule_type=sx.PeriodicSchedule,
        task_registry=registry,
        lock_manager=lock_manager,
        event_sender=event_sender,
        event_receiver=event_receiver,
    )

    async with aio.timeout(wait_delay):
        await executor.run()

    task_args = await task_call
    task_context, number_arg, string_kwarg = task_args

    assert task_context.state is app_state
    assert task_context.lock is task_lock_mock
    assert task_context.task == sx.TaskInfo(
        id=stored_task.id,
        job_id=stored_task.job_id,
        sequence_number=stored_task.sequence_number,
        created_at=stored_task.created_at,
        attempts=stored_task.attempts,
        meta=msgspec.json.decode(stored_task.meta),
    )
    assert number_arg == 1
    assert string_kwarg == "text"
