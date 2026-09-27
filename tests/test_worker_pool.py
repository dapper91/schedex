import asyncio as aio

import pytest

from schedex.executor.worker_pool import WorkerPool

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
