import asyncio as aio
import random as rnd

import pytest

from schedex.runtime.iterator import merge_iterators

pytestmark = pytest.mark.unit


class CustomException(Exception):
    pass


@pytest.mark.no_leaks
@pytest.mark.timeout(1.0)
async def test_merge_iterators(wait_delay: float) -> None:
    async def async_iterator(queue: aio.Queue):
        while True:
            yield await queue.get()

    iter_count = 2
    iterations = iter_count * 2
    queues = [aio.Queue() for _ in range(iter_count)]
    iterators = [async_iterator(queue) for queue in queues]

    iteration = 0
    rnd.choice(queues).put_nowait(iteration)
    async with merge_iterators(*iterators) as merged:
        async for actual_result in merged:
            assert actual_result == iteration
            if iteration == iterations:
                break

            iteration += 1
            rnd.choice(queues).put_nowait(iteration)

    for iterator in iterators:
        with pytest.raises(StopAsyncIteration):
            await anext(iterator)
