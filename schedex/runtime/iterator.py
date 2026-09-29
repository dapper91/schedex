import asyncio as aio
from typing import AsyncGenerator, AsyncIterator


async def wait_next[T](it: AsyncIterator[T]) -> T:
    return await anext(it)


async def merge_iterators[T](*iterators: AsyncIterator[T]) -> AsyncGenerator[T, None]:
    iterators = list(iterators)

    tasks = list(aio.create_task(wait_next(it)) for it in iterators)
    try:
        while True:
            done_tasks, pending_tasks = await aio.wait(tasks, return_when=aio.FIRST_COMPLETED)
            for task in done_tasks:
                task_idx = tasks.index(task)
                tasks[task_idx] = aio.create_task(wait_next(iterators[task_idx]))
                yield await task

    finally:
        exceptions: list[BaseException] = []
        for task in tasks:
            task.cancel()
        for task in tasks:
            try:
                await task
            except (aio.CancelledError, StopAsyncIteration, KeyboardInterrupt):
                pass
            except BaseException as e:
                exceptions.append(e)

        if exceptions:
            raise BaseExceptionGroup("merge iterator error", exceptions)
