import asyncio as aio
import contextlib as cl
import inspect
from typing import AsyncGenerator, AsyncIterator


async def wait_next[T](it: AsyncIterator[T]) -> T:
    return await anext(it)


@cl.asynccontextmanager
async def merge_iterators[T](
    *iterators: AsyncIterator[T], close_generators: bool = True
) -> AsyncGenerator[AsyncIterator[T], None]:
    async def merge(iterators_list: list[AsyncIterator[T]]) -> AsyncGenerator[T, None]:
        tasks = list(aio.create_task(wait_next(it)) for it in iterators_list)
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

    merge_gen = merge(list(iterators))
    try:
        yield merge_gen
    finally:
        await merge_gen.aclose()

        if close_generators:
            for iterator in iterators:
                if inspect.isasyncgen(iterator):
                    try:
                        await iterator.aclose()
                    except GeneratorExit:
                        pass
