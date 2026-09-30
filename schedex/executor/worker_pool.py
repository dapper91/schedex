import asyncio as aio
import contextlib as cl
import datetime as dt
import functools as ft
import logging
from types import TracebackType
from typing import Any, Coroutine, Iterable, Optional, Protocol

from schedex.context import Context, TaskInfo
from schedex.eventbus import Event, EventKind, EventReceiver, EventSender
from schedex.guard import ErrorGuard, RetryPolicy, exponential_delay
from schedex.lock import JobLock, LockManager, TaskLock
from schedex.metadata import metadata_decoder
from schedex.runtime import first, second, select, third
from schedex.schedule import Schedule
from schedex.task import Task, TaskRegistry

from .task_fetcher import TaskFetcher

logger = logging.getLogger(__name__)


class Worker:
    """
    Pool worker.
    """

    def __init__(self, pool: "WorkerPool"):
        self._pool = pool
        self._task: Optional[aio.Task[None]] = None

    def release(self) -> None:
        """
        Releases the worker detaching it from the pool.
        """

        self._pool.release(self)

    def submit(self, coro: Coroutine[Any, Any, None]) -> None:
        """
        Executes a coroutine in a worker.
        """

        if self._task is not None:
            raise RuntimeError("worker is busy")

        self._task = task = aio.create_task(coro)
        task.add_done_callback(self._on_task_done)

    async def wait(self) -> None:
        if self._task is not None:
            await self._task

    def cancel(self) -> None:
        if self._task is not None:
            self._task.cancel()

    def _on_task_done(self, task: aio.Task[None]) -> None:
        try:
            task.result()
        except Exception as e:
            logger.exception("worker task failed: %s", e)
        except aio.CancelledError:
            pass

        self._task = None
        self.release()


class WorkerPool(cl.AbstractAsyncContextManager["WorkerPool"]):
    """
    A pool of limited number of workers. On distraction waits until all the workers finished their job.
    """

    def __init__(self, max_workers: int, shutdown_timeout: float = 60.0):
        self._max_workers = max_workers
        self._shutdown_timeout = shutdown_timeout
        self._workers: set[Worker] = set()
        self._worker_released_event = aio.Event()

    @property
    def size(self) -> int:
        return len(self._workers)

    @property
    def max_workers(self) -> int:
        """
        Returns the maximum number of workers.
        """

        return self._max_workers

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> bool:
        try:
            async with aio.timeout(self._shutdown_timeout):
                for worker in list(self._workers):
                    try:
                        await worker.wait()
                    except BaseException:
                        pass

        except aio.TimeoutError:
            for worker in self._workers:
                worker.cancel()

        return False

    async def acquire(self) -> Worker:
        """
        Returns a worker bound to the pool or waits until any worker finishes its job if the pool is full .
        """

        while len(self._workers) == self._max_workers:
            await self._worker_released_event.wait()
            self._worker_released_event.clear()

        self._workers.add(worker := Worker(self))

        return worker

    def release(self, worker: Worker) -> None:
        """
        Releases a worker.
        """

        self._workers.remove(worker)
        self._worker_released_event.set()


class ExecutorMiddlewareWrappedFunc[CtxT](Protocol):
    """
    Task executor middleware wrapped function.
    """

    async def __call__(self, context: CtxT, task: Task[CtxT, Any, Any], /) -> None:
        pass


class ExecutorMiddleware[CtxT](Protocol):
    """
    Task executor middleware.
    """

    async def __call__(
        self,
        context: CtxT,
        task: Task[CtxT, Any, Any],
        /,
        *,
        wrapped: ExecutorMiddlewareWrappedFunc[CtxT],
    ) -> None:
        """
        Calls the middleware.

        :param context: task context
        :param task: task is being executed
        :param wrapped: function the middleware is wrapping
        :return: wrapped function result
        """


class WorkerPoolExecutor[StT, JlkT: JobLock, TlkT: TaskLock]:
    """
    Worker pool executor.
    """

    def __init__(
        self,
        state: StT,
        max_workers: int,
        polling_interval: dt.timedelta,
        schedule_type: type[Schedule],
        task_registry: TaskRegistry[StT, TlkT],
        lock_manager: LockManager[JlkT, TlkT],
        event_sender: Optional[EventSender] = None,
        event_receiver: Optional[EventReceiver] = None,
        middlewares: Iterable[ExecutorMiddleware[Context[StT, TlkT]]] = (),
        shutdown_timeout: float = 60.0,
        retry_policy: Optional[RetryPolicy] = None,
    ) -> None:
        self._state = state
        self._polling_interval = polling_interval
        self._retry_policy = retry_policy or exponential_delay(
            initial=dt.timedelta(seconds=1),
            maximum=dt.timedelta(seconds=30),
            factor=1.5,
        )
        self._worker_pool: WorkerPool = WorkerPool(max_workers, shutdown_timeout=shutdown_timeout)
        self._task_fetcher = TaskFetcher(polling_interval, lock_manager, schedule_type)
        self._task_registry = task_registry
        self._event_sender = event_sender
        self._event_receiver = event_receiver

        wrapped_process_task = self._process_task
        for middleware in reversed(list(middlewares)):
            wrapped_process_task = ft.partial(middleware, wrapped=wrapped_process_task)

        self._wrapped_process_task = wrapped_process_task

    async def run(self) -> None:
        """
        Runs the executor until it is canceled.
        """

        logger.info("starting executor (%d workers)", self._worker_pool.max_workers)

        if self._event_sender:
            await self._event_sender.send(Event(EventKind.NodeJoined))

        with cl.suppress(aio.CancelledError):
            async with aio.TaskGroup() as tasks:
                task_changed_event = aio.Event()
                tasks.create_task(self._process_tasks(self._task_fetcher, task_changed_event))
                if self._event_receiver:
                    tasks.create_task(self._process_events(self._event_receiver, task_changed_event))

        if self._event_sender:
            await self._event_sender.send(Event(EventKind.NodeLeft))

        logger.info("executor stopped")

    async def _process_tasks(
        self, task_fetcher: TaskFetcher[Schedule, JlkT, TlkT], task_status_changed: aio.Event
    ) -> None:
        logger.info("task processor started")

        with ErrorGuard(self._retry_policy, reset_on_success=True) as guard:
            async with self._worker_pool as worker_pool:
                while ticket := await guard.acquire_async():
                    worker = await worker_pool.acquire()
                    try:
                        with ticket:
                            task_lock, delay = await task_fetcher.fetch_next_task()
                            if task_lock is not None:
                                logger.debug("task '%s' locked", task_lock.task.id)
                                worker.submit(self._execute_task(task_lock))
                            else:
                                match await select(
                                    task_status_changed.wait(),
                                    aio.sleep(delay.total_seconds()),
                                    aio.sleep(self._polling_interval.total_seconds()),
                                ):
                                    case first(_):
                                        task_status_changed.clear()
                                    case second(_):
                                        logger.debug("check delay deadline reached")
                                    case third(_):
                                        logger.debug("polling deadline reached")

                                worker.release()

                    except BaseException:
                        worker.release()
                        raise

    async def _process_events(self, event_receiver: EventReceiver, task_status_changed: aio.Event) -> None:
        logger.info("event processor started")

        with ErrorGuard(self._retry_policy, reset_on_success=True) as guard:
            while ticket := await guard.acquire_async():
                with ticket:
                    async with event_receiver.connect() as event_source:
                        async for event in event_source:
                            logger.debug("processing event %s", event)
                            if event.kind in (
                                EventKind.NodeLeft,  # node may update a task state on shutdown
                                EventKind.JobReady,  # job may spawn a new task
                                EventKind.TaskReady,
                                EventKind.TaskFailed,
                            ):
                                task_status_changed.set()

                            guard.reset()

    async def _execute_task(self, task_lock: TlkT) -> None:
        async with task_lock:
            context = Context(
                state=self._state,
                lock=task_lock,
                task=TaskInfo(
                    id=task_lock.task.id,
                    job_id=task_lock.task.job_id,
                    sequence_number=task_lock.task.sequence_number,
                    created_at=task_lock.task.created_at,
                    attempts=task_lock.task.attempts,
                    meta=metadata_decoder.decode(task_lock.task.meta),
                ),
            )
            task_name = context.lock.task.task_name
            if task := self._task_registry.get(task_name):
                await self._wrapped_process_task(context, task)
            else:
                raise RuntimeError(f"task {task_name} not found")

    async def _process_task(self, context: Context[StT, TlkT], task: Task[Context[StT, TlkT], Any, Any]) -> None:
        logger.debug("processing task '%s'", task.name)

        bound_task = task.bound_task_cls.deserialize(context.lock.task.task_args)
        await task(context, *bound_task.args, **bound_task.kwargs)
