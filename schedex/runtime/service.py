import asyncio as aio
import contextlib as cl
import logging
from types import TracebackType
from typing import Any, Coroutine, Optional

logger = logging.getLogger(__name__)


class Services(cl.AbstractAsyncContextManager["Services"]):
    """
    A group of background services that are automatically canceled on exit.
    """

    def __init__(self) -> None:
        self._service_tasks: set[aio.Task[None]] = set()

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Optional[bool]:
        for task in self._service_tasks:
            task.cancel()

        for task in self._service_tasks:
            try:
                await task
            except aio.CancelledError:
                pass
            except BaseException as exc:
                logger.exception("service execution failed: %s", exc)

        return False

    def launch(self, coro: Coroutine[Any, Any, None]) -> None:
        """
        Launch a background service.

        :param coro: a service function
        """

        task = aio.create_task(coro)
        self._service_tasks.add(task)
