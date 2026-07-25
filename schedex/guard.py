import asyncio as aio
import datetime as dt
import logging as log
import time
from collections import deque
from types import TracebackType
from typing import Awaitable, Callable, Generator, Optional, Self

RetryPolicy = Callable[[], Generator[float, None, None]]


def constant_delay(
    delay: dt.timedelta,
    max_count: Optional[int] = None,
) -> Callable[[], Generator[float, None, None]]:
    """
    Constant delay policy builder.

    :param delay: delay between attempts
    :param max_count: max attempt count
    :return: delay generator
    """

    def generator() -> Generator[float, None, None]:
        count = 0
        while max_count is None or count < max_count:
            yield delay.total_seconds()
            count += 1

    return generator


def exponential_delay(
    initial: dt.timedelta,
    factor: float,
    minimum: dt.timedelta = dt.timedelta(0),
    maximum: Optional[dt.timedelta] = None,
    max_count: Optional[int] = None,
) -> Callable[[], Generator[float, None, None]]:
    """
    Constant delay policy builder.

    :param initial: initial delay
    :param factor: delay multiplication factor
    :param minimum: min delay value
    :param maximum: max delay value
    :param max_count: max attempt count
    :return: delay generator
    """

    def generator() -> Generator[float, None, None]:
        initial_delay = initial.total_seconds()
        min_delay = minimum.total_seconds()
        max_delay = maximum.total_seconds() if maximum is not None else float("Inf")
        count = 0
        while max_count is None or count < max_count:
            yield max(min(initial_delay * factor**count, max_delay), min_delay)
            count += 1

    return generator


class GuardExhausted(Exception):
    """
    Guard max attempts exceeded error.
    """


class ErrorGuard:
    """
    Error guard.

    :param retry_policy: delay policy builder
    :param retry_on: a function that decides whether to retry or not
    :param reset_on_success: reset delay generator on success
    :param waiter: synchronous waiter
    :param async_waiter: asynchronous waiter
    :param logger: logger
    :param raise_last_errors: max last exceptions to hold for group reraise
    :param stack_info: log stack info
    """

    class Ticket:
        """
        Error guard ticket.
        """

        def __init__(self, guard: "ErrorGuard"):
            self._guard = guard

        def __enter__(self) -> Self:
            return self

        def __exit__(
            self,
            exc_type: Optional[type[BaseException]],
            exc_value: Optional[BaseException],
            traceback: Optional[TracebackType],
        ) -> Optional[bool]:
            if exc_value is None:
                return self._guard._on_success()
            else:
                return self._guard._on_exception(exc_value)

    def __init__(
        self,
        retry_policy: Callable[[], Generator[float, None, None]],
        retry_on: Callable[[BaseException], bool] = lambda exc: isinstance(exc, Exception),
        reset_on_success: bool = False,
        waiter: Callable[[float], None] = time.sleep,
        async_waiter: Callable[[float], Awaitable[None]] = aio.sleep,
        logger: Optional[log.Logger] = None,
        raise_last_errors: int = 1,
        stack_info: bool = False,
    ):
        self._retry_policy = retry_policy
        self._retry_on = retry_on
        self._reset_on_success = reset_on_success
        self._waiter = waiter
        self._async_waiter = async_waiter
        self._logger = logger or log.getLogger(__name__)
        self._raise_last_errors = raise_last_errors
        self._last_exceptions = deque[BaseException](maxlen=raise_last_errors)
        self._stack_info = stack_info
        self._delay_gen: Optional[Generator[float, None, None]] = None

    def __enter__(self) -> Self:
        self.reset()
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Optional[bool]:
        if exc_type is GuardExhausted:
            if not self._last_exceptions:
                return True

            self._logger.error("last attempt failed: %s", self._last_exceptions[-1], stack_info=self._stack_info)

            if self._raise_last_errors == 0:
                return True
            elif self._raise_last_errors == 1:
                raise self._last_exceptions[-1] from None
            else:
                raise BaseExceptionGroup("error guard exceptions", list(self._last_exceptions)) from None

        else:
            return False

    async def acquire_async(self) -> Ticket:
        """
        Acquire a ticket asynchronously.

        :return: guard ticket
        """

        if self._delay_gen:
            try:
                delay = next(self._delay_gen)
                self._logger.warning("sleeping for %.2f seconds", delay)
                await self._async_waiter(delay)
            except StopIteration:
                raise GuardExhausted() from None

        return ErrorGuard.Ticket(self)

    def acquire(self) -> Ticket:
        """
        Acquire a ticket.

        :return: guard ticket
        """

        if self._delay_gen:
            try:
                delay = next(self._delay_gen)
                self._logger.warning("sleeping for %.2f seconds", delay)
                self._waiter(delay)
            except StopIteration:
                raise GuardExhausted() from None

        return ErrorGuard.Ticket(self)

    def reset(self) -> None:
        """
        Reset guard statistics.
        """

        self._delay_gen = None

    def _on_exception(self, exc: BaseException) -> bool:
        if not self._retry_on(exc):
            return False

        if self._delay_gen is None:
            self._delay_gen = self._retry_policy()

        self._logger.error("error occurred: %s", exc, stack_info=self._stack_info)
        self._last_exceptions.append(exc)

        return True

    def _on_success(self) -> bool:
        if self._reset_on_success:
            self.reset()

        return True
