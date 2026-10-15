import datetime as dt
from typing import Optional
from unittest import mock as mk

import pytest

from schedex.guard import ErrorGuard, constant_delay, exponential_delay

pytestmark = pytest.mark.unit


class CustomException(Exception):
    pass


@pytest.mark.parametrize(
    "delay, max_count, result",
    [
        (dt.timedelta(seconds=1), 2, [1.0, 1.0]),
        (dt.timedelta(seconds=2, milliseconds=500), 3, [2.5, 2.5, 2.5]),
    ],
)
def test_constant_delay(delay: dt.timedelta, max_count: int, result: list[float]) -> None:
    assert result == list(constant_delay(delay, max_count)())


@pytest.mark.parametrize(
    "initial, factor, minimum, maximum, max_count, result",
    [
        (dt.timedelta(seconds=1), 2, dt.timedelta(seconds=0), None, 4, [1.0, 2.0, 4.0, 8.0]),
        (dt.timedelta(seconds=1), 2, dt.timedelta(seconds=2), None, 4, [2.0, 2.0, 4.0, 8.0]),
        (dt.timedelta(seconds=1), 2, dt.timedelta(seconds=0), dt.timedelta(seconds=3), 5, [1.0, 2.0, 3.0, 3.0, 3.0]),
        (dt.timedelta(seconds=2), 2, dt.timedelta(seconds=0), None, 3, [2.0, 4.0, 8.0]),
        (dt.timedelta(seconds=2), 1, dt.timedelta(seconds=0), None, 3, [2.0, 2.0, 2.0]),
        (dt.timedelta(seconds=4), 0.5, dt.timedelta(seconds=0), None, 3, [4.0, 2.0, 1.0]),
    ],
)
def test_exponential_delay(
    initial: dt.timedelta,
    factor: float,
    minimum: dt.timedelta,
    maximum: Optional[dt.timedelta],
    max_count: int,
    result: list[float],
) -> None:
    assert result == list(exponential_delay(initial, factor, minimum, maximum, max_count)())


@pytest.mark.no_leaks
@pytest.mark.parametrize(
    "initial, factor, max_count, result",
    [
        (dt.timedelta(seconds=1), 2.0, 3, [mk.call(1.0), mk.call(2.0), mk.call(4.0)]),
        (dt.timedelta(seconds=2), 1.0, 2, [mk.call(2.0), mk.call(2.0)]),
    ],
)
@pytest.mark.timeout(1.0)
async def test_guard_policy(initial: dt.timedelta, factor: float, max_count: int, result: list[mk._Call]) -> None:
    waiter = mk.AsyncMock()

    with pytest.raises(CustomException):
        with ErrorGuard(
            retry_policy=exponential_delay(initial=initial, factor=factor, max_count=max_count),
            async_waiter=waiter,
        ) as guard:
            while ticket := await guard.acquire_async():
                with ticket:
                    raise CustomException()

    waiter.assert_has_calls(result)


@pytest.mark.no_leaks
@pytest.mark.parametrize(
    "reset_on_success, initial, factor, max_count, success_on, result",
    [
        (True, dt.timedelta(seconds=1), 2.0, 3, 2, [mk.call(1.0), mk.call(2.0), mk.call(1.0)]),
        (False, dt.timedelta(seconds=1), 2.0, 3, 2, [mk.call(1.0), mk.call(2.0), mk.call(4.0)]),
    ],
)
@pytest.mark.timeout(1.0)
async def test_guard_reset_on_success(
    reset_on_success: bool,
    initial: dt.timedelta,
    factor: float,
    max_count: int,
    success_on: int,
    result: list[mk._Call],
) -> None:
    waiter = mk.AsyncMock()

    iteration = 0
    with pytest.raises(CustomException):
        with ErrorGuard(
            retry_policy=exponential_delay(initial=initial, factor=factor, max_count=max_count),
            async_waiter=waiter,
            reset_on_success=reset_on_success,
        ) as guard:
            while ticket := await guard.acquire_async():
                with ticket:
                    if iteration != success_on:
                        raise CustomException()

                iteration += 1

    waiter.assert_has_calls(result)


@pytest.mark.no_leaks
@pytest.mark.parametrize(
    "retriable_exception, result",
    [
        (CustomException, [mk.call(1.0)]),
        (RuntimeError, []),
    ],
)
@pytest.mark.timeout(1.0)
async def test_guard_reset_on_error(retriable_exception: Exception, result: list[mk._Call]) -> None:
    waiter = mk.AsyncMock()

    with pytest.raises(CustomException):
        with ErrorGuard(
            retry_policy=constant_delay(delay=dt.timedelta(seconds=1), max_count=1),
            async_waiter=waiter,
            retry_on=lambda exc: type(exc) is retriable_exception,
        ) as guard:
            while ticket := await guard.acquire_async():
                with ticket:
                    raise CustomException()

    waiter.assert_has_calls(result)
