import asyncio as aio
import random as rnd

import pytest

from schedex.runtime import fifth, first, fourth, second, select, third

pytestmark = pytest.mark.unit


class CustomException(Exception):
    pass


@pytest.mark.no_leaks
@pytest.mark.timeout(1.0)
@pytest.mark.parametrize("cancel_pending", [True, False])
async def test_select(wait_delay: float, cancel_pending: bool) -> None:
    loop = aio.get_running_loop()
    futures = [aio.Future() for _ in range(5)]

    expected_idx = rnd.randint(0, 4)
    expected_result = "result"
    loop.call_later(callback=lambda: futures[expected_idx].set_result(expected_result), delay=wait_delay)

    match await select(*futures, cancel_pending=cancel_pending):
        case first(actual_result):
            actual_idx = 0
        case second(actual_result):
            actual_idx = 1
        case third(actual_result):
            actual_idx = 2
        case fourth(actual_result):
            actual_idx = 3
        case fifth(actual_result):
            actual_idx = 4
        case _:
            raise AssertionError("unreachable")

    assert actual_idx == expected_idx
    assert actual_result == expected_result

    for idx, future in enumerate(futures):
        if idx == expected_idx:
            assert future.result() == expected_result
        else:
            if cancel_pending:
                assert future.cancelled()
            else:
                assert not future.done()


@pytest.mark.no_leaks
@pytest.mark.timeout(1.0)
async def test_select_exception(wait_delay: float) -> None:
    loop = aio.get_running_loop()
    futures = [aio.Future() for _ in range(2)]

    expected_idx = rnd.randint(0, 1)
    loop.call_later(callback=lambda: futures[expected_idx].set_exception(CustomException()), delay=wait_delay)

    with pytest.raises(CustomException):
        match await select(*futures):
            case first(_):
                pass
            case second(_):
                pass
            case _:
                raise AssertionError("unreachable")

    for idx, future in enumerate(futures):
        if idx == expected_idx:
            assert type(future.exception()) is CustomException
        else:
            assert future.cancelled()


@pytest.mark.no_leaks
@pytest.mark.timeout(1.0)
async def test_select_future_cancelled(wait_delay: float) -> None:
    loop = aio.get_running_loop()
    futures = [aio.Future() for _ in range(2)]

    expected_idx = rnd.randint(0, 1)
    loop.call_later(callback=lambda: futures[expected_idx].cancel(), delay=wait_delay)

    with pytest.raises(aio.CancelledError):
        match await select(*futures):
            case first(_):
                pass
            case second(_):
                pass
            case _:
                raise AssertionError("unreachable")

    for future in futures:
        assert future.cancelled()
