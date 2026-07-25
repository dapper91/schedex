import asyncio as aio
import dataclasses as dc
import logging
import typing
from typing import Any, Coroutine, Iterable, Union

logger = logging.getLogger(__name__)

MAX_ARGS = 5


@dc.dataclass(slots=True, frozen=True)
class param[R]:
    __match_args__ = ("result",)

    result: R


class first[R](param[R]):
    pass


class second[R](param[R]):
    pass


class third[R](param[R]):
    pass


class fourth[R](param[R]):
    pass


class fifth[R](param[R]):
    pass


type Selectable[R] = Union[aio.Future[R], Coroutine[None, None, R]]


@typing.overload
async def select[R1, R2](
    s1: Selectable[R1],
    s2: Selectable[R2],
    /,
    *,
    cancel_pending: bool = True,
) -> Union[first[R1], second[R2]]: ...


@typing.overload
async def select[R1, R2, R3](
    s1: Selectable[R1],
    s2: Selectable[R2],
    s3: Selectable[R3],
    /,
    *,
    cancel_pending: bool = True,
) -> Union[first[R1], second[R2], third[R3]]: ...


@typing.overload
async def select[R1, R2, R3, R4](
    s1: Selectable[R1],
    s2: Selectable[R2],
    s3: Selectable[R3],
    s4: Selectable[R4],
    /,
    *,
    cancel_pending: bool = True,
) -> Union[first[R1], second[R2], third[R3], fourth[R4]]: ...


@typing.overload
async def select[R1, R2, R3, R4, R5](
    s1: Selectable[R1],
    s2: Selectable[R2],
    s3: Selectable[R3],
    s4: Selectable[R4],
    s5: Selectable[R5],
    /,
    *,
    cancel_pending: bool = True,
) -> Union[first[R1], second[R2], third[R3], fourth[R4], fifth[R5]]: ...


async def select(*selectable: Selectable[Any], cancel_pending: bool = True) -> Any:
    """
    Waits for the first coroutine to complete. The other coroutines are canceled.

    :param selectable: awaitable objects to be waited
    :param cancel_pending: whether to cancel pending futures
    :return: the first completed coroutine result
    """

    if len(selectable) > MAX_ARGS:
        raise AssertionError(f"number of parameters must not exceed {MAX_ARGS}")

    param_types = (first, second, third, fourth, fifth)
    assert len(param_types) == MAX_ARGS

    if not cancel_pending and any((not isinstance(obj, aio.Future) for obj in selectable)):
        raise TypeError("cancel_pending with non-future selectable may lead to resource leak")

    futures = [obj if isinstance(obj, aio.Future) else aio.Task(obj) for obj in selectable]

    try:
        done_futures, pending_futures = await aio.wait(futures, return_when=aio.FIRST_COMPLETED)
    except BaseException:
        await _cancel_futures(futures, reraise=True)
        raise

    if cancel_pending:
        await _cancel_futures(pending_futures, reraise=False)

    for done_future in done_futures:
        for Param, future in zip(param_types, futures, strict=False):
            if done_future is future:
                return Param(done_future.result())
    else:
        raise AssertionError("unreachable")


async def _cancel_futures(futures: Iterable[aio.Future[Any]], reraise: bool = True) -> None:
    future_exceptions: list[BaseException] = []
    for future in futures:
        future.cancel()
        try:
            await future
        except aio.CancelledError:
            pass
        except BaseException as e:
            future_exceptions.append(e)

    if future_exceptions and reraise:
        raise BaseExceptionGroup("selector future cancellation failed", future_exceptions)

    for exception in future_exceptions:
        logger.warning("future cancellation failed: %s", exception)
