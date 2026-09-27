from collections import ChainMap
from typing import Any, Callable, Concatenate, MutableMapping, Never

import dishka
from dishka.integrations.base import wrap_injection

from schedex import Context, ExecutorMiddlewareWrappedFunc, Task

CONTAINER_KEY = "dishka_container"

AppState = MutableMapping[str, Any]


class InjectedType:
    def __getattribute__(self, item: Any) -> Never:
        raise AssertionError("argument is not injected")


Injected: Any = InjectedType()


def inject[StT: AppState, **P, R](
    func: Callable[Concatenate[Context[StT, Any], P], R],
) -> Callable[Concatenate[Context[StT, Any], P], R]:
    """
    Function decorator injecting dependencies into the function arguments.
    """

    return wrap_injection(
        func=func,
        is_async=True,
        container_getter=lambda args, kwargs: args[0].state[CONTAINER_KEY],
    )


async def request_container_middleware(
    context: Context[AppState, Any],
    task: Task[Context[AppState, Any], Any, Any],
    /,
    *,
    wrapped: ExecutorMiddlewareWrappedFunc[Context[AppState, Any]],
) -> None:
    """
    A middleware that initialize request-scoped di container.
    Works only with MutableMapping application state.
    """

    container: dishka.AsyncContainer = context.state[CONTAINER_KEY]

    async with container(scope=dishka.Scope.REQUEST) as request_container:
        request_context = ChainMap({CONTAINER_KEY: request_container}, context.state)
        context = Context(state=request_context, task=context.task, lock=context.lock)
        return await wrapped(context, task)
