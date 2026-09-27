import abc
import inspect
import warnings
from inspect import Parameter
from typing import Any, Awaitable, Callable, Concatenate, NotRequired, Optional, Protocol, TypedDict

from schedex.serializer import MsgSpecSerializable
from schedex.serializer.json import JsonSerializable

from .context import Context
from .lock import TaskLock


class BoundTask(MsgSpecSerializable, abc.ABC):
    """
    Task with bound parameters.

    :param name: task name
    :param args: task positional arguments
    :param kwargs: task keyword arguments
    """

    name: str
    args: tuple[Any, ...]
    kwargs: dict[str, Any]


def extract_func_signature(
    func: Callable[..., Any], skip_args: int = 0
) -> tuple[list[tuple[Any, Any]], dict[str, tuple[Any, Any]]]:
    """
    Extracts function signature.

    :param func: function to extract signature from
    :param skip_args: number of positional arguments to skip
    :return: args/kwargs parameters definition
    """

    args_definition: list[tuple[Any, Any]] = []
    kwargs_definition: dict[str, tuple[Any, Any]] = {}

    for param in list(inspect.signature(func).parameters.values())[skip_args:]:
        if param.annotation is Parameter.empty:
            raise RuntimeError(f"{param.name} argument type is missing")

        if param.kind is param.VAR_POSITIONAL:
            raise RuntimeError("args parameters are not allowed")

        elif param.kind is param.VAR_KEYWORD:
            raise RuntimeError("kwargs parameters are not allowed")

        elif param.kind is param.POSITIONAL_OR_KEYWORD:
            raise RuntimeError("positional or keyword parameters are not allowed")

        elif param.kind is param.POSITIONAL_ONLY:
            if param.default is not Parameter.empty:
                raise RuntimeError(
                    f"positional only parameters default values are not allowed, "
                    f"define '{param.name}' parameter as keyword only"
                )
            args_definition.append((param.annotation, param.default))

        elif param.kind is param.KEYWORD_ONLY:
            kwargs_definition[param.name] = (param.annotation, param.default)

        else:
            raise AssertionError("unreachable")

    return args_definition, kwargs_definition


def build_json_serializable_bound_task[CtxT, **P, R](
    func: Callable[Concatenate[CtxT, P], Awaitable[R]],
) -> type[BoundTask]:
    args_definition, kwargs_definition = extract_func_signature(func, skip_args=1)

    args_type = tuple(typ for typ, default in args_definition)
    kwargs_type = {
        name: typ if default is Parameter.empty else NotRequired[typ]
        for name, (typ, default) in kwargs_definition.items()
    }

    class FuncBoundTask(JsonSerializable, BoundTask):
        args: tuple[*args_type] if args_type else tuple[()]  # type: ignore[valid-type]
        kwargs: TypedDict(f"{func.__name__}_kwargs", kwargs_type)  # type: ignore[valid-type]

    return FuncBoundTask


class Task[CtxT, **P, R]:
    """
    Scheduler task.

    :param func: task function
    :param name: task name
    :param bound_task_cls: bound task class
    """

    def __init__(
        self,
        func: Callable[Concatenate[CtxT, P], Awaitable[R]],
        name: str,
        bound_task_cls: type[BoundTask],
    ):
        self._func = func
        self._name = name
        self._bound_task_cls = bound_task_cls

    @property
    def name(self) -> str:
        """
        Task name.
        """

        return self._name

    @property
    def bound_task_cls(self) -> type[BoundTask]:
        """
        Bound task class.
        """

        return self._bound_task_cls

    async def __call__(self, context: CtxT, *args: P.args, **kwargs: P.kwargs) -> R:
        """
        Calls the task function.

        :param context: scheduler context
        :param args: function positional arguments
        :param kwargs: function keyword arguments
        :return: function result
        """

        return await self._func(context, *args, **kwargs)

    def with_params(self, *args: P.args, **kwargs: P.kwargs) -> BoundTask:
        """
        Bind parameters to the task.

        :param args: positional arguments to be bound to the task
        :param kwargs: keyword arguments to be bound to the task
        :return: bound task
        """

        return self._bound_task_cls(name=self._name, args=args, kwargs=kwargs)


class SerializerBuilder[CtxT](Protocol):
    """
    Serializer builder protocol.
    """

    def __call__(self, func: Callable[Concatenate[CtxT, ...], Any]) -> type[BoundTask]:
        """
        Builds a serializer for the function.

        :param func: function to build the serializer for
        :return: bound task
        """


class TaskRegistry[StT, LkT: TaskLock]:
    """
    Scheduler task registry.

    :param serializer_builder: serializer builder to be used to build serializer for the tasks in the registry
    """

    def __init__(self, serializer_builder: Optional[SerializerBuilder[Any]] = None) -> None:
        self._tasks: dict[str, Task[Context[StT, LkT], Any, Any]] = {}
        self._serializer_builder = serializer_builder or build_json_serializable_bound_task

    def task[**P, R](
        self,
        name: Optional[str] = None,
        serializer_builder: Optional[SerializerBuilder[Any]] = None,
    ) -> Callable[[Callable[Concatenate[Context[StT, LkT], P], Awaitable[R]]], Task[Context[StT, LkT], P, R]]:
        """
        Adds task to the registry.

        :param name: task name
        :param serializer_builder: task serializer builder
        """

        def decorator(
            func: Callable[Concatenate[Context[StT, LkT], P], Awaitable[R]],
        ) -> Task[Context[StT, LkT], P, R]:
            task_name = name or func.__name__
            task_serializer_builder = serializer_builder or self._serializer_builder
            bound_task = task_serializer_builder(func)

            if task_name in self._tasks:
                warnings.warn(f"task with name '{task_name}' already registered", stacklevel=1)

            self._tasks[task_name] = task = Task(func, task_name, bound_task)
            return task

        return decorator

    def get(self, name: str) -> Optional[Task[Context[StT, LkT], Any, Any]]:
        """
        Returns a task from the registry by name

        :param name: task name
        :return: task
        """

        return self._tasks.get(name)
