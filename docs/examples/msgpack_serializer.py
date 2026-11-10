from inspect import Parameter
from typing import Any, Awaitable, Callable, Concatenate, MutableMapping, NotRequired, TypedDict

import msgspec

import schedex as sx
import schedex.contrib.sqlalchemy as sxsa

AppState = MutableMapping[str, Any]
TaskLock = sxsa.SqlAlchemySfuTaskLock
Context = sx.Context[AppState, TaskLock]

tasks = sx.TaskRegistry[AppState, TaskLock]()


class MsgPackSerializable(sx.serializer.MsgSpecSerializable):
    @classmethod
    def __build_encoder__(cls) -> sx.serializer.EncoderProto:
        return msgspec.msgpack.Encoder()

    @classmethod
    def __build_decoder__(cls) -> sx.serializer.DecoderProto:
        return msgspec.msgpack.Decoder(type=cls)


def build_bound_task[CtxT, **P, R](func: Callable[Concatenate[CtxT, P], Awaitable[R]]) -> type[sx.BoundTask]:
    args_definition, kwargs_definition = sx.task.extract_func_signature(func, skip_args=1)

    args_type = tuple(typ for typ, default in args_definition)
    kwargs_type = {
        name: typ if default is Parameter.empty else NotRequired[typ]
        for name, (typ, default) in kwargs_definition.items()
    }

    class FuncBoundTask(MsgPackSerializable, sx.BoundTask):
        args: tuple[*args_type] if args_type else tuple[()]  # type: ignore[valid-type]
        kwargs: TypedDict(f"{func.__name__}_kwargs", kwargs_type)  # type: ignore[valid-type]

    return FuncBoundTask


@tasks.task(serializer_builder=build_bound_task)
async def print_message(context: Context, message: str, /) -> None:
    print(message)
