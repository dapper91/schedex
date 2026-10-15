import dataclasses as dc
from typing import Any

import msgspec.json
import pytest

import schedex as sx

pytestmark = pytest.mark.unit


def test_task_registry_task_params_primitive_types() -> None:
    registry = sx.TaskRegistry()

    @registry.task("task_name")
    async def function(
        context: sx.Context[Any, Any],
        *,
        integer: int,
        number: float,
        string: str,
        binary: bytes,
        boolean: bool,
    ) -> None:
        pass

    task = registry.get("task_name")
    assert task is not None
    assert task.name == "task_name"

    bound_task = task.with_params(integer=1, number=2.2, string="text", binary=b"bin", boolean=True)
    serialized = bound_task.serialize()
    assert serialized == msgspec.json.encode(
        {
            "name": "task_name",
            "args": [],
            "kwargs": {"integer": 1, "number": 2.2, "string": "text", "binary": "Ymlu", "boolean": True},
        }
    )


def test_task_registry_task_params_dataclass() -> None:
    registry = sx.TaskRegistry()

    @dc.dataclass
    class Object:
        string: str
        integers: list[int]
        mapping: dict[str, str]

    @registry.task()
    async def function(context: sx.Context[Any, Any], *, obj: Object) -> None:
        pass

    task = registry.get("function")
    assert task is not None
    assert task.name == "function"

    bound_task = task.with_params(obj=Object(string="text", integers=[1, 2], mapping={"key": "value"}))
    serialized = bound_task.serialize()

    assert serialized == msgspec.json.encode(
        {
            "name": "function",
            "args": [],
            "kwargs": {"obj": {"string": "text", "integers": [1, 2], "mapping": {"key": "value"}}},
        }
    )


def test_task_registry_task_params_mixed() -> None:
    registry = sx.TaskRegistry()

    @registry.task("task_name")
    async def function(context: sx.Context[Any, Any], integer: int, number: float, /, *, string: str) -> None:
        pass

    task = registry.get("task_name")
    assert task is not None
    assert task.name == "task_name"

    bound_task = task.with_params(1, 2.2, string="text")
    serialized = bound_task.serialize()
    assert serialized == msgspec.json.encode(
        {
            "name": "task_name",
            "args": [1, 2.2],
            "kwargs": {"string": "text"},
        }
    )
