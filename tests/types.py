import abc
from typing import Any, AsyncGenerator, Generator, Mapping, Optional, TypeVar

FixtureResult = TypeVar("FixtureResult")
AsyncFixture = AsyncGenerator[FixtureResult, None]
Fixture = Generator[FixtureResult, None, None]


class BaseStorage(abc.ABC):
    @abc.abstractmethod
    async def insert(self, fields: Mapping[str, Any]) -> None:
        pass

    @abc.abstractmethod
    async def delete(self, filters: Optional[Mapping[str, Any]] = None) -> None:
        pass

    @abc.abstractmethod
    async def assert_has(self, filters: Mapping[str, Any], count: Optional[int] = None) -> None:
        pass

    @abc.abstractmethod
    async def assert_has_exactly(self, filters: Mapping[str, Any], count: int) -> None:
        pass

    @abc.abstractmethod
    async def assert_empty(self) -> None:
        pass


class StorageTransaction[Tx](abc.ABC):
    @abc.abstractmethod
    async def begin(self) -> Tx:
        pass

    @abc.abstractmethod
    async def commit(self) -> None:
        pass

    @abc.abstractmethod
    async def rollback(self) -> None:
        pass
