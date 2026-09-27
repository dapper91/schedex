from typing import AsyncGenerator, Generator, TypeVar

FixtureResult = TypeVar("FixtureResult")
AsyncFixture = AsyncGenerator[FixtureResult, None]
Fixture = Generator[FixtureResult, None, None]
