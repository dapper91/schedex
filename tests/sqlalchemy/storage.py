from typing import Any, ClassVar, Mapping, Optional

import sqlalchemy as sa
import sqlalchemy.ext.asyncio as aiosa

from schedex.contrib.sqlalchemy import tables as tb
from tests.types import BaseStorage, StorageTransaction


class BaseSqlAlchemyStorage(BaseStorage):
    Table: ClassVar[type[Any]]

    def __init__(self, session: aiosa.AsyncSession):
        self._session = session

    async def insert(self, fields: Mapping[str, Any]) -> None:
        entity = self.Table(**fields)

        async with self._session.begin():
            self._session.add(entity)
            await self._session.flush()

    async def delete(self, filters: Optional[Mapping[str, Any]] = None) -> None:
        query = sa.delete(self.Table)
        if filters:
            query = query.filter_by(**filters)

        async with self._session.begin():
            await self._session.scalar(query)

    async def assert_has(
        self, filters: Mapping[str, Any], count: Optional[int] = None, skip_locked: bool = True
    ) -> None:
        query = sa.select(self.Table)
        if skip_locked:
            query = query.with_for_update(skip_locked=True, read=True)
        if filters:
            query = query.filter_by(**filters)

        async with self._session.begin():
            entities = list(await self._session.scalars(query))
            if count is None:
                assert len(entities) > 0, "entity not found"
            else:
                assert len(entities) == count, f"entity count does not match, actual: {entities}"

    async def assert_has_exactly(self, filters: Mapping[str, Any], count: int, skip_locked: bool = True) -> None:
        await self.assert_has(filters, count, skip_locked)

    async def assert_empty(self) -> None:
        await self.assert_has({}, 0)


class JobSqlAlchemyStorage(BaseSqlAlchemyStorage):
    Table = tb.Job


class TaskSqlAlchemyStorage(BaseSqlAlchemyStorage):
    Table = tb.Task


class SqlAlchemyStorageTransaction[Tx](StorageTransaction):
    def __init__(self, session: aiosa.AsyncSession):
        self._session = session

    async def begin(self) -> Tx:
        await self._session.begin()
        return self._session

    async def commit(self) -> None:
        await self._session.commit()

    async def rollback(self) -> None:
        await self._session.rollback()
