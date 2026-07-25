from typing import Any, ClassVar, Mapping, Optional

import sqlalchemy as sa
import sqlalchemy.ext.asyncio as aiosa

from schedex.contrib.sqlalchemy import tables as tb


class BaseTable[EntityT]:
    Table: ClassVar[type[EntityT]]

    def __init__(self, session: aiosa.AsyncSession):
        self._session = session

    async def insert(self, entity: EntityT) -> EntityT:
        async with self._session.begin():
            self._session.add(entity)
            await self._session.flush()
            self._session.expunge(entity)

        return entity

    async def delete(self, entity_id: str) -> None:
        async with self._session.begin():
            result = await self._session.execute(sa.delete(self.Table).where(self.Table.id == entity_id))
            assert result.rowcount == 1

    async def select(self, entity_id: str) -> EntityT:
        async with self._session.begin():
            entity = await self._session.scalar(sa.select(self.Table).where(self.Table.id == entity_id))
            self._session.expunge(entity)

        assert entity is not None, "entity not found"
        return entity

    async def select_all(self, filters: Optional[Mapping[str, Any]] = None, skip_locked: bool = False) -> list[EntityT]:
        query = sa.select(self.Table)
        if filters:
            query = query.filter_by(**filters)
        if skip_locked:
            query = query.with_for_update(skip_locked=True, read=True)

        async with self._session.begin():
            entities = list(await self._session.scalars(query))
            for entity in entities:
                self._session.expunge(entity)

            return entities

    async def count(self, filters: Optional[Mapping[str, Any]] = None, skip_locked: bool = False) -> int:
        return len(await self.select_all(filters, skip_locked=skip_locked))

    async def empty(self) -> bool:
        return await self.count() == 0


class JobTable(BaseTable[tb.Job]):
    Table = tb.Job


class TaskTable(BaseTable[tb.Task]):
    Table = tb.Task
