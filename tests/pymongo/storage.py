import datetime as dt
from typing import Any, ClassVar, Mapping, Optional

import pymongo.asynchronous.client_session as sess

import schedex.contrib.pymongo as sxpm
from tests.types import BaseStorage, StorageTransaction

type DocumentType = Mapping[str, Any]


class BasePyMongoStorage(BaseStorage):
    Document: ClassVar[type[Any]]
    Collection: ClassVar[str]

    def __init__(self, session: sess.AsyncClientSession, database: str, lock_id: str, lock_period: dt.timedelta):
        self._session = session
        self._lock_id = lock_id
        self._lock_period = lock_period
        self._collection = session.client.get_database(database).get_collection(self.Collection)

    async def insert(self, fields: Mapping[str, Any]) -> None:
        await self._collection.insert_one(self.Document(**fields))

    async def delete(self, filters: Optional[Mapping[str, Any]] = None) -> None:
        if filters:
            await self._collection.delete_one(filters)
        else:
            await self._collection.delete_many({})

    async def assert_has(self, filters: Mapping[str, Any], count: Optional[int] = None) -> None:
        docs: list[DocumentType] = []
        async with self._collection.find(filters) as cursor:
            async for doc in cursor:
                docs.append(doc)

        if count is None:
            assert len(docs) > 0, "entity not found"
        else:
            assert len(docs) == count, f"entity count does not match, actual docs: {docs}"

    async def assert_has_exactly(self, filters: Mapping[str, Any], count: int) -> None:
        await self.assert_has(filters, count)

    async def assert_empty(self) -> None:
        await self.assert_has({}, 0)

    async def assert_is_locked(self, filters: Mapping[str, Any]) -> None:
        filters = dict(
            filters,
            acquired_by=self._lock_id,
            acquired_until=dt.datetime.now(dt.timezone.utc) + self._lock_period,
        )
        await self.assert_has(filters)


class JobPyMongoStorage(BasePyMongoStorage):
    Document = sxpm.Job
    Collection = "schedex_jobs"


class TaskPyMongoStorage(BasePyMongoStorage):
    Document = sxpm.Task
    Collection = "schedex_tasks"


class PyMongoStorageTransaction[Tx](StorageTransaction):
    def __init__(self, session: sess.AsyncClientSession):
        self._session = session

    async def begin(self) -> Tx:
        await self._session.start_transaction()
        return self._session

    async def commit(self) -> None:
        await self._session.commit_transaction()

    async def rollback(self) -> None:
        await self._session.abort_transaction()
