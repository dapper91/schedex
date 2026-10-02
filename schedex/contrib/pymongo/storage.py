from types import TracebackType
from typing import Optional, Self, cast

import pymongo.asynchronous.client_session as mgses
import pymongo.asynchronous.collection as mgcol
import pymongo.asynchronous.mongo_client as mgcli

import schedex as sx

from . import utils
from .schema import JOBS_COLLECTION, DocumentType, Job


class PyMongoStoredJobManager:
    """
    PyMongo job manager.
    """

    async def _get_job(
        self, session: mgses.AsyncClientSession, dbname: Optional[str], job_id: str
    ) -> Optional[sx.StoredJob]:
        collection = cast(mgcol.AsyncCollection[Job], utils.get_collection(session, dbname, JOBS_COLLECTION))

        if (job := await collection.find_one({"id": job_id}, session=session)) is not None:
            return sx.StoredJob(
                id=job["id"],
                created_at=job["created_at"],
                status=job["status"],
                schedule=job["schedule"],
                count=job["count"],
                task_name=job["task_name"],
                task_args=job["task_args"],
                meta=job["meta"],
                run_at=job["run_at"],
            )
        else:
            return None

    async def _add_job(self, session: mgses.AsyncClientSession, dbname: Optional[str], job: sx.StoredJob) -> None:
        collection = cast(mgcol.AsyncCollection[Job], utils.get_collection(session, dbname, JOBS_COLLECTION))
        try:
            await collection.insert_one(
                Job(
                    id=job.id,
                    created_at=job.created_at,
                    status=job.status,
                    schedule=job.schedule,
                    count=job.count,
                    task_name=job.task_name,
                    task_args=job.task_args,
                    meta=job.meta,
                    run_at=job.run_at,
                ),
                session=session,
            )
        except pm.errors.DuplicateKeyError as e:
            raise sx.JobAlreadyExists(job.id) from e

    async def _cancel_job(self, session: mgses.AsyncClientSession, dbname: Optional[str], job_id: str) -> bool:
        collection = cast(mgcol.AsyncCollection[Job], utils.get_collection(session, dbname, JOBS_COLLECTION))
        result = await collection.delete_one({"id": job_id}, session=session)

        return bool(result.deleted_count)


class PyMongoJobManager(PyMongoStoredJobManager, sx.JobManager):
    """
    PyMongo job manager.
    """

    def __init__(self, client: mgcli.AsyncMongoClient[DocumentType], dbname: Optional[str]):
        self._client = client
        self._dbname = dbname

    async def get_job(self, job_id: str) -> Optional[sx.StoredJob]:
        async with self._client.start_session() as session:
            return await self._get_job(session, self._dbname, job_id)

    async def add_job(self, job: sx.StoredJob) -> None:
        async with self._client.start_session() as session:
            return await self._add_job(session, self._dbname, job)

    async def cancel_job(self, job_id: str) -> bool:
        async with self._client.start_session() as session:
            return await self._cancel_job(session, self._dbname, job_id)


class PyMongoOwnedTransaction(PyMongoStoredJobManager, sx.OwnedTransaction[mgses.AsyncClientSession]):
    """
    PyMongo owned transaction.
    """

    def __init__(self, session: mgses.AsyncClientSession, dbname: Optional[str]):
        self._session = session
        self._dbname = dbname

    @property
    def outer(self) -> mgses.AsyncClientSession:
        return self._session

    async def __aenter__(self) -> Self:
        await self._session.start_transaction()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Optional[bool]:
        try:
            await self._session.commit_transaction() if exc_value is None else await self._session.abort_transaction()
        finally:
            await self._session.end_session()

        return False

    async def get_job(self, job_id: str) -> Optional[sx.StoredJob]:
        return await self._get_job(self._session, self._dbname, job_id)

    async def add_job(self, job: sx.StoredJob) -> None:
        return await self._add_job(self._session, self._dbname, job)

    async def cancel_job(self, job_id: str) -> bool:
        return await self._cancel_job(self._session, self._dbname, job_id)

    async def begin(self) -> None:
        await self._session.start_transaction()

    async def commit(self) -> None:
        await self._session.commit_transaction()

    async def rollback(self) -> None:
        await self._session.abort_transaction()


class PyMongoTransaction(PyMongoStoredJobManager, sx.Transaction[mgses.AsyncClientSession]):
    """
    PyMongo transaction.
    """

    def __init__(self, outer_tx: mgses.AsyncClientSession, dbname: Optional[str]):
        assert outer_tx.in_transaction, "transaction is not started"
        self._outer_tx = outer_tx
        self._dbname = dbname

    @property
    def outer(self) -> mgses.AsyncClientSession:
        return self._outer_tx

    async def get_job(self, job_id: str) -> Optional[sx.StoredJob]:
        return await self._get_job(self._outer_tx, self._dbname, job_id)

    async def add_job(self, job: sx.StoredJob) -> None:
        return await self._add_job(self._outer_tx, self._dbname, job)

    async def cancel_job(self, job_id: str) -> bool:
        return await self._cancel_job(self._outer_tx, self._dbname, job_id)


class PyMongoTransactionManager(sx.TransactionManager[mgses.AsyncClientSession]):
    """
    PyMongo storage.
    """

    def __init__(self, client: mgcli.AsyncMongoClient[DocumentType], dbname: Optional[str] = None):
        self._client = client
        self._dbname = dbname

    def begin_transactional(self) -> PyMongoOwnedTransaction:
        return PyMongoOwnedTransaction(self._client.start_session(), self._dbname)

    def within_transaction(self, outer_tx: mgses.AsyncClientSession) -> PyMongoTransaction:
        return PyMongoTransaction(outer_tx, self._dbname)
