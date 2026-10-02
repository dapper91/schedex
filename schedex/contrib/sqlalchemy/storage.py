from types import TracebackType
from typing import Any, Optional, Self, cast

import sqlalchemy as sa
import sqlalchemy.exc
import sqlalchemy.ext.asyncio as aiosa

import schedex as sx

from . import tables


class SqlAlchemyStoredJobManager:
    async def _get_job(self, session: aiosa.AsyncSession, job_id: str) -> Optional[sx.StoredJob]:
        result = await session.execute(sa.select(tables.Job).where(tables.Job.id == job_id))
        if job_row := result.scalar():
            return sx.StoredJob(
                id=job_row.id,
                created_at=job_row.created_at,
                status=job_row.status,
                schedule=job_row.schedule,
                count=job_row.count,
                task_name=job_row.task_name,
                task_args=job_row.task_args,
                meta=job_row.meta,
                run_at=job_row.run_at,
            )
        else:
            return None

    async def _add_job(self, session: aiosa.AsyncSession, job: sx.StoredJob) -> None:
        session.add(
            tables.Job(
                id=job.id,
                created_at=job.created_at,
                status=job.status,
                schedule=job.schedule,
                count=job.count,
                task_name=job.task_name,
                task_args=job.task_args,
                meta=job.meta,
                run_at=job.run_at,
            )
        )
        try:
            await session.flush()
        except sa.exc.IntegrityError as e:
            raise sx.JobAlreadyExists(job.id) from e

    async def _cancel_job(self, session: aiosa.AsyncSession, job_id: str) -> bool:
        result = cast(sa.CursorResult[Any], await session.execute(sa.delete(tables.Job).where(tables.Job.id == job_id)))

        return bool(result.rowcount)


class SqlAlchemyJobManager(SqlAlchemyStoredJobManager, sx.JobManager):
    """
    SqlAlchemy job manager.
    """

    def __init__(self, session_maker: aiosa.async_sessionmaker[aiosa.AsyncSession]):
        self._session_maker = session_maker

    async def get_job(self, job_id: str) -> Optional[sx.StoredJob]:
        async with self._session_maker() as session:
            return await self._get_job(session, job_id)

    async def add_job(self, job: sx.StoredJob) -> None:
        async with self._session_maker() as session:
            async with session.begin():
                return await self._add_job(session, job)

    async def cancel_job(self, job_id: str) -> bool:
        async with self._session_maker() as session:
            async with session.begin():
                return await self._cancel_job(session, job_id)


class SqlAlchemyOwnedTransaction(SqlAlchemyStoredJobManager, sx.OwnedTransaction[aiosa.AsyncSession]):
    """
    SqlAlchemy owned transaction.
    """

    def __init__(self, session: aiosa.AsyncSession):
        self._session = session

    @property
    def outer(self) -> aiosa.AsyncSession:
        return self._session

    async def __aenter__(self) -> Self:
        await self._session.begin()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Optional[bool]:
        try:
            await self._session.commit() if exc_value is None else await self._session.rollback()
        finally:
            await self._session.close()

        return False

    async def get_job(self, job_id: str) -> Optional[sx.StoredJob]:
        return await self._get_job(self._session, job_id)

    async def add_job(self, job: sx.StoredJob) -> None:
        return await self._add_job(self._session, job)

    async def cancel_job(self, job_id: str) -> bool:
        return await self._cancel_job(self._session, job_id)

    async def begin(self) -> None:
        await self._session.begin()

    async def commit(self) -> None:
        await self._session.commit()

    async def rollback(self) -> None:
        await self._session.rollback()


class SqlAlchemyTransaction(SqlAlchemyStoredJobManager, sx.Transaction[aiosa.AsyncSession]):
    """
    SqlAlchemy transaction.
    """

    def __init__(self, outer_tx: aiosa.AsyncSession):
        assert outer_tx.in_transaction(), "transaction is not started"
        self._outer_tx = outer_tx

    @property
    def outer(self) -> aiosa.AsyncSession:
        return self._outer_tx

    async def get_job(self, job_id: str) -> Optional[sx.StoredJob]:
        return await self._get_job(self._outer_tx, job_id)

    async def add_job(self, job: sx.StoredJob) -> None:
        return await self._add_job(self._outer_tx, job)

    async def cancel_job(self, job_id: str) -> bool:
        return await self._cancel_job(self._outer_tx, job_id)


class SqlAlchemyTransactionManager(sx.TransactionManager[aiosa.AsyncSession]):
    """
    SqlAlchemy storage.
    """

    def __init__(self, session_maker: aiosa.async_sessionmaker[aiosa.AsyncSession]):
        self._session_maker = session_maker

    def begin_transactional(self) -> SqlAlchemyOwnedTransaction:
        return SqlAlchemyOwnedTransaction(self._session_maker())

    def within_transaction(self, outer_tx: aiosa.AsyncSession) -> SqlAlchemyTransaction:
        return SqlAlchemyTransaction(outer_tx)
