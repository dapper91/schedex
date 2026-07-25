import pytest
import sqlalchemy.ext.asyncio as aiosa

from schedex import StoredJob
from schedex.contrib.sqlalchemy.storage import SqlAlchemyTransactionManager
from tests.sqlalchemy.factory import JobFactory
from tests.sqlalchemy.tables import JobTable


class CustomException(Exception):
    pass


async def test_sqlalchemy_owned_transaction_add_job_commit(
    sa_session_maker: aiosa.async_sessionmaker[aiosa.AsyncSession], job_factory: type[JobFactory], job_table: JobTable
) -> None:
    tx_mgr = SqlAlchemyTransactionManager(sa_session_maker)

    expected_job = job_factory.build()

    expected_stored_job = StoredJob(
        id=expected_job.id,
        created_at=expected_job.created_at,
        status=expected_job.status,
        schedule=expected_job.schedule,
        count=expected_job.count,
        task_name=expected_job.task_name,
        task_args=expected_job.task_args,
        meta=expected_job.meta,
        run_at=expected_job.run_at,
    )
    tx = tx_mgr.begin_transactional()
    await tx.begin()
    await tx.add_job(expected_stored_job)
    await tx.commit()

    actual_job = await job_table.select(expected_job.id)
    assert actual_job == expected_job


async def test_sqlalchemy_owned_transaction_add_job_rollback(
    sa_session_maker: aiosa.async_sessionmaker[aiosa.AsyncSession], job_factory: type[JobFactory], job_table: JobTable
) -> None:
    tx_mgr = SqlAlchemyTransactionManager(sa_session_maker)

    expected_job = job_factory.build()

    expected_stored_job = StoredJob(
        id=expected_job.id,
        created_at=expected_job.created_at,
        status=expected_job.status,
        schedule=expected_job.schedule,
        count=expected_job.count,
        task_name=expected_job.task_name,
        task_args=expected_job.task_args,
        meta=expected_job.meta,
        run_at=expected_job.run_at,
    )
    tx = tx_mgr.begin_transactional()
    await tx.begin()
    await tx.add_job(expected_stored_job)
    await tx.rollback()

    actual_jobs = await job_table.select_all()
    assert actual_jobs == []


async def test_sqlalchemy_owned_transaction_add_job_context_manager_success(
    sa_session_maker: aiosa.async_sessionmaker[aiosa.AsyncSession], job_factory: type[JobFactory], job_table: JobTable
) -> None:
    tx_mgr = SqlAlchemyTransactionManager(sa_session_maker)

    expected_job = job_factory.build()

    expected_stored_job = StoredJob(
        id=expected_job.id,
        created_at=expected_job.created_at,
        status=expected_job.status,
        schedule=expected_job.schedule,
        count=expected_job.count,
        task_name=expected_job.task_name,
        task_args=expected_job.task_args,
        meta=expected_job.meta,
        run_at=expected_job.run_at,
    )
    async with tx_mgr.begin_transactional() as tx:
        await tx.add_job(expected_stored_job)

    async with tx_mgr.begin_transactional() as tx:
        actual_job = await tx.get_job(expected_job.id)
        assert actual_job == expected_stored_job

    actual_job = await job_table.select(expected_job.id)
    assert actual_job == expected_job


async def test_sqlalchemy_owned_transaction_add_job_context_manager_error(
    sa_session_maker: aiosa.async_sessionmaker[aiosa.AsyncSession], job_factory: type[JobFactory], job_table: JobTable
) -> None:
    tx_mgr = SqlAlchemyTransactionManager(sa_session_maker)

    expected_job = job_factory.build()

    expected_stored_job = StoredJob(
        id=expected_job.id,
        created_at=expected_job.created_at,
        status=expected_job.status,
        schedule=expected_job.schedule,
        count=expected_job.count,
        task_name=expected_job.task_name,
        task_args=expected_job.task_args,
        meta=expected_job.meta,
        run_at=expected_job.run_at,
    )
    with pytest.raises(CustomException):
        async with tx_mgr.begin_transactional() as tx:
            await tx.add_job(expected_stored_job)
            raise CustomException()

    async with tx_mgr.begin_transactional() as tx:
        actual_job = await tx.get_job(expected_job.id)
        assert actual_job is None

    actual_jobs = await job_table.select_all()
    assert actual_jobs == []


async def test_sqlalchemy_owned_transaction_remove_job_context_manager_success(
    sa_session_maker: aiosa.async_sessionmaker[aiosa.AsyncSession], job_factory: type[JobFactory], job_table: JobTable
) -> None:
    tx_mgr = SqlAlchemyTransactionManager(sa_session_maker)

    expected_job = await job_table.insert(job_factory.build())

    async with tx_mgr.begin_transactional() as tx:
        await tx.cancel_job(expected_job.id)

    async with tx_mgr.begin_transactional() as tx:
        stored_job = await tx.get_job(expected_job.id)
        assert stored_job is None

    actual_jobs = await job_table.select_all()
    assert actual_jobs == []


async def test_sqlalchemy_owned_transaction_remove_job_context_manager_error(
    sa_session_maker: aiosa.async_sessionmaker[aiosa.AsyncSession], job_factory: type[JobFactory], job_table: JobTable
) -> None:
    tx_mgr = SqlAlchemyTransactionManager(sa_session_maker)

    expected_job = await job_table.insert(job_factory.build())

    with pytest.raises(CustomException):
        async with tx_mgr.begin_transactional() as tx:
            await tx.cancel_job(expected_job.id)
            raise CustomException()

    actual_jobs = await job_table.select_all()
    assert actual_jobs == [expected_job]


async def test_sqlalchemy_transaction_add_job_commit(
    sa_session_maker: aiosa.async_sessionmaker[aiosa.AsyncSession], job_factory: type[JobFactory], job_table: JobTable
) -> None:
    tx_mgr = SqlAlchemyTransactionManager(sa_session_maker)

    expected_job = job_factory.build()

    expected_stored_job = StoredJob(
        id=expected_job.id,
        created_at=expected_job.created_at,
        status=expected_job.status,
        schedule=expected_job.schedule,
        count=expected_job.count,
        task_name=expected_job.task_name,
        task_args=expected_job.task_args,
        meta=expected_job.meta,
        run_at=expected_job.run_at,
    )
    async with sa_session_maker() as session:
        async with session.begin():
            tx = tx_mgr.within_transaction(session)
            await tx.add_job(expected_stored_job)

    actual_job = await job_table.select(expected_job.id)
    assert actual_job == expected_job


async def test_sqlalchemy_transaction_add_job_rollback(
    sa_session_maker: aiosa.async_sessionmaker[aiosa.AsyncSession], job_factory: type[JobFactory], job_table: JobTable
) -> None:
    tx_mgr = SqlAlchemyTransactionManager(sa_session_maker)

    expected_job = job_factory.build()

    expected_stored_job = StoredJob(
        id=expected_job.id,
        created_at=expected_job.created_at,
        status=expected_job.status,
        schedule=expected_job.schedule,
        count=expected_job.count,
        task_name=expected_job.task_name,
        task_args=expected_job.task_args,
        meta=expected_job.meta,
        run_at=expected_job.run_at,
    )
    async with sa_session_maker() as session:
        await session.begin()
        tx = tx_mgr.within_transaction(session)
        await tx.add_job(expected_stored_job)
        await session.rollback()

    actual_jobs = await job_table.select_all()
    assert actual_jobs == []
