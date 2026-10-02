from typing import Any

import pytest
from pytest_lazy_fixtures import lf

import schedex as sx
from tests.conftest import StoredJobFactory
from tests.types import BaseStorage, StorageTransaction


@pytest.fixture(
    params=[
        pytest.param(
            (
                lf("sa_job_storage"),
                lf("sa_storage_transaction"),
                lf("sa_transaction_manager"),
            ),
            id="sqlalchemy",
        ),
        pytest.param(
            (
                lf("pymongo_job_storage"),
                lf("pymongo_storage_transaction"),
                lf("pymongo_transaction_manager"),
            ),
            id="pymongo",
        ),
    ],
)
async def test_bundle(
    request: pytest.FixtureRequest,
) -> tuple[BaseStorage, StorageTransaction, sx.TransactionManager[Any]]:
    storage: BaseStorage = request.param[0]
    storage_transaction: StorageTransaction = request.param[1]
    transaction_manager: sx.TransactionManager[Any] = request.param[2]
    return storage, storage_transaction, transaction_manager


@pytest.fixture
def job_storage(test_bundle: tuple[BaseStorage, StorageTransaction, sx.TransactionManager[Any]]) -> BaseStorage:
    return test_bundle[0]


@pytest.fixture
def storage_transaction(
    test_bundle: tuple[BaseStorage, StorageTransaction, sx.TransactionManager[Any]],
) -> StorageTransaction[Any]:
    return test_bundle[1]


@pytest.fixture
def transaction_manager(
    test_bundle: tuple[BaseStorage, StorageTransaction, sx.TransactionManager[Any]],
) -> sx.TransactionManager[Any]:
    return test_bundle[2]


class CustomException(Exception):
    pass


@pytest.mark.time_machine("2026-10-10 22:30 +0500", tick=False)
async def test_owned_transaction_add_job_commit(
    stored_job_factory: type[StoredJobFactory],
    job_storage: BaseStorage,
    transaction_manager: sx.TransactionManager[Any],
) -> None:
    stored_job = stored_job_factory.build()

    tx = transaction_manager.begin_transactional()
    await tx.begin()
    await tx.add_job(stored_job)
    await tx.commit()

    await job_storage.assert_has_exactly(
        dict(
            id=stored_job.id,
            created_at=stored_job.created_at,
            status=stored_job.status,
            schedule=stored_job.schedule,
            count=stored_job.count,
            task_name=stored_job.task_name,
            task_args=stored_job.task_args,
            meta=stored_job.meta,
            run_at=stored_job.run_at,
            acquired_by=None,
            acquired_until=None,
        ),
        count=1,
    )


async def test_owned_transaction_add_job_rollback(
    stored_job_factory: type[StoredJobFactory],
    job_storage: BaseStorage,
    transaction_manager: sx.TransactionManager[Any],
) -> None:
    stored_job = stored_job_factory.build()

    tx = transaction_manager.begin_transactional()
    await tx.begin()
    await tx.add_job(stored_job)
    await tx.rollback()

    await job_storage.assert_has_exactly({}, 0)


@pytest.mark.time_machine("2026-10-10 22:30 +0500", tick=False)
async def test_owned_transaction_add_job_context_manager_success(
    stored_job_factory: type[StoredJobFactory],
    job_storage: BaseStorage,
    transaction_manager: sx.TransactionManager[Any],
) -> None:
    stored_job = stored_job_factory.build()

    async with transaction_manager.begin_transactional() as tx:
        await tx.add_job(stored_job)

    async with transaction_manager.begin_transactional() as tx:
        actual_job = await tx.get_job(stored_job.id)
        assert actual_job == stored_job

    await job_storage.assert_has_exactly(
        dict(
            id=stored_job.id,
            created_at=stored_job.created_at,
            status=stored_job.status,
            schedule=stored_job.schedule,
            count=stored_job.count,
            task_name=stored_job.task_name,
            task_args=stored_job.task_args,
            meta=stored_job.meta,
            run_at=stored_job.run_at,
            acquired_by=None,
            acquired_until=None,
        ),
        count=1,
    )


async def test_sqlalchemy_owned_transaction_add_job_context_manager_error(
    stored_job_factory: type[StoredJobFactory],
    job_storage: BaseStorage,
    transaction_manager: sx.TransactionManager[Any],
) -> None:
    stored_job = stored_job_factory.build()

    with pytest.raises(CustomException):
        async with transaction_manager.begin_transactional() as tx:
            await tx.add_job(stored_job)
            raise CustomException()

    async with transaction_manager.begin_transactional() as tx:
        actual_job = await tx.get_job(stored_job.id)
        assert actual_job is None

    await job_storage.assert_has_exactly({}, count=0)


async def test_sqlalchemy_owned_transaction_remove_job_context_manager_success(
    stored_job_factory: type[StoredJobFactory],
    job_storage: BaseStorage,
    transaction_manager: sx.TransactionManager[Any],
) -> None:
    stored_job = stored_job_factory.build()

    await job_storage.insert(
        dict(
            id=stored_job.id,
            created_at=stored_job.created_at,
            status=stored_job.status,
            schedule=stored_job.schedule,
            count=stored_job.count,
            task_name=stored_job.task_name,
            task_args=stored_job.task_args,
            meta=stored_job.meta,
            run_at=stored_job.run_at,
            acquired_by=None,
            acquired_until=None,
        )
    )

    async with transaction_manager.begin_transactional() as tx:
        await tx.cancel_job(stored_job.id)

    async with transaction_manager.begin_transactional() as tx:
        actual_job = await tx.get_job(stored_job.id)
        assert actual_job is None

    await job_storage.assert_has_exactly({}, count=0)


async def test_sqlalchemy_owned_transaction_remove_job_context_manager_error(
    stored_job_factory: type[StoredJobFactory],
    job_storage: BaseStorage,
    transaction_manager: sx.TransactionManager[Any],
) -> None:
    stored_job = stored_job_factory.build()

    await job_storage.insert(
        dict(
            id=stored_job.id,
            created_at=stored_job.created_at,
            status=stored_job.status,
            schedule=stored_job.schedule,
            count=stored_job.count,
            task_name=stored_job.task_name,
            task_args=stored_job.task_args,
            meta=stored_job.meta,
            run_at=stored_job.run_at,
            acquired_by=None,
            acquired_until=None,
        )
    )

    with pytest.raises(CustomException):
        async with transaction_manager.begin_transactional() as tx:
            await tx.cancel_job(stored_job.id)
            raise CustomException()

    await job_storage.assert_has_exactly(
        dict(
            id=stored_job.id,
            created_at=stored_job.created_at,
            status=stored_job.status,
            schedule=stored_job.schedule,
            count=stored_job.count,
            task_name=stored_job.task_name,
            task_args=stored_job.task_args,
            meta=stored_job.meta,
            run_at=stored_job.run_at,
            acquired_by=None,
            acquired_until=None,
        ),
        count=1,
    )


async def test_sqlalchemy_transaction_add_job_commit(
    stored_job_factory: type[StoredJobFactory],
    job_storage: BaseStorage,
    storage_transaction: StorageTransaction[Any],
    transaction_manager: sx.TransactionManager[Any],
) -> None:
    stored_job = stored_job_factory.build()

    outer_tx = await storage_transaction.begin()
    tx = transaction_manager.within_transaction(outer_tx)
    await tx.add_job(stored_job)
    await storage_transaction.commit()

    await job_storage.assert_has_exactly(
        dict(
            id=stored_job.id,
            created_at=stored_job.created_at,
            status=stored_job.status,
            schedule=stored_job.schedule,
            count=stored_job.count,
            task_name=stored_job.task_name,
            task_args=stored_job.task_args,
            meta=stored_job.meta,
            run_at=stored_job.run_at,
            acquired_by=None,
            acquired_until=None,
        ),
        count=1,
    )


async def test_sqlalchemy_transaction_add_job_rollback(
    stored_job_factory: type[StoredJobFactory],
    job_storage: BaseStorage,
    storage_transaction: StorageTransaction[Any],
    transaction_manager: sx.TransactionManager[Any],
) -> None:
    stored_job = stored_job_factory.build()

    outer_tx = await storage_transaction.begin()
    tx = transaction_manager.within_transaction(outer_tx)
    await tx.add_job(stored_job)
    await storage_transaction.rollback()

    await job_storage.assert_has_exactly({}, count=0)
