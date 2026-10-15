import datetime as dt
from typing import Any, Callable, Optional

import pymongo.asynchronous.mongo_client as mgcli
import pytest
from pytest_lazy_fixtures import lf

import schedex as sx
import schedex.contrib.pymongo as sxpm
from tests.types import AsyncFixture

from .storage import DocumentType, JobPyMongoStorage, PyMongoStorageTransaction, TaskPyMongoStorage


@pytest.fixture(scope="session")
def mongodb_dbname() -> str:
    return "test"


@pytest.fixture(scope="session")
def mongodb_url(pytestconfig: pytest.Config) -> str:
    url: Optional[str] = pytestconfig.getoption("--mongodb-url", default=None, skip=True)
    if url is None:
        raise pytest.UsageError("--mongodb-url is required")

    return url


@pytest.fixture(
    scope="session",
    params=[
        pytest.param(lf("mongodb_url"), marks=pytest.mark.mongodb, id="mongodb"),
    ],
)
def mongodb_database_url(pytestconfig: pytest.Config, request: pytest.FixtureRequest) -> str:
    url: str = request.param
    return url


@pytest.fixture(scope="package")
async def pymongo_client(mongodb_database_url: str) -> AsyncFixture[mgcli.AsyncMongoClient[DocumentType]]:
    async with mgcli.AsyncMongoClient(mongodb_database_url) as cli:
        yield cli


@pytest.fixture
async def pymongo_database_client(
    pymongo_client: mgcli.AsyncMongoClient[DocumentType], mongodb_dbname: str
) -> AsyncFixture[mgcli.AsyncMongoClient[DocumentType]]:
    database = pymongo_client.get_database(mongodb_dbname)
    await sxpm.create_schema(database)

    yield pymongo_client

    await database.get_collection(sxpm.TASKS_COLLECTION).drop()
    await database.get_collection(sxpm.JOBS_COLLECTION).drop()


@pytest.fixture
def pymongo_transaction_manager(
    pymongo_database_client: mgcli.AsyncMongoClient[DocumentType], mongodb_dbname: str
) -> sxpm.PyMongoTransactionManager:
    return sxpm.PyMongoTransactionManager(pymongo_database_client, dbname=mongodb_dbname)


@pytest.fixture
async def pymongo_lock_manager_builder(
    pymongo_database_client: mgcli.AsyncMongoClient[DocumentType],
    mongodb_dbname: str,
) -> Callable[..., sxpm.PyMongoLeasingLockManager]:
    def builder(**kwargs: Any) -> sxpm.PyMongoLeasingLockManager:
        return sxpm.PyMongoLeasingLockManager(
            pymongo_database_client,
            dbname=mongodb_dbname,
            period=dt.timedelta(seconds=1),
            identifier="lock-id",
            **kwargs,
        )

    return builder


@pytest.fixture
async def pymongo_lock_manager(
    pymongo_lock_manager_builder: Callable[..., sxpm.PyMongoLeasingLockManager],
) -> sxpm.PyMongoLeasingLockManager:
    return pymongo_lock_manager_builder()


@pytest.fixture
def pymongo_scheduler_builder(
    pymongo_database_client: mgcli.AsyncMongoClient[DocumentType],
    mongodb_dbname: str,
) -> Callable[..., sxpm.PyMongoScheduler[sx.PeriodicSchedule]]:
    def builder(**kwargs: Any) -> sxpm.PyMongoScheduler[sx.PeriodicSchedule]:
        return sxpm.PyMongoScheduler(
            pymongo_database_client,
            schedule_type=sx.PeriodicSchedule,
            dbname=mongodb_dbname,
            **kwargs,
        )

    return builder


@pytest.fixture
def pymongo_scheduler(
    pymongo_scheduler_builder: Callable[..., sxpm.PyMongoScheduler[sx.PeriodicSchedule]],
) -> sxpm.PyMongoScheduler[sx.PeriodicSchedule]:
    return pymongo_scheduler_builder()


@pytest.fixture
def pymongo_transactional_scheduler_builder(
    pymongo_database_client: mgcli.AsyncMongoClient[DocumentType],
    mongodb_dbname: str,
) -> Callable[..., sxpm.PyMongoTransactionalScheduler[sx.PeriodicSchedule]]:
    def builder(**kwargs: Any) -> sxpm.PyMongoTransactionalScheduler[sx.PeriodicSchedule]:
        return sxpm.PyMongoTransactionalScheduler(
            pymongo_database_client,
            schedule_type=sx.PeriodicSchedule,
            dbname=mongodb_dbname,
            **kwargs,
        )

    return builder


@pytest.fixture
def pymongo_transactional_scheduler(
    pymongo_transactional_scheduler_builder: Callable[..., sxpm.PyMongoTransactionalScheduler[sx.PeriodicSchedule]],
) -> sxpm.PyMongoTransactionalScheduler[sx.PeriodicSchedule]:
    return pymongo_transactional_scheduler_builder()


@pytest.fixture
async def pymongo_job_storage(
    pymongo_database_client: mgcli.AsyncMongoClient[DocumentType], mongodb_dbname: str
) -> AsyncFixture[JobPyMongoStorage]:
    async with pymongo_database_client.start_session() as session:
        yield JobPyMongoStorage(session, mongodb_dbname)


@pytest.fixture
async def pymongo_task_storage(
    pymongo_database_client: mgcli.AsyncMongoClient[DocumentType], mongodb_dbname: str
) -> AsyncFixture[TaskPyMongoStorage]:
    async with pymongo_database_client.start_session() as session:
        yield TaskPyMongoStorage(session, mongodb_dbname)


@pytest.fixture
async def pymongo_storage_transaction(
    pymongo_database_client: mgcli.AsyncMongoClient[DocumentType],
) -> AsyncFixture[PyMongoStorageTransaction]:
    async with pymongo_database_client.start_session() as session:
        yield PyMongoStorageTransaction(session)
