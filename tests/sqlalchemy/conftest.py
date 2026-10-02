import datetime as dt
from typing import Optional

import pytest
import sqlalchemy as sa
import sqlalchemy.ext.asyncio as aiosa
from pytest_lazy_fixtures import lf

import schedex as sx
import schedex.contrib.sqlalchemy as sxsa
from schedex.contrib.sqlalchemy import tables
from tests.sqlalchemy import utils
from tests.types import AsyncFixture

from .storage import JobSqlAlchemyStorage, SqlAlchemyStorageTransaction, TaskSqlAlchemyStorage

SessionMaker = sa.ext.asyncio.async_sessionmaker[sa.ext.asyncio.AsyncSession]


@pytest.fixture(scope="session")
def postgres_dialect() -> str:
    return "psycopg"


@pytest.fixture(scope="session")
def postgres_url(pytestconfig: pytest.Config, postgres_dialect: str) -> sa.URL:
    url_str: Optional[str] = pytestconfig.getoption("--postgres-url", default=None, skip=True)
    if url_str is None:
        raise pytest.UsageError("--postgres-url is required")

    url = sa.make_url(url_str)
    if url.drivername not in ("postgresql", f"postgresql+{postgres_dialect}"):
        raise pytest.UsageError(f"driver {url.drivername} not supported")

    return url.set(drivername=f"postgresql+{postgres_dialect}")


@pytest.fixture(scope="session")
def mysql_dialect() -> str:
    return "asyncmy"


@pytest.fixture(scope="session")
def mysql_url(pytestconfig: pytest.Config, mysql_dialect: str) -> sa.URL:
    url_str: Optional[str] = pytestconfig.getoption("--mysql-url", default=None, skip=True)
    if url_str is None:
        raise pytest.UsageError("--mysql-url is required")

    url = sa.make_url(url_str)
    if url.drivername not in ("mysql", f"mysql+{mysql_dialect}"):
        raise pytest.UsageError(f"driver {url.drivername} not supported")

    return url.set(drivername=f"mysql+{mysql_dialect}")


@pytest.fixture(
    scope="session",
    params=[
        pytest.param(lf("postgres_url"), marks=pytest.mark.postgres, id="postgres"),
        pytest.param(lf("mysql_url"), marks=pytest.mark.mysql, id="mysql"),
    ],
)
def sa_database_url(pytestconfig: pytest.Config, request: pytest.FixtureRequest) -> sa.URL:
    url: sa.URL = request.param
    return url


@pytest.fixture(scope="package")
async def sa_engine(sa_database_url: sa.URL) -> AsyncFixture[aiosa.AsyncEngine]:
    engine = sa.ext.asyncio.create_async_engine(sa_database_url, poolclass=utils.TrackingConnectionPool)
    yield engine
    await engine.dispose()


@pytest.fixture
async def sa_db_engine(sa_engine: aiosa.AsyncEngine) -> AsyncFixture[aiosa.AsyncEngine]:
    assert isinstance(sa_engine.pool, utils.TrackingConnectionPool)

    async with sa_engine.begin() as conn:
        await conn.run_sync(tables.BaseModel.metadata.create_all)

    yield sa_engine

    checked_out_connections = await sa_engine.pool.invalidate_checked_out()
    async with sa_engine.begin() as conn:
        await conn.run_sync(tables.BaseModel.metadata.drop_all)

    assert not checked_out_connections, "some connections leaked out of the pool"


@pytest.fixture
def sa_session_maker(sa_db_engine: aiosa.AsyncEngine) -> SessionMaker:
    return sa.ext.asyncio.async_sessionmaker(sa_db_engine)


@pytest.fixture
async def sa_session(
    sa_session_maker: aiosa.async_sessionmaker[aiosa.AsyncSession],
) -> AsyncFixture[aiosa.AsyncSession]:
    async with sa_session_maker() as session:
        yield session


@pytest.fixture
def sa_transaction_manager(
    sa_session_maker: sa.ext.asyncio.async_sessionmaker[sa.ext.asyncio.AsyncSession],
) -> sxsa.SqlAlchemyTransactionManager:
    return sxsa.SqlAlchemyTransactionManager(sa_session_maker)


@pytest.fixture
def sa_scheduler(
    sa_session_maker: sa.ext.asyncio.async_sessionmaker[sa.ext.asyncio.AsyncSession],
) -> sxsa.SqlAlchemyScheduler[sx.PeriodicSchedule]:
    return sxsa.SqlAlchemyScheduler(sa_session_maker, schedule_type=sx.PeriodicSchedule)


@pytest.fixture
def sa_transactional_scheduler(
    sa_session_maker: sa.ext.asyncio.async_sessionmaker[sa.ext.asyncio.AsyncSession],
) -> sxsa.SqlAlchemyTransactionalScheduler[sx.PeriodicSchedule]:
    return sxsa.SqlAlchemyTransactionalScheduler(sa_session_maker, schedule_type=sx.PeriodicSchedule)


@pytest.fixture
async def sa_sfu_lock_manager(sa_db_engine: aiosa.AsyncEngine) -> AsyncFixture[sxsa.SqlAlchemySfuLockManager]:
    yield sxsa.SqlAlchemySfuLockManager(sa_db_engine)


@pytest.fixture
async def sa_leasing_lock_manager(sa_db_engine: aiosa.AsyncEngine) -> AsyncFixture[sxsa.SqlAlchemyLeasingLockManager]:
    yield sxsa.SqlAlchemyLeasingLockManager(sa_db_engine, period=dt.timedelta(seconds=1), identifier="lock-id")


@pytest.fixture
async def sa_job_storage(
    sa_session_maker: sa.ext.asyncio.async_sessionmaker[sa.ext.asyncio.AsyncSession],
) -> AsyncFixture[JobSqlAlchemyStorage]:
    async with sa_session_maker() as session:
        yield JobSqlAlchemyStorage(session)


@pytest.fixture
async def sa_task_storage(
    sa_session_maker: sa.ext.asyncio.async_sessionmaker[sa.ext.asyncio.AsyncSession],
) -> AsyncFixture[TaskSqlAlchemyStorage]:
    async with sa_session_maker() as session:
        yield TaskSqlAlchemyStorage(session)


@pytest.fixture
async def sa_storage_transaction(
    sa_session_maker: sa.ext.asyncio.async_sessionmaker[sa.ext.asyncio.AsyncSession],
) -> AsyncFixture[SqlAlchemyStorageTransaction]:
    async with sa_session_maker() as session:
        yield SqlAlchemyStorageTransaction(session)
