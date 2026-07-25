from typing import Optional

import pytest
import sqlalchemy as sa
import sqlalchemy.ext.asyncio as aiosa
from pytest_lazy_fixtures import lf

from schedex.contrib.sqlalchemy import tables
from tests.sqlalchemy import utils
from tests.sqlalchemy.factory import JobFactory, TaskFactory, set_factory_async_session
from tests.sqlalchemy.tables import JobTable, TaskTable
from tests.types import AsyncFixture, Fixture

SessionMaker = sa.ext.asyncio.async_sessionmaker[sa.ext.asyncio.AsyncSession]


@pytest.fixture(scope="package")
def postgres_dialect() -> str:
    return "psycopg"


@pytest.fixture(scope="package")
def postgres_url(pytestconfig: pytest.Config, postgres_dialect: str) -> sa.URL:
    url_str: Optional[str] = pytestconfig.getoption("--postgres-url", default=None, skip=True)
    if url_str is None:
        raise pytest.UsageError("--postgres-url is required")

    url = sa.make_url(url_str)
    if url.drivername not in ("postgresql", f"postgresql+{postgres_dialect}"):
        raise pytest.UsageError(f"driver {url.drivername} not supported")

    return url.set(drivername=f"postgresql+{postgres_dialect}")


@pytest.fixture(scope="package")
def mysql_dialect() -> str:
    return "asyncmy"


@pytest.fixture(scope="package")
def mysql_url(pytestconfig: pytest.Config, mysql_dialect: str) -> sa.URL:
    url_str: Optional[str] = pytestconfig.getoption("--mysql-url", default=None, skip=True)
    if url_str is None:
        raise pytest.UsageError("--mysql-url is required")

    url = sa.make_url(url_str)
    if url.drivername not in ("mysql", f"mysql+{mysql_dialect}"):
        raise pytest.UsageError(f"driver {url.drivername} not supported")

    return url.set(drivername=f"mysql+{mysql_dialect}")


@pytest.fixture(
    scope="package",
    params=[
        pytest.param(lf("postgres_url"), marks=pytest.mark.postgres, id="postgres"),
        pytest.param(lf("mysql_url"), marks=pytest.mark.mysql, id="mysql"),
    ],
)
def database_url(pytestconfig: pytest.Config, request: pytest.FixtureRequest) -> sa.URL:
    url: sa.URL = request.param
    return url


@pytest.fixture(scope="package")
async def sa_engine(database_url: sa.URL) -> AsyncFixture[aiosa.AsyncEngine]:
    engine = sa.ext.asyncio.create_async_engine(database_url, poolclass=utils.TrackingConnectionPool)
    yield engine
    await engine.dispose()


@pytest.fixture(scope="package")
async def sa_database(sa_engine: aiosa.AsyncEngine) -> AsyncFixture[aiosa.AsyncEngine]:
    assert isinstance(sa_engine.pool, utils.TrackingConnectionPool)

    async with sa_engine.begin() as conn:
        await conn.run_sync(tables.BaseModel.metadata.create_all)

    yield sa_engine

    checked_out_connections = await sa_engine.pool.invalidate_checked_out()
    async with sa_engine.begin() as conn:
        await conn.run_sync(tables.BaseModel.metadata.drop_all)

    assert not checked_out_connections, "some connections leaked out of the pool"


@pytest.fixture(scope="package")
def sa_session_maker(sa_database: aiosa.AsyncEngine) -> SessionMaker:
    return sa.ext.asyncio.async_sessionmaker(sa_database)


@pytest.fixture(scope="package")
async def sa_session(
    sa_session_maker: aiosa.async_sessionmaker[aiosa.AsyncSession],
) -> AsyncFixture[aiosa.AsyncSession]:
    async with sa_session_maker() as session:
        yield session


@pytest.fixture(scope="package")
def job_factory(sa_session: aiosa.AsyncSession) -> Fixture[type[JobFactory]]:
    with set_factory_async_session(sa_session, JobFactory) as factory:
        yield factory


@pytest.fixture(scope="package")
async def job_table(sa_session: aiosa.AsyncSession):
    return JobTable(sa_session)


@pytest.fixture(scope="package")
def task_factory(sa_session: aiosa.AsyncSession) -> Fixture[type[TaskFactory]]:
    with set_factory_async_session(sa_session, TaskFactory) as factory:
        yield factory


@pytest.fixture(scope="package")
async def task_table(sa_session: aiosa.AsyncSession):
    return TaskTable(sa_session)
