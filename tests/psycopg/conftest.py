from typing import Optional

import psycopg_pool as ppg_pool
import pytest

from tests.types import AsyncFixture


@pytest.fixture(scope="package")
def database_url(pytestconfig: pytest.Config) -> str:
    url: Optional[str] = pytestconfig.getoption("--postgres-url", default=None, skip=True)
    if url is None:
        raise pytest.UsageError("--postgres-url is required")

    driver, sep, url = url.partition("://")
    if driver not in ("postgresql", "postgresql+psycopg"):
        raise pytest.UsageError(f"driver {driver} not supported")

    return f"postgresql://{url or ''}"


@pytest.fixture
async def connection_pool(database_url: str) -> AsyncFixture[ppg_pool.AsyncConnectionPool]:
    async with ppg_pool.AsyncConnectionPool(database_url) as pool:
        yield pool
