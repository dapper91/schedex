import pytest

from tests.factories import StoredJobFactory, StoredTaskFactory
from tests.types import Fixture

pytest_plugins = (
    "tests.sqlalchemy",
    "tests.pymongo",
)


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "unit: unit tests")
    config.addinivalue_line("markers", "postgres: postgresql database tests")
    config.addinivalue_line("markers", "mysql: mysql database tests")
    config.addinivalue_line("markers", "psycopg: psycopg postgresql driver tests")
    config.addinivalue_line("markers", "pymongo: mongodb database tests")
    config.addinivalue_line("markers", "no_leaks: detect asyncio task leaks")


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption("--postgres-url", action="store", help="postgresql database URL")
    parser.addoption("--mysql-url", action="store", help="mysql database URL")
    parser.addoption("--mongodb-url", action="store", help="mongodb database URL")
    parser.addoption("--wait-delay", action="store", default=0.1, type=float, help="wait delay in seconds")


@pytest.fixture(scope="session")
def wait_delay(pytestconfig) -> float:
    return pytestconfig.getoption("--wait-delay", 0.1)


@pytest.fixture(scope="session")
def stored_job_factory() -> Fixture[type[StoredJobFactory]]:
    yield StoredJobFactory


@pytest.fixture(scope="session")
def stored_task_factory() -> Fixture[type[StoredTaskFactory]]:
    yield StoredTaskFactory
