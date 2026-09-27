import pytest


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "unit: unit tests")
    config.addinivalue_line("markers", "postgres: postgresql database tests")
    config.addinivalue_line("markers", "mysql: mysql database tests")
    config.addinivalue_line("markers", "psycopg: psycopg postgresql driver tests")


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption("--postgres-url", action="store", help="postgresql database URL")
    parser.addoption("--mysql-url", action="store", help="mysql database URL")
