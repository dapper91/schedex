import contextlib as cl
import datetime as dt
import uuid
from typing import Any, Generator

import sqlalchemy.ext.asyncio as aiosa
from polyfactory.factories import sqlalchemy_factory as sa_factory

from schedex import JobStatus, TaskStatus
from schedex.contrib.sqlalchemy import tables as tb


@cl.contextmanager
def set_factory_async_session[F: type[sa_factory.SQLAlchemyFactory[Any]]](
    db_session: aiosa.AsyncSession,
    factory: F,
) -> Generator[F, None, None]:
    factory.__async_session__ = db_session
    yield factory
    factory.__async_session__ = None


class TaskFactory(sa_factory.SQLAlchemyFactory[tb.Task]):
    id = lambda: uuid.uuid4().hex
    created_at = lambda: dt.datetime.now(tz=dt.timezone.utc)
    task_name = "task_name"
    task_args = b"task_args"
    meta = b"meta"
    run_at = lambda: dt.datetime.now(tz=dt.timezone.utc)
    acquired_by = None
    acquired_until = None

    @classmethod
    def pending(cls) -> type["TaskFactory"]:
        return cls.create_factory(status=TaskStatus.Pending, attempts=0)

    @classmethod
    def succeeded(cls) -> type["TaskFactory"]:
        return cls.create_factory(status=TaskStatus.Succeeded, attempts=0)

    @classmethod
    def failed(cls) -> type["TaskFactory"]:
        return cls.create_factory(status=TaskStatus.Failed, attempts=1)

    @classmethod
    def error(cls) -> type["TaskFactory"]:
        return cls.create_factory(status=TaskStatus.Error, attempts=1)

    @classmethod
    def executing(cls, acquired_by: str, acquired_until: dt.datetime) -> type["TaskFactory"]:
        return cls.create_factory(status=TaskStatus.Executing, acquired_by=acquired_by, acquired_until=acquired_until)


class JobFactory(sa_factory.SQLAlchemyFactory[tb.Job]):
    id = lambda: uuid.uuid4().hex
    created_at = lambda: dt.datetime.now(tz=dt.timezone.utc)
    status = JobStatus.Active
    schedule = b"schedule"
    count = 0
    task_name = "task_name"
    task_args = b"task_args"
    meta = b"meta"
    run_at = lambda: dt.datetime.now(tz=dt.timezone.utc)
    acquired_by = None
    acquired_until = None
