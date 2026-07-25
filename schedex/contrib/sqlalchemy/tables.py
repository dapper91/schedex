import datetime as dt
from typing import Optional

import sqlalchemy as sa
from sqlalchemy import orm
from sqlalchemy.dialects import mysql

import schedex as sx


class UniversalDateTime(sa.TypeDecorator[dt.datetime]):
    """
    Datetime type wrapper.
    Implements universal datetime type behavior for any database backend.
    """

    impl = sa.DateTime
    cache_ok = True

    def process_bind_param(self, value: Optional[dt.datetime], dialect: sa.Dialect) -> Optional[dt.datetime]:
        if value is None:
            return None

        if value.tzinfo is None:
            return value

        return value.astimezone(dt.timezone.utc)

    def process_result_value(self, value: Optional[dt.datetime], dialect: sa.Dialect) -> Optional[dt.datetime]:
        if value is None:
            return None

        return value.replace(tzinfo=dt.timezone.utc)

    def load_dialect_impl(self, dialect: sa.Dialect) -> sa.types.TypeEngine[dt.datetime]:
        if dialect.name == "mysql":
            type_obj = mysql.DATETIME(fsp=6)
        else:
            type_obj = sa.DateTime()

        return dialect.type_descriptor(type_obj)


class BaseModel(orm.DeclarativeBase):
    pass


class Job(orm.MappedAsDataclass, BaseModel, kw_only=True):
    __tablename__ = "schedex_jobs"

    # job unique identifier
    id: orm.Mapped[str] = orm.mapped_column(sa.String(64), primary_key=True)
    # job creation timestamp
    created_at: orm.Mapped[dt.datetime] = orm.mapped_column(
        UniversalDateTime(),
        default_factory=lambda: dt.datetime.now(tz=dt.timezone.utc),
    )
    # job status
    status: orm.Mapped[sx.JobStatus] = orm.mapped_column(
        sa.Enum(sx.JobStatus, native_enum=False),
        default=sx.JobStatus.Active,
    )
    # job schedule
    schedule: orm.Mapped[bytes] = orm.mapped_column(sa.LargeBinary())
    # job run count
    count: orm.Mapped[int] = orm.mapped_column(sa.BigInteger(), default=0)
    # task name to be executed
    task_name: orm.Mapped[str] = orm.mapped_column(sa.String(255))
    # task args
    task_args: orm.Mapped[bytes] = orm.mapped_column(sa.LargeBinary())
    # job metadata
    meta: orm.Mapped[bytes] = orm.mapped_column(sa.LargeBinary())
    # job next run time
    run_at: orm.Mapped[dt.datetime] = orm.mapped_column(UniversalDateTime(), index=True)
    # identifier of the lock that acquired the job
    acquired_by: orm.Mapped[Optional[str]] = orm.mapped_column(sa.String(36), default=None)
    # timestamp until the lock acquired the job
    acquired_until: orm.Mapped[Optional[dt.datetime]] = orm.mapped_column(UniversalDateTime(), index=True, default=None)


class Task(orm.MappedAsDataclass, BaseModel, kw_only=True):
    __tablename__ = "schedex_tasks"

    # task unique identifier
    id: orm.Mapped[str] = orm.mapped_column(sa.String(64), primary_key=True)
    # task creation timestamp
    created_at: orm.Mapped[dt.datetime] = orm.mapped_column(
        UniversalDateTime(),
        default_factory=lambda: dt.datetime.now(tz=dt.timezone.utc),
    )
    # task status
    status: orm.Mapped[sx.TaskStatus] = orm.mapped_column(
        sa.Enum(sx.TaskStatus, native_enum=False),
        default=sx.TaskStatus.Pending,
    )
    # job identifier which spawned the task
    job_id: orm.Mapped[str] = orm.mapped_column(sa.String(64), index=True)
    # task sequence number (unique within a job)
    sequence_number: orm.Mapped[int] = orm.mapped_column(sa.BigInteger())
    # task execution attempts
    attempts: orm.Mapped[int] = orm.mapped_column(sa.BigInteger(), default=0)
    # task name
    task_name: orm.Mapped[str] = orm.mapped_column(sa.String(255))
    # task args
    task_args: orm.Mapped[bytes] = orm.mapped_column(sa.LargeBinary())
    # task metadata
    meta: orm.Mapped[bytes] = orm.mapped_column(sa.LargeBinary())
    # task run time
    run_at: orm.Mapped[dt.datetime] = orm.mapped_column(UniversalDateTime(), index=True)
    # identifier of the lock that acquired the task
    acquired_by: orm.Mapped[Optional[str]] = orm.mapped_column(sa.String(36), default=None)
    # timestamp until the lock acquired the task
    acquired_until: orm.Mapped[Optional[dt.datetime]] = orm.mapped_column(UniversalDateTime(), index=True, default=None)
