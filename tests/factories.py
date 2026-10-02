import datetime as dt
import uuid

import polyfactory as pf

import schedex as sx
import schedex.serializer.json


class StoredJobFactory(pf.factories.DataclassFactory[sx.StoredJob]):
    id = lambda: uuid.uuid4().hex
    created_at = lambda: dt.datetime.now(tz=dt.timezone.utc)
    status = sx.JobStatus.Active
    schedule = b'{"type":"periodic","start_at":"2026-10-01T22:30:00","delay":null}'
    count = 0
    task_name = "task_name"
    task_args = b'{"args": [], "kwargs": {}}'
    meta = b'{"key":"value"}'
    run_at = lambda: dt.datetime.now(tz=dt.timezone.utc)
    acquired_by = None
    acquired_until = None


class StoredTaskFactory(pf.factories.DataclassFactory[sx.StoredTask]):
    id = lambda: uuid.uuid4().hex
    job_id: str
    sequence_number: int = 0
    created_at = lambda: dt.datetime.now(tz=dt.timezone.utc)
    status: sx.TaskStatus
    attempts: int = 0
    task_name = "task_name"
    task_args = b'{"args": [], "kwargs": {}}'
    meta = b'{"key":"value"}'
    run_at = lambda: dt.datetime.now(tz=dt.timezone.utc)
    acquired_by = None
    acquired_until = None

    @classmethod
    def pending(cls) -> type["StoredTaskFactory"]:
        return cls.create_factory(status=sx.TaskStatus.Pending, attempts=0)

    @classmethod
    def succeeded(cls) -> type["StoredTaskFactory"]:
        return cls.create_factory(status=sx.TaskStatus.Succeeded, attempts=0)

    @classmethod
    def failed(cls) -> type["StoredTaskFactory"]:
        return cls.create_factory(status=sx.TaskStatus.Failed, attempts=1)

    @classmethod
    def error(cls) -> type["StoredTaskFactory"]:
        return cls.create_factory(status=sx.TaskStatus.Error, attempts=1)

    @classmethod
    def executing(cls, acquired_by: str, acquired_until: dt.datetime) -> type["StoredTaskFactory"]:
        return cls.create_factory(
            status=sx.TaskStatus.Executing, acquired_by=acquired_by, acquired_until=acquired_until
        )


class JsonSerializableBoundTask(sx.serializer.json.JsonSerializable, sx.BoundTask):
    pass


class JobFactory(pf.factories.DataclassFactory[sx.Job]):
    id = lambda: uuid.uuid4().hex
    schedule = sx.PeriodicSchedule.after(delay=dt.timedelta(seconds=1))
    meta = {"key": "value"}
    task = JsonSerializableBoundTask(name="test_task", args=(), kwargs={})
