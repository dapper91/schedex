import dataclasses as dc

import pymongo.asynchronous.mongo_client as mgcli
import pytest

import schedex as sx
import schedex.contrib.pymongo as sxpm
from tests.factories import StoredJobFactory, StoredTaskFactory

from .storage import DocumentType, JobPyMongoStorage, TaskPyMongoStorage

pytestmark = pytest.mark.mongodb


@pytest.mark.no_leaks
async def test_mongo_event_receiver(
    stored_job_factory: StoredJobFactory,
    stored_task_factory: StoredTaskFactory,
    pymongo_job_storage: JobPyMongoStorage,
    pymongo_task_storage: TaskPyMongoStorage,
    pymongo_client: mgcli.AsyncMongoClient[DocumentType],
    mongodb_dbname: str,
) -> None:
    event_receiver = sxpm.PyMongoEventReceiver(pymongo_client, dbname=mongodb_dbname)

    async with event_receiver.connect() as event_source:
        stored_job = stored_job_factory.build()
        await pymongo_job_storage.insert(dc.asdict(stored_job))
        event = await anext(event_source)
        assert event.kind == sx.EventKind.JobReady

        await pymongo_job_storage.delete(dict(id=stored_job.id))
        event = await anext(event_source)
        assert event.kind == sx.EventKind.JobCanceled

        stored_task = stored_task_factory.build()
        await pymongo_task_storage.insert(dc.asdict(stored_task))
        event = await anext(event_source)
        assert event.kind == sx.EventKind.TaskReady

        await pymongo_task_storage.delete(dict(id=stored_task.id))
        event = await anext(event_source)
        assert event.kind == sx.EventKind.TaskSucceeded
