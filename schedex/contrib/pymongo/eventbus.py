import contextlib as cl
import logging
from typing import Any, AsyncGenerator, AsyncIterator, Mapping

import pymongo.asynchronous.mongo_client as pmcli

from schedex import Event, EventKind, EventReceiver
from schedex.runtime import iterator as it

from . import utils
from .schema import JOBS_COLLECTION, TASKS_COLLECTION, DocumentType

logger = logging.getLogger(__name__)


class PyMongoEventReceiver(EventReceiver):
    """
    PyMongo event receiver.

    :param client: PyMongo client
    :param dbname: database name
    """

    def __init__(self, client: pmcli.AsyncMongoClient[DocumentType], dbname: str):
        self._client = client
        self._dbname = dbname

    @cl.asynccontextmanager
    async def connect(self) -> AsyncGenerator[AsyncIterator[Event], None]:
        logger.info("connecting to event source ...")

        async with self._client.start_session() as session:
            watch_pipeline: list[Mapping[str, Any]] = [
                {"$match": {"operationType": {"$in": ["insert", "replace", "delete", "update"]}}},
                {"$project": {"_id": 1, "ns": 1, "operationType": 1}},
            ]
            jobs_collection = utils.get_collection(session, self._dbname, JOBS_COLLECTION)
            job_changes_stream = await jobs_collection.watch(watch_pipeline, session=session)

            tasks_collection = utils.get_collection(session, self._dbname, TASKS_COLLECTION)
            task_changes_stream = await tasks_collection.watch(watch_pipeline, session=session)

            logger.info("event source configured")

            async with job_changes_stream, task_changes_stream:
                async with it.merge_iterators(job_changes_stream, task_changes_stream) as merged:
                    yield self._generate_events(merged)

    async def _generate_events(self, changes_stream: AsyncIterator[DocumentType]) -> AsyncGenerator[Event, None]:
        async for notification in changes_stream:
            collection, operation_type = notification["ns"]["coll"], notification["operationType"]

            if (collection, operation_type) == ("schedex_jobs", "delete"):
                event = Event(kind=EventKind.JobCanceled)
            elif (collection, operation_type) == ("schedex_jobs", "update"):
                event = Event(kind=EventKind.JobReady)
            elif (collection, operation_type) == ("schedex_jobs", "insert"):
                event = Event(kind=EventKind.JobReady)
            elif (collection, operation_type) == ("schedex_jobs", "replace"):
                event = Event(kind=EventKind.JobReady)
            elif (collection, operation_type) == ("schedex_tasks", "delete"):
                event = Event(kind=EventKind.TaskSucceeded)
            elif (collection, operation_type) == ("schedex_tasks", "update"):
                event = Event(kind=EventKind.TaskReady)
            elif (collection, operation_type) == ("schedex_tasks", "insert"):
                event = Event(kind=EventKind.TaskReady)
            elif (collection, operation_type) == ("schedex_tasks", "replace"):
                event = Event(kind=EventKind.TaskReady)
            else:
                continue

            logger.debug("received event: %s", event)

            yield event
