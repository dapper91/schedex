import datetime as dt
import inspect
import typing
from typing import Any, Mapping, NotRequired, Optional, TypedDict, Union

import pymongo as pm
import pymongo.asynchronous.mongo_client as pmcli
import pymongo.errors

from schedex import JobStatus, TaskStatus

type DocumentType = Mapping[str, Any]

JOBS_COLLECTION = "schedex_jobs"


class Job(TypedDict):
    # job unique identifier
    id: str
    # job creation timestamp
    created_at: dt.datetime
    # job status
    status: JobStatus
    # job schedule
    schedule: bytes
    # job run count
    count: int
    # task name to be executed
    task_name: str
    # task args
    task_args: bytes
    # job metadata
    meta: bytes
    # job next run time
    run_at: dt.datetime
    # identifier of the lock that acquired the job
    acquired_by: NotRequired[Optional[str]]
    # timestamp until the lock acquired the job
    acquired_until: NotRequired[Optional[dt.datetime]]


async def create_jobs_schema(database: pmcli.database.AsyncDatabase[DocumentType], validate: bool = True) -> None:
    collection = database.get_collection(JOBS_COLLECTION)
    session = database.client.start_session()

    try:
        await database.create_collection(
            JOBS_COLLECTION,
            session=session,
            validator=_build_bson_schema(Job) if validate else None,
            check_exists=True,
        )
    except pm.errors.CollectionInvalid:
        pass

    await collection.create_indexes(
        [
            pm.IndexModel(
                [("id", pm.DESCENDING)],
                unique=True,
                name="id_unique",
            ),
            pm.IndexModel(
                [("run_at", pm.DESCENDING)],
                name="run_at",
            ),
            pm.IndexModel(
                [("acquired_until", pm.DESCENDING)],
                name="acquired_until",
            ),
        ],
        session=session,
    )


TASKS_COLLECTION = "schedex_tasks"


class Task(TypedDict):
    # task unique identifier
    id: str
    # task creation timestamp
    created_at: dt.datetime
    # task status
    status: TaskStatus
    # job identifier which spawned the task
    job_id: str
    # task sequence number (unique within a job)
    sequence_number: int
    # task execution attempts
    attempts: int
    # task name
    task_name: str
    # task args
    task_args: bytes
    # task metadata
    meta: bytes
    # task run time
    run_at: dt.datetime
    # identifier of the lock that acquired the task
    acquired_by: NotRequired[Optional[str]]
    # timestamp until the lock acquired the task
    acquired_until: NotRequired[Optional[dt.datetime]]


async def create_tasks_schema(database: pmcli.database.AsyncDatabase[DocumentType], validate: bool = True) -> None:
    collection = database.get_collection(TASKS_COLLECTION)
    session = database.client.start_session()

    try:
        await database.create_collection(
            TASKS_COLLECTION,
            session=session,
            validator=_build_bson_schema(Task) if validate else None,
            check_exists=True,
        )
    except pm.errors.CollectionInvalid:
        pass

    await collection.create_indexes(
        [
            pm.IndexModel(
                [("id", pm.DESCENDING)],
                unique=True,
                name="id_unique",
            ),
            pm.IndexModel(
                [("job_id", pm.DESCENDING)],
                name="job_id",
            ),
            pm.IndexModel(
                [("run_at", pm.DESCENDING)],
                name="run_at",
            ),
            pm.IndexModel(
                [("acquired_until", pm.DESCENDING)],
                name="acquired_until",
            ),
        ],
        session=session,
    )


async def create_schema(database: pmcli.database.AsyncDatabase[DocumentType], validate: bool = True) -> None:
    await create_jobs_schema(database, validate)
    await create_tasks_schema(database, validate)


def _build_bson_schema(doc: type[Any]) -> dict[str, Any]:
    assert hasattr(doc, "__annotations__") and hasattr(doc, "__required_keys__"), "argument must be a TypedDict"

    bson_type_map: Mapping[type, str] = {
        str: "string",
        int: "int",
        float: "double",
        bool: "bool",
        bytes: "binData",
        dict: "object",
        dt.date: "date",
        dt.datetime: "date",
        type(None): "null",
    }

    properties: dict[str, dict[str, Any]] = {}
    for field_name, field_type in doc.__annotations__.items():
        schema = properties[field_name] = {"bsonType": []}

        if typing.get_origin(field_type) is NotRequired:  # type: ignore[comparison-overlap]
            field_type = typing.get_args(field_type)[0]

        if (origin := typing.get_origin(field_type)) is not None:
            args = typing.get_args(field_type)
            if origin is Union and len(args) == 2 and args[1] is type(None):  # type: ignore[comparison-overlap]
                field_type = args[0]
                schema["bsonType"].append("null")

        for py_type, bson_type in bson_type_map.items():
            if inspect.isclass(field_type) and issubclass(field_type, py_type):
                schema["bsonType"].append(bson_type)
                break
        else:
            raise TypeError(f"type {field_type} is not supported")

    required: list[str] = list(doc.__required_keys__)

    return {
        "$jsonSchema": {
            "bsonType": "object",
            "properties": properties,
            "required": required,
        }
    }
