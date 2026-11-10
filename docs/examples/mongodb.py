import datetime as dt
import logging
from typing import Any, MutableMapping

import pymongo.asynchronous.mongo_client as aiomc

import schedex as sx
import schedex.contrib.pymongo as sxpm

AppState = MutableMapping[str, Any]
TaskLock = sxpm.PyMongoLeasingTaskLock
Context = sx.Context[AppState, TaskLock]

tasks = sx.TaskRegistry[AppState, TaskLock]()


@tasks.task(name="print")
async def print_message(context: Context, message: str, /) -> None:
    print(message)


async def start_scheduler() -> None:
    logging.basicConfig(level=logging.INFO)

    client = aiomc.AsyncMongoClient("mongodb://user:password@localhost:27017/?replicaSet=rs0")
    await sxpm.create_schema(client.get_database("myapp"))

    scheduler = sxpm.PyMongoTransactionalScheduler(
        client,
        schedule_type=sx.PeriodicSchedule,
        dbname="myapp",
    )
    await scheduler.add_job(
        sx.Job(
            sx.PeriodicSchedule.after(dt.timedelta(seconds=0)),
            print_message.with_params("hello world!!!"),
        )
    )


async def start_executor() -> None:
    logging.basicConfig(level=logging.INFO)

    client = aiomc.AsyncMongoClient("mongodb://user:password@localhost:27017/?replicaSet=rs0")
    await sxpm.create_schema(client.get_database("myapp"))

    app_state: AppState = {}

    executor = sx.WorkerPoolExecutor(
        max_workers=3,
        state=app_state,
        polling_interval=dt.timedelta(seconds=15),
        schedule_type=sx.PeriodicSchedule,
        task_registry=tasks,
        lock_manager=sxpm.PyMongoLeasingLockManager(
            client,
            dbname="myapp",
            period=dt.timedelta(seconds=15),
        ),
        event_receiver=sxpm.PyMongoEventReceiver(client, dbname="myapp"),
    )
    await executor.run()
