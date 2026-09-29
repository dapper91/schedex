import argparse
import asyncio as aio
import datetime as dt
import logging
import uuid
from typing import Any, MutableMapping

import pymongo.asynchronous.mongo_client as aiomc

import schedex as sx
import schedex.contrib.pymongo as sxpm

AppState = MutableMapping[str, Any]
TaskLock = sxpm.PyMongoLeasingTaskLock
Context = sx.Context[AppState, TaskLock]

tasks = sx.TaskRegistry[AppState, TaskLock]()


@tasks.task(name="print")
async def print_message(context: Context, message: str, /, *, end: str = "\n") -> None:
    print(message, end=end)


async def start_scheduler() -> None:
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
            print_message.with_params("let's get started"),
        )
    )

    async with scheduler.transactional() as scheduler_tx:
        await scheduler_tx.add_job(
            sx.Job(
                sx.PeriodicSchedule.periodically(
                    start_at=dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=0),
                    delay=dt.timedelta(seconds=2),
                ).with_max_count(5),
                print_message.with_params("hello", end=" "),
            )
        )
        await scheduler_tx.add_job(
            sx.Job(
                sx.PeriodicSchedule.periodically(
                    start_at=dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=1),
                    delay=dt.timedelta(seconds=2),
                ).with_max_count(5),
                print_message.with_params("world!!!"),
            )
        )


async def start_executor() -> None:
    client = aiomc.AsyncMongoClient("mongodb://user:password@localhost:27017/?replicaSet=rs0")

    await sxpm.create_schema(client.get_database("myapp"))

    app_state: AppState = {}

    worker_id = str(uuid.uuid4())
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
            identifier=worker_id,
        ),
        event_receiver=sxpm.PyMongoEventReceiver(client, dbname="myapp"),
    )
    await executor.run()


logging.basicConfig(level=logging.INFO)

parser = argparse.ArgumentParser()
parser.add_argument("action", choices=["scheduler", "executor"])
args = parser.parse_args()

if args.action == "scheduler":
    aio.run(start_scheduler())
elif args.action == "executor":
    aio.run(start_executor())
else:
    raise AssertionError("unreachable")
