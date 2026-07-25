import argparse
import asyncio as aio
import datetime as dt
import logging
from typing import Any, MutableMapping

import sqlalchemy.ext.asyncio as aiosa

import schedex as sx
import schedex.contrib.sqlalchemy as sxsa
import schedex.metadata

AppState = MutableMapping[str, Any]
TaskLock = sxsa.SqlAlchemySfuTaskLock
Context = sx.Context[AppState, TaskLock]

tasks = sx.TaskRegistry[AppState, TaskLock]()


@tasks.task()
async def print_message(context: Context, message: str, /, *, end: str = "\n") -> None:
    print(message, end=end)


async def start_scheduler() -> None:
    db_engine = aiosa.create_async_engine("postgresql+psycopg://user:password@localhost:5432/myapp")

    async with db_engine.begin() as conn:
        await conn.run_sync(sxsa.BaseModel.metadata.create_all)

    scheduler = sxsa.SqlAlchemyTransactionalScheduler(
        aiosa.async_sessionmaker(db_engine),
        schedule_type=sx.PeriodicSchedule,
    )

    await scheduler.add_job(
        sx.Job(
            sx.PeriodicSchedule.periodically(
                start_at=dt.datetime.now(dt.timezone.utc),
                delay=dt.timedelta(seconds=2),
            ).with_max_count(5),
            print_message.with_params("hello world!!!"),
        )
    )


async def start_executor() -> None:
    db_engine = aiosa.create_async_engine("postgresql+psycopg://user:password@localhost:5432/myapp")

    async with db_engine.begin() as conn:
        await conn.run_sync(sxsa.BaseModel.metadata.create_all)

    async for task_lock in sx.TaskFetcher(
        polling_interval=dt.timedelta(seconds=1),
        schedule_type=sx.PeriodicSchedule,
        lock_manager=sxsa.SqlAlchemySfuLockManager(db_engine),
    ):
        async with task_lock:
            if task_lock.task.task_name == "print_message":
                bound_task = print_message.bound_task_cls.deserialize(task_lock.task.task_args)
                await print_message(
                    Context(
                        {},
                        sx.TaskInfo(
                            id=task_lock.task.id,
                            job_id=task_lock.task.job_id,
                            sequence_number=task_lock.task.sequence_number,
                            created_at=task_lock.task.created_at,
                            attempts=task_lock.task.attempts,
                            meta=schedex.metadata.metadata_decoder.decode(task_lock.task.meta),
                        ),
                        task_lock,
                    ),
                    *bound_task.args,
                    **bound_task.kwargs,
                )


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
