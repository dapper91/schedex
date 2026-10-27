import argparse
import asyncio as aio
import datetime as dt
import logging
import random as rnd
from typing import Any, MutableMapping

import sqlalchemy.ext.asyncio as aiosa

import schedex as sx
import schedex.contrib.sqlalchemy as sxsa

AppState = MutableMapping[str, Any]
TaskLock = sxsa.SqlAlchemySfuTaskLock
Context = sx.Context[AppState, TaskLock]

tasks = sx.TaskRegistry[AppState, TaskLock]()


async def retry_task_middleware(
    context: Context,
    task: sx.Task[Context, Any, Any],
    /,
    *,
    wrapped: sx.ExecutorMiddlewareWrappedFunc[Context],
) -> None:
    max_attempts = context.state.get("max_attempts", 1)

    try:
        await wrapped(context, task)
    except Exception as e:
        if context.lock.task.attempts < max_attempts - 1:
            delay = dt.timedelta(seconds=1) * (context.lock.task.attempts + 1)
            logging.warning("task execution failed, retrying after %s: %s", delay, e)
            await context.lock.postpone(delay=delay)
        else:
            raise e


@tasks.task(name="print")
async def print_message(context: Context, message: str, /, *, end: str = "\n") -> None:
    if rnd.random() < 0.9:
        raise Exception("print message error")

    print(message, end=end)


async def start_scheduler() -> None:
    db_engine = aiosa.create_async_engine("postgresql+psycopg://user:password@localhost:5432/myapp")

    async with db_engine.begin() as conn:
        await conn.run_sync(sxsa.BaseModel.metadata.create_all)

    scheduler = sxsa.SqlAlchemyScheduler(
        aiosa.async_sessionmaker(db_engine),
        schedule_type=sx.PeriodicSchedule,
    )
    await scheduler.add_job(
        sx.Job(
            sx.PeriodicSchedule.after(dt.timedelta(seconds=0)),
            print_message.with_params("hello world!!!"),
        )
    )


async def start_executor() -> None:
    db_engine = aiosa.create_async_engine("postgresql+psycopg://user:password@localhost:5432/myapp")
    async with db_engine.begin() as conn:
        await conn.run_sync(sxsa.BaseModel.metadata.create_all)

    app_state: AppState = {
        "max_attempts": 5,
    }

    executor = sx.WorkerPoolExecutor(
        max_workers=3,
        state=app_state,
        polling_interval=dt.timedelta(seconds=1),
        schedule_type=sx.PeriodicSchedule,
        task_registry=tasks,
        lock_manager=sxsa.SqlAlchemySfuLockManager(db_engine),
        middlewares=[retry_task_middleware],
    )
    await executor.run()


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]: %(message)s")

parser = argparse.ArgumentParser()
parser.add_argument("action", choices=["scheduler", "executor"])
args = parser.parse_args()

if args.action == "scheduler":
    aio.run(start_scheduler())
elif args.action == "executor":
    aio.run(start_executor())
else:
    raise AssertionError("unreachable")
