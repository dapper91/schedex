import argparse
import asyncio as aio
import contextvars as cv
import datetime as dt
import logging
import uuid
from typing import Any, MutableMapping, cast

import sqlalchemy.ext.asyncio as aiosa

import schedex as sx
import schedex.contrib.sqlalchemy as sxsa

AppState = MutableMapping[str, Any]
TaskLock = sxsa.SqlAlchemySfuTaskLock
Context = sx.Context[AppState, TaskLock]

tasks = sx.TaskRegistry[AppState, TaskLock]()
request_id = cv.ContextVar[str]("request_id")


@tasks.task(name="print")
async def print_message(context: Context, message: str, /) -> None:
    print(f"request-id[{request_id.get()}]: {message}")


async def set_request_id_middleware(
    context: Context,
    task: sx.Task[Context, Any, Any],
    /,
    *,
    wrapped: sx.ExecutorMiddlewareWrappedFunc[Context],
) -> None:
    job_request_id = cast(str, context.task.meta["X-Request-Id"])
    request_id.set(job_request_id)
    await wrapped(context, task)


async def start_scheduler() -> None:
    db_engine = aiosa.create_async_engine("postgresql+psycopg://user:password@localhost:5432/myapp")

    async with db_engine.begin() as conn:
        await conn.run_sync(sxsa.BaseModel.metadata.create_all)

    scheduler = sxsa.SqlAlchemyTransactionalScheduler(
        aiosa.async_sessionmaker(db_engine),
        schedule_type=sx.PeriodicSchedule,
    )

    job_request_id = str(uuid.uuid4())
    await scheduler.add_job(
        sx.Job(
            sx.PeriodicSchedule.periodically(
                start_at=dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=0),
                delay=dt.timedelta(seconds=2),
            ).with_max_count(5),
            print_message.with_params("hello world!!!"),
            meta={"X-Request-Id": job_request_id},
        )
    )


async def start_executor() -> None:
    db_engine = aiosa.create_async_engine("postgresql+psycopg://user:password@localhost:5432/myapp")

    async with db_engine.begin() as conn:
        await conn.run_sync(sxsa.BaseModel.metadata.create_all)

    app_state: AppState = {}
    executor = sx.WorkerPoolExecutor(
        max_workers=3,
        state=app_state,
        polling_interval=dt.timedelta(seconds=1),
        schedule_type=sx.PeriodicSchedule,
        task_registry=tasks,
        lock_manager=sxsa.SqlAlchemySfuLockManager(db_engine),
        middlewares=[set_request_id_middleware],
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
