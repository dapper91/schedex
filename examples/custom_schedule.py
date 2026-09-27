import argparse
import asyncio as aio
import datetime as dt
import logging
import zoneinfo
from typing import Any, MutableMapping, Union

import sqlalchemy.ext.asyncio as aiosa

import schedex as sx
import schedex.contrib.sqlalchemy as sxsa
from schedex import CombinedSchedule, PeriodicSchedule
from schedex.schedule import cron

AppState = MutableMapping[str, Any]
TaskLock = sxsa.SqlAlchemySfuTaskLock
Context = sx.Context[AppState, TaskLock]

tasks = sx.TaskRegistry[AppState, TaskLock]()


class CustomSchedule(CombinedSchedule):
    schedules: tuple[Union[PeriodicSchedule, cron.CronSchedule], ...]


@tasks.task()
async def print_message(context: Context, message: str, /) -> None:
    print(f"{dt.datetime.now()} [{context.task.sequence_number:02}] {message}")


async def start_scheduler() -> None:
    db_engine = aiosa.create_async_engine("postgresql+psycopg://user:password@localhost:5432/myapp")

    async with db_engine.begin() as conn:
        await conn.run_sync(sxsa.BaseModel.metadata.create_all)

    scheduler = sxsa.SqlAlchemyTransactionalScheduler(
        aiosa.async_sessionmaker(db_engine),
        schedule_type=CustomSchedule,
    )

    await scheduler.add_job(
        sx.Job(
            CustomSchedule(
                (
                    PeriodicSchedule.backoff(
                        start_at=dt.datetime.now(tz=dt.UTC),
                        delay=dt.timedelta(seconds=1),
                        multiplier=2.0,
                    ).with_max_count(6),
                    cron.CronSchedule(
                        "* * * * *",
                        timezone=zoneinfo.ZoneInfo("UTC"),
                        start_at=dt.datetime.now(tz=dt.UTC) + dt.timedelta(minutes=1),
                    ),
                )
            ),
            print_message.with_params("ping"),
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
        schedule_type=CustomSchedule,
        task_registry=tasks,
        lock_manager=sxsa.SqlAlchemySfuLockManager(db_engine),
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
