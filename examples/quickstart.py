import argparse
import asyncio as aio
import datetime as dt
import logging
from typing import Any, MutableMapping

import psycopg_pool as ppg_pool
import sqlalchemy.ext.asyncio as aiosa

import schedex as sx
import schedex.contrib.psycopg as sxpg
import schedex.contrib.sqlalchemy as sxsa

AppState = MutableMapping[str, Any]
TaskLock = sxsa.SqlAlchemySfuTaskLock
Context = sx.Context[AppState, TaskLock]

tasks = sx.TaskRegistry[AppState, TaskLock]()


@tasks.task(name="print")
async def print_message(context: Context, message: str, /, *, end: str = "\n") -> None:
    print(message, end=end)


async def start_scheduler() -> None:
    db_engine = aiosa.create_async_engine("postgresql+psycopg://user:password@localhost:5432/myapp")

    async with db_engine.begin() as conn:
        await conn.run_sync(sxsa.BaseModel.metadata.create_all)

    async with ppg_pool.AsyncConnectionPool("postgresql://user:password@localhost:5432/myapp") as pg_pool:
        event_sender = sxpg.PsycopgEventSender(pg_pool)

        async with sxsa.SqlAlchemyTransactionalScheduler(
            aiosa.async_sessionmaker(db_engine),
            schedule_type=sx.PeriodicSchedule,
            event_sender=event_sender,
            graceful_timeout=0.1,
        ) as scheduler:
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
    db_engine = aiosa.create_async_engine("postgresql+psycopg://user:password@localhost:5432/myapp")
    async with db_engine.begin() as conn:
        await conn.run_sync(sxsa.BaseModel.metadata.create_all)

    app_state: AppState = {}

    async with ppg_pool.AsyncConnectionPool("postgresql://user:password@localhost:5432/myapp") as pg_pool:
        executor = sx.WorkerPoolExecutor(
            max_workers=3,
            state=app_state,
            polling_interval=dt.timedelta(seconds=15),
            schedule_type=sx.PeriodicSchedule,
            task_registry=tasks,
            lock_manager=sxsa.SqlAlchemySfuLockManager(db_engine, sxpg.PsycopgEventSender(pg_pool)),
            event_receiver=sxpg.PsycopgEventReceiver(pg_pool),
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
