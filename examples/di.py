import argparse
import asyncio as aio
import datetime as dt
import logging
from typing import Any, Iterable, MutableMapping

import dishka as di
import sqlalchemy.ext.asyncio as aiosa

import schedex as sx
import schedex.contrib.dishka as sxdi
import schedex.contrib.sqlalchemy as sxsa

AppState = MutableMapping[str, Any]
TaskLock = sxsa.SqlAlchemySfuTaskLock
Context = sx.Context[AppState, TaskLock]

tasks = sx.TaskRegistry[AppState, TaskLock]()


class MessageSender:
    async def send_message(self, message: str) -> None:
        print(message)


class ComponentProvider(di.Provider):
    @di.provide(scope=di.Scope.APP)
    def message_sender(self) -> Iterable[MessageSender]:
        yield MessageSender()


@tasks.task(name="print")
@sxdi.inject
async def print_message(
    context: Context,
    message: str,
    /,
    message_sender: di.FromDishka[MessageSender] = sxdi.Injected,
) -> None:
    await message_sender.send_message(message)


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
            sx.PeriodicSchedule.after(delay=dt.timedelta(seconds=0)),
            print_message.with_params("hello world!!!"),
        )
    )


async def start_executor() -> None:
    db_engine = aiosa.create_async_engine("postgresql+psycopg://user:password@localhost:5432/myapp")

    async with db_engine.begin() as conn:
        await conn.run_sync(sxsa.BaseModel.metadata.create_all)

    di_container = di.make_async_container(ComponentProvider())

    app_state: AppState = {sxdi.CONTAINER_KEY: di_container}
    executor = sx.WorkerPoolExecutor(
        max_workers=3,
        state=app_state,
        polling_interval=dt.timedelta(seconds=1),
        schedule_type=sx.PeriodicSchedule,
        task_registry=tasks,
        lock_manager=sxsa.SqlAlchemySfuLockManager(db_engine),
        middlewares=[
            sxdi.request_container_middleware,
        ],
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
