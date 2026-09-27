import asyncio as aio
import datetime as dt
import logging
from typing import Any, MutableMapping

import sqlalchemy.ext.asyncio as aiosa

import schedex as sx
import schedex.contrib.sqlalchemy as sxsa

AppState = MutableMapping[str, Any]
TaskLock = sxsa.SqlAlchemySfuTaskLock
Context = sx.Context[AppState, TaskLock]

tasks = sx.TaskRegistry[AppState, TaskLock]()


@tasks.task(name="print")
async def print_message(context: Context, message: str, /, *, end: str = "\n") -> None:
    print(message, end=end)


async def main() -> None:
    logging.basicConfig(level=logging.INFO)

    db_engine = aiosa.create_async_engine("postgresql+psycopg://user:password@localhost:5432/myapp")
    async with db_engine.begin() as conn:
        await conn.run_sync(sxsa.BaseModel.metadata.create_all)

    event_sender, event_receiver = sx.mem.eventbus.build(queue_maxsize=128)

    executor_task = aio.create_task(start_executor(db_engine, event_sender, event_receiver))
    await aio.sleep(2.0)  # wait for executor to be initialized
    scheduler_task = aio.create_task(schedule_jobs(db_engine, event_sender))

    await aio.wait((executor_task, scheduler_task), return_when=aio.FIRST_EXCEPTION)


async def schedule_jobs(db_engine: aiosa.AsyncEngine, event_sender: sx.EventSender) -> None:
    scheduler = sxsa.SqlAlchemyTransactionalScheduler(
        aiosa.async_sessionmaker(db_engine),
        schedule_type=sx.PeriodicSchedule,
        event_sender=event_sender,
    )
    await scheduler.add_job(
        sx.Job(
            sx.PeriodicSchedule.periodically(
                start_at=dt.datetime.now(dt.timezone.utc),
                delay=dt.timedelta(seconds=1),
            ).with_max_count(5),
            print_message.with_params("hello world!!!"),
        )
    )


async def start_executor(
    db_engine: aiosa.AsyncEngine, event_sender: sx.EventSender, event_receiver: sx.EventReceiver
) -> None:
    executor = sx.WorkerPoolExecutor(
        max_workers=1,
        state={},
        polling_interval=dt.timedelta(seconds=30),
        schedule_type=sx.PeriodicSchedule,
        task_registry=tasks,
        lock_manager=sxsa.SqlAlchemySfuLockManager(db_engine),
        event_sender=event_sender,
        event_receiver=event_receiver,
    )

    await executor.run()


aio.run(main())
