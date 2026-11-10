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

    scheduler = sxsa.SqlAlchemyScheduler(
        aiosa.async_sessionmaker(db_engine),
        schedule_type=sx.PeriodicSchedule,
    )

    await scheduler.add_job(
        sx.Job(
            sx.PeriodicSchedule.periodically(
                start_at=dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=0),
                delay=dt.timedelta(seconds=2),
            ).with_max_count(5),
            print_message.with_params("hello", end=" "),
        )
    )
    await scheduler.add_job(
        sx.Job(
            sx.PeriodicSchedule.periodically(
                start_at=dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=1),
                delay=dt.timedelta(seconds=2),
            ).with_max_count(5),
            print_message.with_params("world!!!"),
        )
    )
