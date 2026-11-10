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


@tasks.task()
async def print_message(context: Context, message: str, /) -> None:
    print(message)


async def main() -> None:
    logging.basicConfig(level=logging.INFO)

    db_engine = aiosa.create_async_engine("postgresql+psycopg://user:password@localhost:5432/myapp")

    async with db_engine.begin() as conn:
        await conn.run_sync(sxsa.BaseModel.metadata.create_all)

    scheduler = sxsa.SqlAlchemyScheduler(
        aiosa.async_sessionmaker(db_engine),
        schedule_type=sx.PeriodicSchedule,
    )

    job_id = "print-message"
    await scheduler.add_job(
        sx.Job(
            sx.PeriodicSchedule.periodically(
                start_at=dt.datetime.now(dt.timezone.utc),
                delay=dt.timedelta(seconds=1),
            ),
            print_message.with_params("hello world!!!"),
            id=job_id,
        )
    )
    job = await scheduler.get_job(job_id)
    if job and job.status is sx.JobStatus.Active:
        await scheduler.cancel_job(job.id)
        print(f"job {job.id} cancelled")
