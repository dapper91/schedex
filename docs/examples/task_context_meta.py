import datetime as dt
import uuid
from typing import Any

import sqlalchemy.ext.asyncio as aiosa

import schedex as sx
import schedex.contrib.sqlalchemy as sxsa

tasks = sx.TaskRegistry[dict[str, Any], sxsa.SqlAlchemySfuTaskLock]()


@tasks.task()
async def task_function(context: sx.Context) -> None:
    trace_id = context.task.meta["trace_id"]
    print(f"task trace: {trace_id}")


async def main() -> None:
    db_engine = aiosa.create_async_engine("postgresql+psycopg://user:password@localhost:5432/myapp")

    scheduler = sxsa.SqlAlchemyScheduler(
        aiosa.async_sessionmaker(db_engine),
        schedule_type=sx.PeriodicSchedule,
    )

    trace_id = uuid.uuid4().hex
    await scheduler.add_job(
        sx.Job(
            sx.PeriodicSchedule.periodically(
                start_at=dt.datetime.now(dt.timezone.utc),
                delay=dt.timedelta(seconds=2),
            ).with_max_count(5),
            task_function.with_params(),
            meta={"trace_id": trace_id},
        )
    )
