import datetime as dt
import logging
from typing import Any, MutableMapping

import sqlalchemy.ext.asyncio as aiosa

import schedex as sx
import schedex.contrib.sqlalchemy as sxsa

AppState = MutableMapping[str, Any]
TaskLock = sxsa.SqlAlchemyLeasingTaskLock
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

    app_state: AppState = {}

    executor = sx.WorkerPoolExecutor(
        state=app_state,
        max_workers=3,
        polling_interval=dt.timedelta(seconds=30),
        schedule_type=sx.PeriodicSchedule,
        task_registry=tasks,
        lock_manager=sxsa.SqlAlchemyLeasingLockManager(
            db_engine,
            period=dt.timedelta(seconds=60),
            autoextend=True,
            autoextend_after=dt.timedelta(seconds=50),
        ),
    )

    await executor.run()
