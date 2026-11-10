import dataclasses as dc
import datetime as dt

import sqlalchemy.ext.asyncio as aiosa

import schedex as sx
import schedex.contrib.sqlalchemy as sxsa


@dc.dataclass
class AppState:
    db_engine: aiosa.AsyncEngine


tasks = sx.TaskRegistry[AppState, sxsa.SqlAlchemySfuTaskLock]()


@tasks.task()
async def task_function(context: sx.Context) -> None:
    async with context.state.db_engine.begin() as conn:
        await conn.execute("...")


async def main() -> None:
    db_engine = aiosa.create_async_engine("postgresql+psycopg://user:password@localhost:5432/myapp")

    executor = sx.WorkerPoolExecutor(
        state=AppState(
            db_engine=db_engine,
        ),
        max_workers=3,
        polling_interval=dt.timedelta(seconds=60),
        schedule_type=sx.PeriodicSchedule,
        task_registry=tasks,
        lock_manager=sxsa.SqlAlchemySfuLockManager(db_engine),
    )
    await executor.run()
