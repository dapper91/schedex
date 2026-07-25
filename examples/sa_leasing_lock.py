import argparse
import asyncio as aio
import datetime as dt
import logging
import random as rnd
import uuid
from collections import deque
from typing import Any, MutableMapping

import sqlalchemy.ext.asyncio as aiosa

import schedex as sx
import schedex.contrib.sqlalchemy as sxsa

AppState = MutableMapping[str, Any]
TaskLock = sxsa.SqlAlchemyLeasingTaskLock
Context = sx.Context[AppState, TaskLock]

tasks = sx.TaskRegistry[AppState, TaskLock]()


async def download_page(page: str) -> None:
    logging.info("downloading page '%s' ...", page)
    await aio.sleep(rnd.randint(0, 10))
    logging.info("page '%s' downloaded", page)


@tasks.task()
async def download_pages(context: Context, *, pages: list[str]) -> None:
    logging.info("download process started")

    pages = deque(pages)
    while pages:
        page = pages[0]

        if (remain := await context.lock.remain()) is None:
            logging.warning("task lock expired")
            raise RuntimeError() from None

        try:
            await aio.wait_for(download_page(page), timeout=(remain - dt.timedelta(seconds=2)).total_seconds())
        except aio.TimeoutError:
            logging.info("reacquiring task lock for another %s ...", context.lock.period)
            if not await context.lock.extend(context.lock.period):
                logging.warning("lock reacquire failed")
                raise RuntimeError() from None
        else:
            pages.popleft()

    logging.info("task finished successfully")


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
            download_pages.with_params(
                pages=[
                    "https://en.wikipedia.org/wiki/Python_(programming_language)",
                    "https://en.wikipedia.org/wiki/Rust_(programming_language)",
                    "https://en.wikipedia.org/wiki/Linux",
                    "https://en.wikipedia.org/wiki/Firefox",
                    # ...
                ],
            ),
        )
    )


async def start_executor() -> None:
    db_engine = aiosa.create_async_engine("postgresql+psycopg://user:password@localhost:5432/myapp")

    async with db_engine.begin() as conn:
        await conn.run_sync(sxsa.BaseModel.metadata.create_all)

    worker_id = str(uuid.uuid4())
    app_state: AppState = {}
    executor = sx.WorkerPoolExecutor(
        max_workers=3,
        shutdown_timeout=5,
        state=app_state,
        polling_interval=dt.timedelta(seconds=1),
        schedule_type=sx.PeriodicSchedule,
        task_registry=tasks,
        lock_manager=sxsa.SqlAlchemyLeasingLockManager(
            db_engine,
            period=dt.timedelta(seconds=15),
            identifier=worker_id,
        ),
    )

    await executor.run()


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]: %(message)s")

parser = argparse.ArgumentParser()
parser.add_argument("action", choices=["scheduler", "executor"])
args = parser.parse_args()

if args.action == "scheduler":
    aio.run(start_scheduler())
elif args.action == "executor":
    aio.run(start_executor())
else:
    raise AssertionError("unreachable")
