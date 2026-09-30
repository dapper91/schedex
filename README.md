[![Downloads][download-badge]][download-url]
[![License][licence-badge]][licence-url]
[![Python Versions][python-version-badge]][python-version-url]
[![Build status][build-badge]][build-url]

[download-badge]: https://static.pepy.tech/personalized-badge/schedex?period=month&units=international_system&left_color=grey&right_color=orange&left_text=Downloads/month
[download-url]: https://pepy.tech/project/schedex
[licence-badge]: https://img.shields.io/badge/license-Unlicense-blue.svg
[licence-url]: https://github.com/dapper91/schedex/blob/master/LICENSE
[python-version-badge]: https://img.shields.io/pypi/pyversions/schedex.svg
[python-version-url]: https://pypi.org/project/schedex
[build-badge]: https://github.com/dapper91/schedex/actions/workflows/test.yml/badge.svg?branch=master
[build-url]: https://github.com/dapper91/schedex/actions/workflows/test.yml


# schedex

Distributed persistent extendable job scheduling library

## Installation

```commandline
pip3 install schedex
```

**Optional dependencies**:

- **cron**: crontab based schedules
- **sqlalchemy**: [sqlalchemy](https://www.sqlalchemy.org/) backed scheduler
- **psycopg**: [psycopg](https://www.psycopg.org/psycopg3/docs/) backed scheduler and event broker (through LISTEN/NOTIFY mechanism)
- **asyncmy**: mysql([asyncmy](https://github.com/long2ice/asyncmy)) backed scheduler
- **pymongo**: mongodb([pymongo](https://pymongo.readthedocs.io/en/stable/index.html)) backed scheduler
- **opentelemetry**: telemetry compatible with [opentelemetry](https://opentelemetry.io/) standard
- **dishka**: dependency injection with [dishka](https://dishka.readthedocs.io/) library

## Overview

`Schedex` is a modern strictly-typed distributed persistent backend agnostic job scheduling library.

### Features

- **Database agnostic** nature allows to use it with any relation database (PostgreSQL, MySql, MsSql, MariaDB, Percona etc.)
or NoSql one (MongoDB, Redis, etc.) which let you pick up the best solution suitable for your requirements.

- The library is aimed to real-world projects so that it supports essential features required in production like
**transactional updates** - may be necessary if your application logic requires scheduling multiple jobs or 
cancel one job and schedule another or modify your domain data and schedule a job atomically.

- For data serialization `schedex` uses [msgspec](https://msgspec.dev/) library which make it **serialization format agnostic** (JSON, MsgPack etc.) 

- **Strongly typed** codebase helps typecheckers like mypy or pyright to validate your code for type correctness 
and fix more bugs on development stage.

- The library supports **locking mechanism customization** so that you can choose the one that is more suitable 
in your environment. It supports select-for-update locks (with relational databases), leasing locks; but you can
implement your own one if it is necessary.

### Why yet another scheduler?

There are some libraries that occupy the same niche. But they have some disadvantages 
which motivated to write `schedex` instead.

#### [procrastinate](https://procrastinate.readthedocs.io)
    
Mature and well tested library with great documentation and support. But it is tied to PostgreSQL which 
doesn't allow to use it with different databases. No transaction support make it useless when the business 
logic requires strong consistency.

#### [apscheduler](https://apscheduler.readthedocs.io)

Mature and well tested database agnostic library, but has some disadvantages: it is not strictly typed which
leads to many bugs in runtime; only one locking mechanism supported (leasing locks), transactional modifications
are not supported which makes it problematic to use the library in strong consistency required applications.

## Quickstart

```python
import datetime as dt
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
    db_url = "postgresql+psycopg://user:password@localhost:5432/myapp"
    db_engine = aiosa.create_async_engine(db_url)

    async with db_engine.begin() as conn:
        await conn.run_sync(sxsa.BaseModel.metadata.create_all)

    async with ppg_pool.AsyncConnectionPool(db_url) as pg_pool:
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
    db_url = "postgresql+psycopg://user:password@localhost:5432/myapp"
    db_engine = aiosa.create_async_engine(db_url)
    async with db_engine.begin() as conn:
        await conn.run_sync(sxsa.BaseModel.metadata.create_all)

    app_state: AppState = {}

    async with ppg_pool.AsyncConnectionPool(db_url) as pg_pool:
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

```
