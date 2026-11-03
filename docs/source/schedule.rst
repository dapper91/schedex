Schedule
--------

A **schedule** defines the timetable according to which a task is executed. The library supports several types of
schedules to accommodate different execution patterns. **Periodic** schedules allow tasks to be executed
at regular intervals using a *constant* delay or an *exponentially* increasing delay.
They also support configuring the maximum *execution count* and applying *jitter* to introduce
randomness into execution intervals and help to prevent synchronized task execution.

For more flexible scheduling, the library supports **cron** schedules, provided by the optional **cron** extension,
which allow tasks to be executed according to standard cron expressions. Additionally, the library provides
**composite** schedules, which combine multiple schedules into a single schedule,
allowing a task to be triggered by several independent scheduling rules.

.. code-block:: python

    import datetime as dt
    import zoneinfo
    from typing import Any, MutableMapping, Union

    import sqlalchemy.ext.asyncio as aiosa

    import schedex as sx
    import schedex.contrib.sqlalchemy as sxsa
    from schedex import CombinedSchedule, PeriodicSchedule
    from schedex.schedule import cron

    AppState = MutableMapping[str, Any]
    TaskLock = sxsa.SqlAlchemySfuTaskLock
    Context = sx.Context[AppState, TaskLock]

    tasks = sx.TaskRegistry[AppState, TaskLock]()


    class CustomSchedule(CombinedSchedule):
        schedules: tuple[Union[PeriodicSchedule, cron.CronSchedule], ...]


    @tasks.task()
    async def print_message(context: Context, message: str, /) -> None:
        print(f"{dt.datetime.now()} [{context.task.sequence_number:02}] {message}")


    async def start_scheduler() -> None:
        db_engine = aiosa.create_async_engine("postgresql+psycopg://user:password@localhost:5432/myapp")

        async with db_engine.begin() as conn:
            await conn.run_sync(sxsa.BaseModel.metadata.create_all)

        scheduler = sxsa.SqlAlchemyTransactionalScheduler(
            aiosa.async_sessionmaker(db_engine),
            schedule_type=CustomSchedule,
        )

        await scheduler.add_job(
            sx.Job(
                CustomSchedule(
                    (
                        PeriodicSchedule.backoff(
                            start_at=dt.datetime.now(tz=dt.UTC),
                            delay=dt.timedelta(seconds=1),
                            multiplier=2.0,
                        ).with_max_count(6),
                        cron.CronSchedule(
                            "* * * * *",
                            timezone=zoneinfo.ZoneInfo("UTC"),
                            start_at=dt.datetime.now(tz=dt.UTC) + dt.timedelta(minutes=1),
                        ),
                    )
                ),
                print_message.with_params("ping"),
            )
        )
