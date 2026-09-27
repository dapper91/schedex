import asyncio as aio
import datetime as dt
import logging
import zoneinfo
from typing import Any, MutableMapping

import sqlalchemy as sa
import sqlalchemy.ext.asyncio as aiosa
from sqlalchemy import orm

import schedex as sx
import schedex.contrib.sqlalchemy as sxsa
from schedex.schedule import cron

AppState = MutableMapping[str, Any]
TaskLock = sxsa.SqlAlchemySfuTaskLock
Context = sx.Context[AppState, TaskLock]

tasks = sx.TaskRegistry[AppState, TaskLock]()


class BaseModel(orm.DeclarativeBase):
    pass


class User(orm.MappedAsDataclass, BaseModel, kw_only=True):
    __tablename__ = "users"

    id: orm.Mapped[int] = orm.mapped_column(sa.Integer, primary_key=True, init=False)
    name: orm.Mapped[str]
    birthday: orm.Mapped[dt.date]
    email: orm.Mapped[str]


@tasks.task()
async def send_email(context: Context, *, name: str, email: str) -> None:
    print(f"sending email to {email} with message: Happy birthday dear {name}!!!")


async def main() -> None:
    db_engine = aiosa.create_async_engine("postgresql+psycopg://user:password@localhost:5432/myapp")
    session_maker = aiosa.async_sessionmaker(db_engine)

    async with db_engine.begin() as conn:
        await conn.run_sync(sxsa.BaseModel.metadata.create_all)
        await conn.run_sync(BaseModel.metadata.create_all)

    scheduler = sxsa.SqlAlchemyTransactionalScheduler(session_maker, schedule_type=cron.CronSchedule)

    user = User(name="John Doe", birthday=dt.date(2000, 6, 1), email="johndoe@mail.com")

    async with session_maker() as db_session:
        async with db_session.begin():
            scheduler_tx = scheduler.within_transaction(db_session)
            await scheduler_tx.add_job(
                sx.Job(
                    cron.CronSchedule(
                        "{minute} {hour} {day} {month} *".format(
                            day=user.birthday.day,
                            month=user.birthday.month,
                            hour=9,
                            minute=0,
                        ),
                        zoneinfo.ZoneInfo("UTC"),
                    ),
                    send_email.with_params(name=user.name, email=user.email),
                )
            )
            db_session.add(user)


logging.basicConfig(level=logging.INFO)

aio.run(main())
