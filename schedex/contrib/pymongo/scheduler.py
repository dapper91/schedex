from typing import Iterable, Optional

import pymongo.asynchronous.client_session as pmses
import pymongo.asynchronous.mongo_client as pmcli

import schedex as sx

from .schema import DocumentType
from .storage import PyMongoJobManager, PyMongoTransactionManager


class PyMongoScheduler[SchT: sx.Schedule](sx.Scheduler[SchT]):
    """
    PyMongo job scheduler.

    :param client: mongodb client
    :param schedule_type: schedule type
    :param event_sender: event sender
    :param middlewares: scheduler middlewares
    :param dbname: database name
    """

    def __init__(
        self,
        client: pmcli.AsyncMongoClient[DocumentType],
        schedule_type: type[SchT],
        event_sender: Optional[sx.EventSender] = None,
        middlewares: Iterable[sx.SchedulerMiddleware[SchT]] = (),
        dbname: Optional[str] = None,
    ):
        super().__init__(PyMongoJobManager(client, dbname), schedule_type, event_sender, middlewares)
        self._schedule_type = schedule_type
        self._event_sender = event_sender


class PyMongoTransactionalScheduler[SchT: sx.Schedule](sx.TransactionalScheduler[pmses.AsyncClientSession, SchT]):
    """
    PyMongo transactional job scheduler.

    :param client: mongodb client
    :param schedule_type: schedule type
    :param event_sender: event sender
    :param middlewares: scheduler middlewares
    :param dbname: database name
    """

    def __init__(
        self,
        client: pmcli.AsyncMongoClient[DocumentType],
        schedule_type: type[SchT],
        event_sender: Optional[sx.EventSender] = None,
        middlewares: Iterable[sx.SchedulerMiddleware[SchT]] = (),
        dbname: Optional[str] = None,
    ):
        super().__init__(
            PyMongoTransactionManager(client, dbname),
            PyMongoJobManager(client, dbname),
            schedule_type,
            event_sender,
            middlewares,
        )
