API
---


Scheduler
~~~~~~~~~

.. currentmodule:: schedex

.. autoclass:: Job
.. autoclass:: JobData
.. autoclass:: Context
.. autoclass:: TaskInfo

.. autoclass:: Scheduler
.. autoclass:: SchedulerMiddleware
.. autoclass:: SchedulerMiddlewareWrappedFunc

.. autoclass:: TransactionalScheduler
.. autoclass:: SchedulerTransaction
.. autoclass:: OwnedSchedulerTransaction

.. autoclass:: TaskRegistry
.. autoclass:: Task
.. autoclass:: BoundTask

.. autoclass:: Serializable
.. autoclass:: SerializerBuilder
.. autoclass:: MsgSpecSerializable


.. currentmodule:: schedex.serializer.json

.. autoclass:: JsonSerializable


Schedule
~~~~~~~~

.. currentmodule:: schedex.schedule

.. autoclass:: Schedule
.. autoclass:: PeriodicSchedule


.. currentmodule:: schedex.schedule.cron

.. autoclass:: CronSchedule


Eventbus
~~~~~~~~

.. currentmodule:: schedex.eventbus

.. autoclass:: Event
.. autoclass:: EventKind
.. autoclass:: EventReceiver
.. autoclass:: EventSender


Executor
~~~~~~~~

.. currentmodule:: schedex.executor

.. autoclass:: WorkerPoolExecutor
.. autoclass:: WorkerPool
.. autoclass:: Worker
.. autoclass:: ExecutorMiddleware
.. autoclass:: ExecutorMiddlewareWrappedFunc


Locks
~~~~~

.. currentmodule:: schedex.lock

.. autoclass:: LockManager
.. autoclass:: Lock
.. autoclass:: JobLock
.. autoclass:: TaskLock
.. autoclass:: BreakTask


Storage
~~~~~~~

.. currentmodule:: schedex.storage

.. autoclass:: JobManager
.. autoclass:: JobStatus
.. autoclass:: StoredJob
.. autoclass:: TaskManager
.. autoclass:: TaskStatus
.. autoclass:: StoredTask
.. autoclass:: TransactionManager
.. autoclass:: OwnedTransaction
.. autoclass:: Transaction
.. autoclass:: JobAlreadyExists



In-Memory
~~~~~~~~~

.. currentmodule:: schedex.mem.eventbus

.. autoclass:: InMemoryEventReceiver
.. autoclass:: InMemoryEventSender
.. autofunction:: build


SqlAlchemy
~~~~~~~~~~

.. currentmodule:: schedex.contrib.sqlalchemy

.. autoclass:: BaseModel
.. autoclass:: SqlAlchemyScheduler
.. autoclass:: SqlAlchemyTransactionalScheduler
.. autoclass:: SqlAlchemyTransactionManager
.. autoclass:: SqlAlchemyOwnedTransaction
.. autoclass:: SqlAlchemyTransaction

.. autoclass:: SqlAlchemyJobManager

.. autoclass:: SqlAlchemySfuLockManager
.. autoclass:: SqlAlchemySfuJobLock
.. autoclass:: SqlAlchemySfuTaskLock
.. autoclass:: SqlAlchemyLeasingLockManager
.. autoclass:: SqlAlchemyLeasingJobLock
.. autoclass:: SqlAlchemyLeasingTaskLock


PyMongo
~~~~~~~

.. currentmodule:: schedex.contrib.pymongo

.. autoclass:: PyMongoScheduler
.. autoclass:: PyMongoTransactionalScheduler

.. autoclass:: PyMongoTransactionManager
.. autoclass:: PyMongoTransaction
.. autoclass:: PyMongoOwnedTransaction

.. autoclass:: PyMongoLeasingLockManager
.. autoclass:: PyMongoLeasingJobLock
.. autoclass:: PyMongoLeasingTaskLock

.. autoclass:: PyMongoEventReceiver
.. autoclass:: PyMongoJobManager
.. autoclass:: LockFailedError
.. autoclass:: Job
.. autoclass:: Task
.. autofunction:: create_schema


Opentelemetry
~~~~~~~~~~~~~

.. currentmodule:: schedex.contrib.opentelemetry

.. autofunction:: build_job_schedule_tracing_middleware
.. autofunction:: build_job_schedule_metrics_collecting_middleware
.. autofunction:: build_task_execution_tracing_middleware
.. autofunction:: build_task_execution_metrics_collecting_middleware


Dishka
~~~~~~

.. currentmodule:: schedex.contrib.dishka

.. autofunction:: inject
.. autofunction:: request_container_middleware


Error guard
~~~~~~~~~~~

.. currentmodule:: schedex.guard

.. autofunction:: constant_delay
.. autofunction:: exponential_delay
.. autoclass:: GuardExhausted
.. autoclass:: ErrorGuard


Runtime
~~~~~~~

.. currentmodule:: schedex.runtime

.. autofunction:: select
.. autoclass:: first
.. autoclass:: second
.. autoclass:: third
.. autoclass:: fourth
.. autoclass:: fifth


.. currentmodule:: schedex.runtime.iterator

.. autofunction:: merge_iterators
