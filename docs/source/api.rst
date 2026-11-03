API
---


Scheduler
~~~~~~~~~

.. currentmodule:: schedex

.. autoclass:: Job
.. autoclass:: Context
.. autoclass:: TaskInfo

.. autoclass:: Scheduler
.. autoclass:: SchedulerMiddleware
.. autoclass:: SchedulerMiddlewareWrappedFunc

.. autoclass:: TransactionalScheduler
.. autoclass:: TransactionManager
.. autoclass:: OwnedTransaction
.. autoclass:: Transaction

.. autoclass:: TaskRegistry
.. autoclass:: Task
.. autoclass:: BoundTask

.. autoclass:: Serializable


Schedule
~~~~~~~~

.. currentmodule:: schedex.schedule

.. autoclass:: Schedule
.. autoclass:: CombinedSchedule
.. autoclass:: PeriodicSchedule


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


Storage
~~~~~~~

.. currentmodule:: schedex.storage

.. autoclass:: JobManager
.. autoclass:: JobStatus
.. autoclass:: StoredJob
.. autoclass:: TaskManager
.. autoclass:: TaskStatus
.. autoclass:: StoredTask



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
