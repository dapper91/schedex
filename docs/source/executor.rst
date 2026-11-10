Executor
--------

An executor is the runtime component responsible for executing scheduled tasks. It continuously monitors available
jobs and their schedules, determines when a task is due for execution, and dispatches it accordingly.

The executor resolves tasks using the task registry, prepares the execution context, acquires the appropriate lock
to prevent concurrent execution, and invokes the associated method with the configured arguments.

It also manages the task execution lifecycle, including handling successful completion and failures, releasing locks,
and emitting events to notify other components about execution-related activities.

The executor accepts the following configuration parameters:

- **state** - an application-specific state object that is made available to every task execution through its context.
  It can be used to provide shared resources and dependencies, such as database connections, application settings,
  API clients, or service instances.

- **polling_interval** — defines how frequently the executor checks the storage backend for jobs that are ready for execution.
  A shorter interval reduces scheduling latency but increases the number of database queries,
  while a longer interval reduces database load at the cost of potentially delaying job execution.

- **max_workers** — specifies the maximum number of tasks that can be executed concurrently by a single executor instance.
  This parameter helps control resource consumption and prevent excessive parallelism.

- **task_registry** — a registry containing all tasks available for execution. When a job becomes ready for execution,
  the executor uses the registry to resolve its task identifier into the corresponding task implementation.

- **cancel_on_lock_expired** — determines whether a running task should be cancelled during its execution if the lock expires.
  This can help prevent tasks from continuing to execute after losing ownership of their locks, reducing the risk of concurrent execution.

- **lock_manager** — defines the locking mechanism used by the executor to acquire task locks.
  The library provides built-in implementations, such as select-for-update and leasing-based locks.

- **shutdown_timeout** — specifies the maximum amount of time the executor waits for running tasks to finish during shutdown.
  This allows the application to perform a graceful shutdown without waiting indefinitely for long-running tasks.

- **event_sender** — a sending endpoint of an event bus responsible for publishing executor events, such as task execution
  started, completed, failed, or other significant lifecycle events. It can be used to propagate execution information
  to other components or scheduler instances.

- **event_receiver** — a receiving endpoint of an event bus responsible for receiving events from other scheduler or
  executor instances. It allows the executor to react to external changes and notifications without relying
  exclusively on periodic polling.


.. literalinclude:: ../examples/executor.py
