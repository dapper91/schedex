Executor
--------

An executor is the runtime component responsible for executing scheduled tasks. It continuously monitors available
jobs and their schedules, determines when a task is due for execution, and dispatches it accordingly.
The executor resolves tasks using the task registry, prepares the execution context, acquires the appropriate lock
to prevent concurrent execution, and invokes the associated method with the configured arguments.
It also manages the task execution lifecycle, including handling successful completion and failures, releasing locks,
and emitting events to notify other components about execution-related activities.
