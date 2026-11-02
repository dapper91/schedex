Context
-------

A context is an object that provides access to information and resources associated with the current task execution.
It contains execution metadata, such as the task ID, task name, execution attempt counter, and task-specific metadata.
It also provides access to the task's lock and the shared application state, allowing tasks to interact with
application-level resources. The context is created for each task execution and is passed to the executing method,
providing a unified interface for accessing execution-related information, managing task concurrency,
and interacting with the application environment.
