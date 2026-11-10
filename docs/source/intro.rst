Terminology
------------

This section describes the core concepts and terminology used throughout the library.

**Method**

A method is a Python function that is executed by the library according to a defined schedule.

**Task**

A task is an object that represents a method along with additional metadata required for its execution,
such as a task name and argument serializer/deserializer.

**Task Registry**

A task registry is a collection of registered tasks available to the executor. It provides a way to discover and
retrieve tasks by their identifiers.

**Schedule**

A schedule defines the execution timetable for a task. It specifies when and how frequently the task should be executed.

**Job**

A job is an object that combines a task, its execution arguments, and its schedule. It represents a complete unit of scheduled work.

**Context**

A context is an object that provides information and resources associated with a task execution. It may contain:

- Task identification and metadata, such as task ID and task name.
- Execution information, such as the current execution attempt.
- The task lock.
- The application state.
- The task metadata (trace id, trance baggage and so on).

The context is made available during task execution and provides access to the information and resources required by the task.

**State**

A state is an object that holds application-specific data shared across task executions. It may contain resources and
configurations such as database connections, application settings, and other application-level dependencies.

**Scheduler**

A scheduler is a client-facing interface for managing jobs. It provides methods for adding, updating, and cancelling scheduled jobs.

**Executor**

An executor is the runtime component responsible for monitoring scheduled jobs and executing their associated tasks according to their schedules.

**Lock**

A lock is an object that prevents concurrent execution of the same task, ensuring that only one execution can hold the lock at a time.

**Event**

An event is an object that represents an action or occurrence within a scheduler instance or across the scheduler cluster.
Events can be used to communicate changes, execution activity, and other significant occurrences.

**Eventbus**

An eventbus is an event transmission channel of many-to-many type that consists of multiple senders and multiple receivers.
