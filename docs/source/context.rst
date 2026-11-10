Context
-------

A context is an object that provides access to information and resources associated with the current task execution.
It contains execution metadata, such as the task ID, task name, execution attempt counter

.. literalinclude:: ../examples/task_context_data.py


and task-specific metadata.

.. literalinclude:: ../examples/task_context_meta.py


It also provides access to the task's lock and the shared application state, allowing tasks to interact with
application-level resources.

.. literalinclude:: ../examples/task_context_state.py


To prevent concurrent task execution the executor automatically acquires a lock and releases it after the task finished.
The lock is created for each task and passed to the executing method, providing an interface for obtaining
information about the lock.

.. literalinclude:: ../examples/task_context_lock.py
