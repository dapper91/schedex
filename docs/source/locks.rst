Locks
-----

Locks are used to ensure that a task is not executed concurrently by multiple executor instances.
``schedex`` provides two built-in locking strategies: SELECT FOR UPDATE (SFU) locks and leasing-based locks.

SFU Lock
________

This mechanism relies on the database's row-level locking capabilities using the ``SELECT ... FOR UPDATE`` statement.
When an executor acquires a lock, the corresponding database row is locked within a transaction,
preventing other executors from acquiring the same lock until the transaction is completed.
This mechanism is available for SQLAlchemy backends using databases that support ``SELECT ... FOR UPDATE`` statement.
It is not available for MongoDB.

.. literalinclude:: ../examples/sfu_lock.py


Leasing Lock
____________

Instead of relying on database-level row locking, this mechanism uses a time-limited lease associated with a task.
An executor acquires a lease for a specified period, during which other executors cannot acquire the same lock.
Once the lease expires, another executor may acquire it. Leasing locks are supported by both MongoDB and SQLAlchemy
backends and can be used by any database backend.

Leasing locks support **autoextend** mechanism that automatically renews an acquired lease before it expires.
The executor periodically extends the lease after a configured interval, allowing long-running tasks to retain
their locks without manual extension or requiring an excessively long initial lease duration.
This is particularly useful for tasks whose execution time is difficult to predict.

.. literalinclude:: ../examples/leasing_lock.py


If automatic extension is not acceptable locks provide an interface to extend it manually at an appropriate task
execution point. Besides autoextend feature the executor allows a running task to be automatically cancelled
when its lock expires. This helps prevent a task from continuing execution after losing ownership of its lock,
reducing the risk of concurrent execution by multiple executor instances.

.. literalinclude:: ../examples/leasing_lock_manual_extend.py
