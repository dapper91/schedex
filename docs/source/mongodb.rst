MongoDB
-------

``schedex`` provides built-in MongoDB support through the pymongo library. The MongoDB extension implements
the scheduler, executor, and event bus components using MongoDB Change Streams (watch mechanism),
allowing instances to react to database changes and communicate across the scheduler cluster.
Since MongoDB does not support locking queries, the backend supports only leasing-based locks to
prevent concurrent processing of the same task.

The backend also supports MongoDB transactions, allowing multiple scheduling operations to be performed atomically.
This is particularly useful when application logic requires modifying domain data and scheduling or updating jobs
as part of a single transaction. Note that MongoDB transactions require a replica set or a sharded cluster.

.. literalinclude:: ../examples/mongodb.py
