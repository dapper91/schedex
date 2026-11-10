Scheduler
---------

A scheduler is the client-facing interface for managing scheduled jobs. It provides methods for creating,
updating, retrieving, and cancelling jobs, allowing applications to control task execution.

.. literalinclude:: ../examples/scheduler.py


Transactions
____________

The library provies a transactional scheduler subtype (if data store supports atomic updates) that may be necessary
if your application logic requires atomic updates. There are two transaction types:

* **Owned transactions** - created by the scheduler itself, allows to modify multiple jobs atomically

.. literalinclude:: ../examples/scheduler_owned_transaction.py


* **Outer transactions** - created outside and passed to the scheduler, used to modify jobs and your domain data atomically

.. literalinclude:: ../examples/scheduler_outer_transaction.py
