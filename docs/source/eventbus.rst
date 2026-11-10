Eventbus
--------

Event Sender
____________

An event sender is a component responsible for publishing events that describe actions or changes occurring
within a scheduler instance or across the scheduler cluster. It provides an interface for emitting events
to the configured event transport, making execution-related information available to other components or
external consumers. Events may represent job creation, updates, cancellation, task execution, completion,
failure, and other significant lifecycle changes.

Event Receiver
______________

An event receiver is a component responsible for consuming and processing events published by event senders.
It provides an interface for subscribing to event streams and receiving notifications about actions and changes
occurring within a scheduler instance or across the cluster. Event receivers allow applications to react
to scheduler activity, implement custom monitoring and logging, trigger additional workflows,
or integrate the library with external systems.

.. important::
    Event bus **doesn't guarantee event delivery** so that some of them may be lost
    during network issues or node failures or restarts.

The following example demonstrates an event bus implementation via PostgreSQL LISTEN / NOTIFY mechanism.

.. literalinclude:: ../examples/sqlalchemy.py
