Motivation
----------

Why yet another scheduler?

There are some libraries that occupy the same niche. But they have some disadvantages
which motivated to write ``schedex`` instead.

* procrastinate_

Mature and well tested library with great documentation and support. But it is tied to PostgreSQL which
doesn't allow to use it with different databases. No transaction support make it useless when the business
logic requires strong consistency.

* apscheduler_

Mature and well tested database agnostic library, but has some disadvantages: it is not strictly typed which
leads to many bugs in runtime; only one locking mechanism supported (leasing locks), transactional modifications
are not supported which makes it problematic to use the library in strong consistency required applications.


.. _procrastinate: https://procrastinate.readthedocs.io
.. _apscheduler: https://apscheduler.readthedocs.io
