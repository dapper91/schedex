SqlAlchemy
----------

``schedex`` provides built-in SQLAlchemy support, allowing you to use any relational database
supported by SQLAlchemy as the storage backend. This includes databases such as
PostgreSQL, MySQL, MariaDB, Microsoft SQL Server, and others.

The backend supports two types of locking mechanisms. Databases that support the ``SELECT ... FOR UPDATE`` statement
can use row-level locks to ensure safe concurrent task execution. For databases that do not support this statement,
leasing-based locks can be used instead.

Transactional updates are fully supported, allowing you to perform multiple scheduling operations atomically
or combine job scheduling and updates to your application data within a single database transaction.

The SQLAlchemy backend does not provide a built-in event bus.
However, you can implement event notifications using database-specific mechanisms.
For example, PostgreSQL users can leverage LISTEN / NOTIFY through the psycopg extension to propagate events across scheduler instances.

.. literalinclude:: ../examples/sqlalchemy.py
