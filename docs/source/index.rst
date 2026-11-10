schedex
=======

Overview
--------

``schedex`` is a modern strictly-typed distributed persistent backend agnostic job scheduling library.


Features
~~~~~~~~

- **Database agnostic** nature allows to use it with any relation database (PostgreSQL, MySql, MsSql, MariaDB, Percona etc.)
  or NoSql one (MongoDB, Redis, etc.) which let you pick up the best solution suitable for your requirements.

- The library is aimed to real-world projects so that it supports essential features required in production like
  **transactional updates** - may be necessary if your application logic requires scheduling multiple jobs or
  modify your domain data and schedule a job atomically.

- For data serialization ``schedex`` uses msgspec_ library which make it **serialization format agnostic** (JSON, MsgPack etc.)

- **Strongly typed** codebase helps typecheckers like mypy or pyright to validate your code for type correctness
  and fix more bugs on development stage.

- The library supports **locking mechanism customization** so that you can choose the one that is more suitable
  in your environment. It supports select-for-update locks (with relational databases), leasing locks; but you can
  implement your own one if it is necessary.


Quickstart
----------

.. literalinclude:: ../examples/quickstart_scheduler.py
   :caption: sqlalchemy scheduler

.. literalinclude:: ../examples/quickstart_executor.py
   :caption: sqlalchemy executor


.. toctree::
    :hidden:
    :maxdepth: 2
    :caption: Overview

    motivation.rst
    install.rst
    intro.rst

.. toctree::
    :hidden:
    :maxdepth: 2
    :caption: Usage

    schedule.rst
    scheduler.rst
    context.rst
    executor.rst
    locks.rst
    eventbus.rst
    retries.rst
    serialization.rst


.. toctree::
    :hidden:
    :maxdepth: 2
    :caption: Extensions

    sqlalchemy.rst
    mongodb.rst
    di.rst
    telemetry.rst


.. toctree::
    :hidden:
    :maxdepth: 2
    :caption: Reference

    api.rst
    development.rst


.. _msgspec: https://msgspec.dev/
