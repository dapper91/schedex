Installation
------------

.. code-block:: sh

    pip3 install schedex

Optional dependencies:

* **cron**: crontab based schedules
* **sqlalchemy**: sqlalchemy_ backed scheduler
* **psycopg**: psycopg_ backed scheduler and event broker (through LISTEN/NOTIFY mechanism)
* **asyncmy**: mysql(asyncmy_) backed scheduler
* **pymongo**: mongodb(pymongo_) backed scheduler
* **opentelemetry**: telemetry compatible with opentelemetry_ standard
* **dishka**: dependency injection with dishka_ library


.. _sqlalchemy: https://www.sqlalchemy.org/
.. _psycopg: https://www.psycopg.org/psycopg3/docs/
.. _asyncmy: https://github.com/long2ice/asyncmy
.. _pymongo: https://pymongo.readthedocs.io/en/stable/index.html
.. _opentelemetry: https://opentelemetry.io/
.. _dishka: https://dishka.readthedocs.io/
