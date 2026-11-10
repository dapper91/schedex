Schedule
--------

A **schedule** defines the timetable according to which a task is executed. The library supports several types of
schedules to accommodate different execution patterns. **Periodic** schedules allow tasks to be executed
at regular intervals using a *constant* delay or an *exponentially* increasing one.
They also support configuring the maximum *execution count* to limit the number of task launches.

The following example demonstrate the creation of a periodic schedule. The execution starts in 1 minute and will
be repeated 6 times with an interval of 30 seconds after the first execution and increased by 2 after each subsequent one.

.. literalinclude:: ../examples/periodic_schedule.py


A *jitter* lets to apply some randomness into execution intervals and help to prevent synchronized task execution

.. literalinclude:: ../examples/schedule_jitter.py


For more flexible scheduling the library supports **cron** schedules, provided by the optional **cron** extension,
which allow tasks to be executed according to standard cron expressions.

.. literalinclude:: ../examples/cron_schedule.py


.. important::
    Only one schedule type can be used within schedulers/executors setup which means you can't use
    periodic and cron schedules together.

If you desire to use periodic and cron schedules together you may declare your own schedule type.

.. literalinclude:: ../examples/custom_schedule.py
