Telemetry
---------

The library provides built-in opentelemetry_ integration for collecting metrics and distributed traces related to
task scheduling and execution. It allows developers to monitor scheduler and executor activity,
track task execution duration, identify failures, and gain visibility into the overall system behavior.

The integration can be used with OpenTelemetry-compatible observability platforms and backends,
enabling seamless incorporation of the library into existing monitoring and tracing infrastructure.

.. _opentelemetry: https://opentelemetry.io/


.. literalinclude:: ../examples/telemetry.py
