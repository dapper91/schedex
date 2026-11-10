Retries
-------

Short-term retries
__________________

For short-term retries (when it is tolerable to keep the retry state in memory) you may only use 3rd party
retry libraries like tenacity_ to configure repeatable task execution on temporary errors.

.. literalinclude:: ../examples/short_term_retry.py


Long-term retries
_________________

Long-term retries may be implemented with the appropriate middleware postponing the task execution.

.. literalinclude:: ../examples/long_term_retry.py


.. _tenacity: https://tenacity.readthedocs.io
