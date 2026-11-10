Serialization
-------------

The library uses msgspec json serialization protocol for task parameters serialization by default, but
you can implement your own serializer/deserializer if the default one doesn't fix you.


MsgPack serializer
__________________

.. literalinclude:: ../examples/msgpack_serializer.py
