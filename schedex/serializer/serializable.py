import abc
import inspect
from typing import Any, ClassVar, Protocol, Self

import msgspec


class EncoderProto(Protocol):
    """
    Encoder protocol.
    """

    def encode(self, obj: Any) -> bytes:
        """
        Encode the object into bytes.
        """


class DecoderProto(Protocol):
    """
    Decoder protocol.
    """

    def decode(self, data: bytes) -> Any:
        """
        Deserializes the object from bytes.
        """


class Serializable(abc.ABC):
    """
    Serializable type.
    """

    __encoder__: ClassVar[EncoderProto]
    __decoder__: ClassVar[DecoderProto]

    @classmethod
    @abc.abstractmethod
    def __build_encoder__(cls) -> EncoderProto:
        pass

    @classmethod
    @abc.abstractmethod
    def __build_decoder__(cls) -> DecoderProto:
        pass

    @abc.abstractmethod
    def serialize(self) -> bytes:
        """
        Serializes the object into bytes.
        """

    @classmethod
    @abc.abstractmethod
    def deserialize(cls, data: bytes) -> Self:
        """
        Deserializes the object from bytes.
        """


class SerializableMeta(msgspec.StructMeta, abc.ABCMeta):
    def __new__(
        mcls, name: str, bases: tuple[type, ...], attrs: dict[str, Any], /, **kwargs: Any
    ) -> type[Serializable]:
        cls = super().__new__(mcls, name, bases, attrs, **kwargs)
        assert issubclass(cls, Serializable), "SerializableMeta can be applied to Serializable class only"

        if not inspect.isabstract(cls):
            cls.__encoder__ = cls.__build_encoder__()
            cls.__decoder__ = cls.__build_decoder__()

        return cls


class MsgSpecSerializable(Serializable, msgspec.Struct, abc.ABC, metaclass=SerializableMeta):
    """
    MsgSpec serializable type.
    """

    def serialize(self) -> bytes:
        return self.__encoder__.encode(self)

    @classmethod
    def deserialize(cls, data: bytes) -> Self:
        return cls.__decoder__.decode(data)
