import abc
import zoneinfo
from typing import Any, ClassVar, Union

import msgspec

from .serializable import DecoderProto, EncoderProto, MsgSpecSerializable

NativeType = Union[int, float, bool, str]
NativeCollection = Union[tuple[NativeType], list[NativeType], dict[NativeType, NativeType]]
PrimitiveType = Union[NativeType | NativeCollection]


class TypeEncoder[T](abc.ABC):
    """
    Base type encoder.
    """

    type: ClassVar[type[T]]

    @classmethod
    @abc.abstractmethod
    def encode(cls, value: T) -> PrimitiveType:
        """
        Encodes the value into a primitive type.
        """

    @classmethod
    @abc.abstractmethod
    def decode(cls, prim: PrimitiveType) -> T:
        """
        Decodes the value from the primitive type.
        """


class ZoneInfoEncoder(TypeEncoder[zoneinfo.ZoneInfo]):
    """
    Time zone info encoder.
    """

    type = zoneinfo.ZoneInfo

    @classmethod
    def encode(cls, value: zoneinfo.ZoneInfo) -> PrimitiveType:
        return value.key

    @classmethod
    def decode(cls, prim: PrimitiveType) -> zoneinfo.ZoneInfo:
        if isinstance(prim, str):
            return zoneinfo.ZoneInfo(prim)
        else:
            raise TypeError(f"Objects of type {type(prim)} are not supported")


ENCODERS: dict[type[Any], TypeEncoder[Any]] = {
    ZoneInfoEncoder.type: ZoneInfoEncoder(),
}


def encode_extra(obj: Any) -> Any:
    if encoder := ENCODERS.get(type(obj)):
        return encoder.encode(obj)
    else:
        raise NotImplementedError(f"Objects of type {type(obj)} are not supported")


def decode_extra(tp: type, obj: Any) -> Any:
    if decoder := ENCODERS.get(tp):
        return decoder.decode(obj)
    else:
        raise TypeError(f"Objects of type {tp} are not supported")


class JsonSerializable(MsgSpecSerializable):
    """
    JSON serializable minxin.
    """

    @classmethod
    def __build_encoder__(cls) -> EncoderProto:
        return msgspec.json.Encoder(enc_hook=encode_extra)

    @classmethod
    def __build_decoder__(cls) -> DecoderProto:
        return msgspec.json.Decoder(type=cls, dec_hook=decode_extra)
