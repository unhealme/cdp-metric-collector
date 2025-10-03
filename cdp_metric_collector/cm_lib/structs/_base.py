from collections.abc import Callable
from datetime import datetime
from typing import Any, ClassVar, Self

from msgspec import Struct, json, structs, yaml


class Progressive(Struct):
    startTime: int
    endTime: int | None

    def duration(self):
        start = datetime.fromtimestamp(self.startTime / 1000)
        if self.endTime:
            end = datetime.fromtimestamp(self.endTime / 1000)
            x, y = sorted((end, start), reverse=True)
            elapsed = x - y
        else:
            end = None
            elapsed = datetime.now() - start
        return start, end, elapsed


class DTNoTZ(Struct):
    def __post_init__(self):
        for field in structs.fields(self):
            if field.type is datetime:
                setattr(
                    self,
                    field.name,
                    datetime.replace(getattr(self, field.name), tzinfo=None),
                )


class Decodable(Struct):
    __jdec__: ClassVar[json.Decoder[Self]]
    __dec_hook__: ClassVar[Callable[[type, Any], Any] | None] = None

    @classmethod
    def decode_json(cls, data: bytes, /):
        try:
            return cls.__jdec__.decode(data)
        except AttributeError:
            cls.__jdec__ = json.Decoder(cls, dec_hook=cls.__dec_hook__)
            return cls.__jdec__.decode(data)

    @classmethod
    def decode_yaml(cls, data: bytes, /):
        return yaml.decode(data, type=cls, dec_hook=cls.__dec_hook__)
