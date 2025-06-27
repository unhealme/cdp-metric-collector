from datetime import datetime
from typing import ClassVar, Self

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
    __json_dec: ClassVar[json.Decoder[Self]]

    @classmethod
    def decode_json(cls, data: bytes, /) -> Self:
        try:
            return cls.__json_dec.decode(data)
        except AttributeError:
            cls.__json_dec = json.Decoder(cls)
            return cls.decode_json(data)

    @classmethod
    def decode_yaml(cls, data: bytes, /) -> Self:
        return yaml.decode(data, type=cls)
