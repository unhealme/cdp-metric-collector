from msgspec import Struct

from . import RangerResultPage


class RangerService(Struct):
    name: str
    type: str
    displayName: str


class RangerServiceList(RangerResultPage):
    services: list[RangerService]
