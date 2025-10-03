from msgspec import Struct

TYPE_CHECKING = False
if TYPE_CHECKING:
    from typing import Any, Self


class HiveDatabase(Struct, array_like=True):
    name: str
    comment: str
    location: str
    location_managed: str
    owner: str
    owner_type: str
    param: str

    def __str__(self) -> str:
        return self.name

    def __eq__(self, other: "Any") -> bool:
        if isinstance(other, self.__class__):
            return self.name.__eq__(other.name)
        return super().__eq__(other)

    def __lt__(self, other: "Self"):
        return self.name.__lt__(other.name)
