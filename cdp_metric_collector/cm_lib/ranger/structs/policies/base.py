from typing import ClassVar, Iterable

from msgspec import Struct


class RangerPolicyItemAccess(Struct):
    type: str
    isAllowed: bool


class RangerPolicyItem(Struct):
    accesses: list[RangerPolicyItemAccess]
    users: list[str]
    groups: list[str]
    roles: list[str]


class RangerPolicyResource(Struct):
    values: list[str]
    isExcludes: bool
    isRecursive: bool


class RangerResourceBase(Struct, forbid_unknown_fields=True):
    type: ClassVar[str]

    def format_values(self) -> Iterable[str]:
        raise NotImplementedError
