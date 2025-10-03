from msgspec import Struct

from . import RangerResultPage


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


class RangerPolicy(Struct):
    isEnabled: bool
    resources: dict[str, RangerPolicyResource]
    policyItems: list[RangerPolicyItem]
    denyPolicyItems: list[RangerPolicyItem]
    allowExceptions: list[RangerPolicyItem]
    denyExceptions: list[RangerPolicyItem]


class RangerPolicyList(RangerResultPage):
    policies: list[RangerPolicy]
