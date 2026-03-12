__all__ = (
    "RangerPolicy",
    "RangerPolicyItem",
    "RangerPolicyList",
    "RangerPolicyResource",
    "RangerResourceBase",
    "RangerResourceHetuCatalog",
    "RangerResourceHetuColumn",
    "RangerResourceHetuFunction",
    "RangerResourceHetuProcedure",
    "RangerResourceHetuSchema",
    "RangerResourceHetuSessionProperty",
    "RangerResourceHetuSystemProperty",
    "RangerResourceHetuTable",
    "RangerResourceHetuTrinoUser",
    "RangerResourceHiveColumn",
    "RangerResourceHiveDatabase",
    "RangerResourceHiveGlobal",
    "RangerResourceHiveService",
    "RangerResourceHiveStorage",
    "RangerResourceHiveTable",
    "RangerResourceHiveUDF",
    "RangerResourceHiveURL",
)

from msgspec import Raw as _Raw
from msgspec import Struct as _Struct
from msgspec import ValidationError as _ValidationError
from msgspec import json as _json

from cdp_metric_collector.cm_lib.ranger.structs import (
    RangerResultPage as _RangerResultPage,
)

from .base import RangerPolicyItem, RangerPolicyResource, RangerResourceBase
from .hetu import (
    RangerResourceHetuCatalog,
    RangerResourceHetuColumn,
    RangerResourceHetuFunction,
    RangerResourceHetuProcedure,
    RangerResourceHetuSchema,
    RangerResourceHetuSessionProperty,
    RangerResourceHetuSystemProperty,
    RangerResourceHetuTable,
    RangerResourceHetuTrinoUser,
)
from .hive import (
    RangerResourceHiveColumn,
    RangerResourceHiveDatabase,
    RangerResourceHiveGlobal,
    RangerResourceHiveService,
    RangerResourceHiveStorage,
    RangerResourceHiveTable,
    RangerResourceHiveUDF,
    RangerResourceHiveURL,
)


class RangerPolicy(_Struct):
    service: str
    serviceType: str
    isEnabled: bool
    resources: _Raw
    policyItems: list[RangerPolicyItem]
    denyPolicyItems: list[RangerPolicyItem]
    allowExceptions: list[RangerPolicyItem]
    denyExceptions: list[RangerPolicyItem]

    def decode_resource(self):
        last_error = _ValidationError()
        for t in (
            RangerResourceHetuCatalog,
            RangerResourceHetuColumn,
            RangerResourceHetuFunction,
            RangerResourceHetuProcedure,
            RangerResourceHetuSchema,
            RangerResourceHetuSessionProperty,
            RangerResourceHetuSystemProperty,
            RangerResourceHetuTable,
            RangerResourceHetuTrinoUser,
            RangerResourceHiveColumn,
            RangerResourceHiveDatabase,
            RangerResourceHiveGlobal,
            RangerResourceHiveService,
            RangerResourceHiveStorage,
            RangerResourceHiveTable,
            RangerResourceHiveUDF,
            RangerResourceHiveURL,
        ):
            try:
                return _json.decode(self.resources, type=t)
            except _ValidationError as e:
                last_error = e
        err = f"Unable to decode {_json.decode(self.resources)}"
        raise ValueError(err) from last_error


class RangerPolicyList(_RangerResultPage):
    policies: list[RangerPolicy]
