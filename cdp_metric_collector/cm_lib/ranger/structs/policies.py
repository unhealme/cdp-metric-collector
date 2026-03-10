from typing import ClassVar, Iterable

from msgspec import Raw, Struct, ValidationError, field, json

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


class RangerResourceHiveBase(Struct, forbid_unknown_fields=True):
    type: ClassVar[str]

    def format_values(self) -> Iterable[str]:
        raise NotImplementedError


class RangerResourceHiveColumn(RangerResourceHiveBase):
    type: ClassVar[str] = "Column"
    column: RangerPolicyResource
    database: RangerPolicyResource
    table: RangerPolicyResource

    def format_values(self):
        for d in self.database.values:
            for t in self.table.values:
                for c in self.column.values:
                    yield f"{d}.{t}.{c}"


class RangerResourceHiveDatabase(RangerResourceHiveBase):
    type: ClassVar[str] = "Database"
    database: RangerPolicyResource

    def format_values(self):
        yield from self.database.values


class RangerResourceHiveGlobal(RangerResourceHiveBase):
    type: ClassVar[str] = "Global"
    global_resource: RangerPolicyResource = field(name="global")

    def format_values(self):
        yield from self.global_resource.values


class RangerResourceHiveService(RangerResourceHiveBase):
    type: ClassVar[str] = "Hive Service"
    hiveservice: RangerPolicyResource

    def format_values(self):
        yield from self.hiveservice.values


class RangerResourceHiveStorage(RangerResourceHiveBase):
    type: ClassVar[str] = "Storage"
    storage_type: RangerPolicyResource = field(name="storage-type")
    storage_url: RangerPolicyResource = field(name="storage-url")

    def format_values(self):
        for st in self.storage_type.values:
            for su in self.storage_url.values:
                yield f"{st}: {su}"


class RangerResourceHiveTable(RangerResourceHiveBase):
    type: ClassVar[str] = "Table"
    database: RangerPolicyResource
    table: RangerPolicyResource

    def format_values(self):
        for d in self.database.values:
            for t in self.table.values:
                yield f"{d}.{t}"


class RangerResourceHiveUDF(RangerResourceHiveBase):
    type: ClassVar[str] = "UDF"
    database: RangerPolicyResource
    udf: RangerPolicyResource

    def format_values(self):
        for d in self.database.values:
            for u in self.udf.values:
                yield f"{d}.{u}"


class RangerResourceHiveURL(RangerResourceHiveBase):
    type: ClassVar[str] = "URL"
    url: RangerPolicyResource

    def format_values(self):
        yield from self.url.values


class RangerPolicy(Struct):
    service: str
    serviceType: str
    isEnabled: bool
    resources: Raw
    policyItems: list[RangerPolicyItem]
    denyPolicyItems: list[RangerPolicyItem]
    allowExceptions: list[RangerPolicyItem]
    denyExceptions: list[RangerPolicyItem]

    def decode_resource(self):
        last_error = ValidationError()
        for t in (
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
                return json.decode(self.resources, type=t)
            except ValidationError as e:
                last_error = e
        err = f"Unable to decode {json.decode(self.resources)}"
        raise ValueError(err) from last_error


class RangerPolicyList(RangerResultPage):
    policies: list[RangerPolicy]
