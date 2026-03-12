from typing import ClassVar

from msgspec import field

from . import RangerPolicyResource, RangerResourceBase


class RangerResourceHiveColumn(RangerResourceBase):
    type: ClassVar[str] = "Column"
    column: RangerPolicyResource
    database: RangerPolicyResource
    table: RangerPolicyResource

    def format_values(self):
        for d in self.database.values:
            for t in self.table.values:
                for c in self.column.values:
                    yield f"{d}.{t}.{c}"


class RangerResourceHiveDatabase(RangerResourceBase):
    type: ClassVar[str] = "Database"
    database: RangerPolicyResource

    def format_values(self):
        yield from self.database.values


class RangerResourceHiveGlobal(RangerResourceBase):
    type: ClassVar[str] = "Global"
    global_resource: RangerPolicyResource = field(name="global")

    def format_values(self):
        yield from self.global_resource.values


class RangerResourceHiveService(RangerResourceBase):
    type: ClassVar[str] = "Hive Service"
    hiveservice: RangerPolicyResource

    def format_values(self):
        yield from self.hiveservice.values


class RangerResourceHiveStorage(RangerResourceBase):
    type: ClassVar[str] = "Storage"
    storage_type: RangerPolicyResource = field(name="storage-type")
    storage_url: RangerPolicyResource = field(name="storage-url")

    def format_values(self):
        for st in self.storage_type.values:
            for su in self.storage_url.values:
                yield f"{st}: {su}"


class RangerResourceHiveTable(RangerResourceBase):
    type: ClassVar[str] = "Table"
    database: RangerPolicyResource
    table: RangerPolicyResource

    def format_values(self):
        for d in self.database.values:
            for t in self.table.values:
                yield f"{d}.{t}"


class RangerResourceHiveUDF(RangerResourceBase):
    type: ClassVar[str] = "UDF"
    database: RangerPolicyResource
    udf: RangerPolicyResource

    def format_values(self):
        for d in self.database.values:
            for u in self.udf.values:
                yield f"{d}.{u}"


class RangerResourceHiveURL(RangerResourceBase):
    type: ClassVar[str] = "URL"
    url: RangerPolicyResource

    def format_values(self):
        yield from self.url.values
