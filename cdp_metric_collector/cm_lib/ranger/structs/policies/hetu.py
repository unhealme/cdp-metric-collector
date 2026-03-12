from typing import ClassVar

from . import RangerPolicyResource, RangerResourceBase


class RangerResourceHetuCatalog(RangerResourceBase):
    type: ClassVar[str] = "Catalog"
    catalog: RangerPolicyResource

    def format_values(self):
        yield from self.catalog.values


class RangerResourceHetuColumn(RangerResourceBase):
    type: ClassVar[str] = "Column"
    catalog: RangerPolicyResource
    column: RangerPolicyResource
    schema: RangerPolicyResource
    table: RangerPolicyResource

    def format_values(self):
        for cat in self.catalog.values:
            for s in self.schema.values:
                for t in self.table.values:
                    for col in self.column.values:
                        yield f"{cat}.{s}.{t}.{col}"


class RangerResourceHetuFunction(RangerResourceBase):
    type: ClassVar[str] = "Function"
    function: RangerPolicyResource

    def format_values(self):
        yield from self.function.values


class RangerResourceHetuProcedure(RangerResourceBase):
    type: ClassVar[str] = "Procedure"
    catalog: RangerPolicyResource
    procedure: RangerPolicyResource
    schema: RangerPolicyResource

    def format_values(self):
        for c in self.catalog.values:
            for s in self.schema.values:
                for p in self.procedure.values:
                    yield f"{c}.{s}.{p}"


class RangerResourceHetuSchema(RangerResourceBase):
    type: ClassVar[str] = "Schema"
    catalog: RangerPolicyResource
    schema: RangerPolicyResource

    def format_values(self):
        for c in self.catalog.values:
            for s in self.schema.values:
                yield f"{c}.{s}"


class RangerResourceHetuSessionProperty(RangerResourceBase):
    type: ClassVar[str] = "Session Property"
    catalog: RangerPolicyResource
    sessionproperty: RangerPolicyResource

    def format_values(self):
        for c in self.catalog.values:
            for s in self.sessionproperty.values:
                yield f"{c}.{s}"


class RangerResourceHetuSystemProperty(RangerResourceBase):
    type: ClassVar[str] = "System Property"
    systemproperty: RangerPolicyResource

    def format_values(self):
        yield from self.systemproperty.values


class RangerResourceHetuTable(RangerResourceBase):
    type: ClassVar[str] = "Table"
    catalog: RangerPolicyResource
    schema: RangerPolicyResource
    table: RangerPolicyResource

    def format_values(self):
        for c in self.catalog.values:
            for s in self.schema.values:
                for t in self.table.values:
                    yield f"{c}.{s}.{t}"


class RangerResourceHetuTrinoUser(RangerResourceBase):
    type: ClassVar[str] = "Trino User"
    trinouser: RangerPolicyResource

    def format_values(self):
        yield from self.trinouser.values
