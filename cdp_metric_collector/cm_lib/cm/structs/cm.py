from datetime import datetime
from enum import Enum, IntEnum
from pathlib import Path

from msgspec import UNSET, Struct, UnsetType, field

from cdp_metric_collector.cm_lib.structs import Decodable, DTNoTZ
from cdp_metric_collector.cm_lib.utils import JSON_ENC, pretty_size


class User(Struct):
    name: str


class Role(Struct):
    users: list[User]
    displayName: str


class AuthRoles(Decodable):
    items: list[Role]


class RoleClass(IntEnum):
    EDGENODE = 0
    WORKER = 1
    MASTER = 2

    def __str__(self):
        return self.name.capitalize()


class ServiceRole(Struct):
    serviceName: str
    roleName: str
    roleStatus: str | UnsetType = UNSET
    clusterName: str | UnsetType = UNSET

    @property
    def roleNameStrip(self):
        return self.roleName.partition("-")[2].rpartition("-")[0]

    @property
    def roleClass(self):
        match self.roleName.partition("-")[2].rpartition("-")[0]:
            case (
                "NODEMANAGER"
                | "DATANODE"
                | "REGIONSERVER"
                | "OZONE_DATANODE"
                | "KUDU_TSERVER"
                | "IMPALAD"
            ):
                return RoleClass.WORKER
            case "GATEWAY" | "S3_GATEWAY":
                return RoleClass.EDGENODE
            case _:
                return RoleClass.MASTER


class Cluster(Struct):
    clusterName: str
    displayName: str | UnsetType = UNSET

    def __str__(self):
        if self.clusterName == self.displayName or not self.displayName:
            return self.clusterName
        return f"{self.displayName} ({self.clusterName})"


class Distribution(Struct):
    distributionType: str
    name: str
    version: str

    def __str__(self):
        return f"{self.distributionType} ({self.name} {self.version})"


class Host(Struct):
    hostId: str
    roleRefs: list[ServiceRole]
    ipAddress: str
    hostname: str
    rackId: str
    commissionState: str
    numCores: int
    totalPhysMemBytes: int
    numPhysicalCores: int | UnsetType = UNSET
    clusterRef: Cluster | UnsetType = UNSET
    distribution: Distribution | UnsetType = UNSET

    @property
    def hostClass(self):
        return max(x.roleClass for x in self.roleRefs)

    @property
    def coreSpec(self):
        if not self.numPhysicalCores:
            return str(self.numCores)
        else:
            return f"{self.numPhysicalCores}/{self.numCores}"


class Hosts(Decodable):
    items: list[Host]


class HealthStatus(Enum):
    RED = "RED"
    YELLOW = "YELLOW"
    GREEN = "GREEN"


class EntityType(Enum):
    ROLE = "ROLE"
    HOST = "HOST"
    SERVICE = "SERVICE"


class UnhealthyCheck(Struct):
    testIdentifier: str
    name: str
    health: HealthStatus
    entityId: str
    entityType: EntityType


class UnhealthyEntity(Struct):
    name: str
    health: HealthStatus
    entityId: str
    entityType: EntityType
    clusterName: str
    hostName: str = ""


class HealthIssues(Decodable):
    unhealthyChecks: list[UnhealthyCheck]
    unhealthyEntities: list[UnhealthyEntity]

    def __iter__(self):
        entities = {x.entityId: x for x in self.unhealthyEntities}
        for health in self.unhealthyChecks:
            entity = entities[health.entityId]
            yield (
                health.testIdentifier,
                health.name,
                health.health.value,
                health.entityId,
                health.entityType.value,
                entity.hostName,
                entity.clusterName,
            )


class APICommand(Decodable):
    id: int
    name: str
    startTime: datetime
    active: bool
    endTime: datetime | UnsetType = UNSET
    success: bool | UnsetType = UNSET
    resultMessage: str | UnsetType = UNSET

    def dump(self, fp: Path | str):
        Path(fp).write_bytes(JSON_ENC.encode(self))


class Commands(Decodable):
    items: list[APICommand]


class FileBrowserPath(DTNoTZ):
    Path: str
    Owner: str
    Group: str
    Mode: str
    LastAccess: datetime = field(name="Last Access")
    LastModified: datetime = field(name="Last Modified")
    Size: str
    Usage: str = field(name="Total Size")
    Content: str = field(name="File and Directory Count")

    def is_dir(self) -> bool:
        return (int(self.Mode, 10) & 0o0170000) == 0o0040000

    def is_file(self) -> bool:
        return (int(self.Mode, 10) & 0o0170000) == 0o0100000

    def __iter__(self):
        yield self.Mode
        yield self.Path
        yield self.Owner
        yield self.Group
        yield self.LastAccess.isoformat(" ")
        yield self.LastModified.isoformat(" ")
        yield self.Size
        yield pretty_size(int(self.Size, 10))
        yield self.Usage
        yield pretty_size(int(self.Usage, 10))
        yield self.Content
