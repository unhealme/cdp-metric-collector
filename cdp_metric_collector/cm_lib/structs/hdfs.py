from enum import Enum

from msgspec import UNSET, Struct, UnsetType, json

from cdp_metric_collector.cm_lib.utils import ABC, pretty_size

from ._abc import Decodable

TYPE_CHECKING = False
if TYPE_CHECKING:
    from typing import Any


class FileType(Enum):
    FILE = "FILE"
    DIRECTORY = "DIRECTORY"
    SYMLINK = "SYMLINK"


class StorageType(Struct):
    consumed: int  # The storage type space consumed.
    quota: int  # The storage type quota.


class QuotaType(Struct):
    ARCHIVE: StorageType | UnsetType = UNSET
    DISK: StorageType | UnsetType = UNSET
    SSD: StorageType | UnsetType = UNSET


class ContentSummaryProperties(Struct):
    directoryCount: int  # The number of directories.
    fileCount: int  # The number of files.
    length: int  # The number of bytes used by the content.
    quota: int  # The namespace quota of this directory.
    spaceConsumed: int  # The disk space consumed by the content.
    spaceQuota: int  # The disk space quota.
    typeQuota: QuotaType

    @property
    def length_hr(self):
        return pretty_size(self.length)

    @property
    def spaceConsumed_hr(self):
        return pretty_size(self.spaceConsumed)

    @property
    def spaceQuota_hr(self):
        return pretty_size(self.spaceQuota)

    @property
    def spaceConsumed_perc(self):
        if self.spaceQuota > 0:
            perc = self.spaceConsumed / self.spaceQuota
            return f"{perc:.2%}"
        return "0.00%"


class ContentSummary(Decodable):
    ContentSummary: ContentSummaryProperties


class FileStatusProperties(Struct):
    accessTime: int  # The access time.
    blockSize: int  # The block size of a file.
    group: str  # The group owner.
    length: int  # The number of bytes in a file.
    modificationTime: int  # The modification time
    owner: str  # The user who is the owner.
    pathSuffix: str  # The path suffix.
    permission: str  # The permission represented as a octal string.
    replication: int  # The number of replication of a file.
    type: FileType  # The type of the path object.
    symlink: str | UnsetType = UNSET  # The link target of a symlink.
    aclBit: bool | UnsetType = UNSET  # Has ACLs set or not.
    encBit: bool | UnsetType = UNSET  # Is Encrypted or not.
    ecBit: bool | UnsetType = UNSET  # Is ErasureCoded or not.
    ecPolicy: str | UnsetType = UNSET  # The namenode of ErasureCodePolicy.


class FileStatus(Decodable):
    FileStatus: FileStatusProperties


class FileStatusArray(Struct):
    FileStatus: list[FileStatusProperties]


class FileStatuses(Decodable):
    FileStatuses: FileStatusArray


# namenode info at https://namenode_host/jmx


class Addr(ABC):
    host: str
    port: int

    def __init__(self, value: str):
        host, _, port = value.partition(":")
        self.host = host
        self.port = int(port, 10)

    def __hash__(self):
        return hash((self.host, self.port))


class NodeStatus(Struct):
    lastContact: int
    xferaddr: Addr
    volfails: int | UnsetType = UNSET
    failedStorageIDs: list[str] | UnsetType = UNSET
    lastVolumeFailureDate: int | UnsetType = UNSET
    # usedSpace: int
    # capacity: int
    # remaining: int
    # numBlocks: int


class Nodes(dict[Addr, NodeStatus]):
    __slots__ = ()


class NameNodeInfo(Struct):
    LiveNodes: Nodes
    DeadNodes: Nodes


def DFSHealth_dec_hook(t: type, v: "Any") -> "Any":
    if t is Nodes:
        return Nodes(NodesJDec.decode(v))
    elif t is Addr:
        return Addr(v)
    err = f"type {type} is not implemented"
    raise NotImplementedError(err)


NodesJDec = json.Decoder(dict[Addr, NodeStatus], dec_hook=DFSHealth_dec_hook)


class DFSHealth(Decodable):
    __dec_hook__ = DFSHealth_dec_hook

    beans: list[NameNodeInfo]
