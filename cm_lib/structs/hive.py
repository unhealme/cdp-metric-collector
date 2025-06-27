from typing import Any, Self

from msgspec import UNSET, Struct, UnsetType, field

from cm_lib.utils import encode_json_str

from ._abc import Decodable, Progressive


class DagInfoData(Struct):
    applicationId: str
    initTime: int | None
    startTime: int
    endTime: int | None
    status: str
    queueName: str | None


class DagInfo(Struct):
    dagInfo: DagInfoData


class QueryInfo(Progressive):
    dags: list[DagInfo]
    queryId: str


class Table(Struct):
    table: str
    database: str

    def __str__(self) -> str:
        return f"{self.database}.{self.table}"


class QueryDetailsConfig(Struct, omit_defaults=True):
    hive_hdfs_session_path: str | UnsetType = field(
        name="_hive.hdfs.session.path", default=UNSET
    )
    hive_server2_thrift_bind_host: str | UnsetType = field(
        name="hive.server2.thrift.bind.host", default=UNSET
    )
    hive_tez_container_size: str | UnsetType = field(
        name="hive.tez.container.size", default=UNSET
    )
    tez_am_resource_cpu_vcores: str | UnsetType = field(
        name="tez.am.resource.cpu.vcores", default=UNSET
    )
    tez_am_resource_memory_mb: str | UnsetType = field(
        name="tez.am.resource.memory.mb", default=UNSET
    )
    tez_queue_name: str | UnsetType = field(name="tez.queue.name", default=UNSET)
    tez_runtime_shuffle_fetch_buffer_percent: str | UnsetType = field(
        name="tez.runtime.shuffle.fetch.buffer.percent", default=UNSET
    )
    tez_submit_host_address: str | UnsetType = field(
        name="tez.submit.host.address", default=UNSET
    )
    tez_submit_host: str | UnsetType = field(name="tez.submit.host", default=UNSET)
    tez_task_resource_cpu_vcores: str | UnsetType = field(
        name="tez.task.resource.cpu.vcores", default=UNSET
    )
    tez_task_resource_memory_mb: str | UnsetType = field(
        name="tez.task.resource.memory.mb", default=UNSET
    )


class QueryExtendedDetails(Struct):
    configuration: QueryDetailsConfig | None

    def get_config(self):
        if self.configuration:
            return encode_json_str(self.configuration)
        return ""


class QueryExtendedData(QueryInfo):
    details: QueryExtendedDetails
    query: str | None
    elapsedTime: None
    status: str
    queueName: str
    requestUser: str
    dataRead: int | None
    dataWritten: int | None
    executionMode: str
    tablesRead: list[Table]
    tablesWritten: list[Table]
    databasesUsed: list[dict[str, int]]
    usedCBO: bool


class QueryExtendedInfo(Decodable):
    query: QueryExtendedData


class QuerySearchMeta(Struct):
    limit: int
    offset: int
    size: int
    updateTime: int


class QuerySearchResult(Decodable):
    queries: list[QueryInfo]
    meta: QuerySearchMeta


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

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, self.__class__):
            return self.name.__eq__(other.name)
        return super().__eq__(other)

    def __lt__(self, other: Self):
        return self.name.__lt__(other.name)
