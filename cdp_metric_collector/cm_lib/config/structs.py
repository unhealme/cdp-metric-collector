from typing import Annotated

from msgspec import UNSET, Struct, UnsetType

from cdp_metric_collector.cm_lib.cm import Creds
from cdp_metric_collector.cm_lib.structs import Decodable


class CMConfig(Struct):
    api_ver: Annotated[int | UnsetType, "CM_API_VER"] = UNSET
    auth: Annotated[Creds | UnsetType, "CM_AUTH"] = UNSET
    cluster_name: Annotated[str | UnsetType, "CM_CLUSTER_NAME"] = UNSET
    file_browser_path: Annotated[str | UnsetType, "FILE_BROWSER_PATH"] = UNSET
    host: Annotated[str | UnsetType, "CM_HOST"] = UNSET


class HDFSConfig(Struct):
    landing_path: Annotated[str | UnsetType, "HDFS_LANDING_PATH"] = UNSET
    namenode_host: Annotated[list[str] | UnsetType, "HDFS_NAMENODE_HOST"] = UNSET
    rebalance_path: Annotated[str | UnsetType, "HDFS_REBALANCE_PATH"] = UNSET
    rebalance_role: Annotated[str | UnsetType, "HDFS_REBALANCE_ROLE"] = UNSET
    rebalance_status: Annotated[str | UnsetType, "HDFS_REBALANCE_STATUS"] = UNSET


class HiveConfig(Struct):
    foundation_schema: Annotated[list[str] | UnsetType, "FOUNDATION_SCHEMA"] = UNSET
    url: Annotated[str | UnsetType, "HIVE_URL"] = UNSET


class HueConfig(Struct):
    qp_host: Annotated[str | UnsetType, "HUEQP_HOST"] = UNSET
    username: Annotated[str | UnsetType, "HUE_USER"] = UNSET


class RangerConfig(Struct):
    host: Annotated[str | UnsetType, "RANGER_HOST"] = UNSET


class SparkConfig(Struct):
    history_host: Annotated[list[str] | UnsetType, "SPARK_HISTORY_HOST"] = UNSET


class YARNConfig(Struct):
    rm_host: Annotated[list[str] | UnsetType, "YARN_RM_HOST"] = UNSET


class Config(Decodable):
    cm: CMConfig | UnsetType = UNSET
    hdfs: HDFSConfig | UnsetType = UNSET
    hive: HiveConfig | UnsetType = UNSET
    hue: HueConfig | UnsetType = UNSET
    ranger: RangerConfig | UnsetType = UNSET
    spark: SparkConfig | UnsetType = UNSET
    yarn: YARNConfig | UnsetType = UNSET
