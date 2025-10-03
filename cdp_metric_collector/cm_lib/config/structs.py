from msgspec import Struct

from cdp_metric_collector.cm_lib.cm import CMAuth
from cdp_metric_collector.cm_lib.structs import Decodable


class CMConfig(Struct):
    api_ver: int
    auth: CMAuth
    cluster_name: str
    file_browser_path: str
    host: str


class HDFSConfig(Struct):
    landing_path: str
    namenode_host: list[str]
    rebalance_path: str
    rebalance_role: str
    rebalance_status: str


class HiveConfig(Struct):
    foundation_schema: list[str]
    url: str


class HueConfig(Struct):
    qp_host: str
    username: str


class RangerConfig(Struct):
    host: str


class SparkConfig(Struct):
    history_host: str


class YARNConfig(Struct):
    rm_host: list[str]


class Config(Decodable):
    cm: CMConfig
    hdfs: HDFSConfig
    hive: HiveConfig
    hue: HueConfig
    ranger: RangerConfig
    spark: SparkConfig
    yarn: YARNConfig
