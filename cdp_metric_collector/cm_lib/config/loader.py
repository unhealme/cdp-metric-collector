from pathlib import Path

from cdp_metric_collector.cm_lib import config

from .structs import Config


def load_all():
    cp = Path.home() / ".config" / "cdp_metric_collector"
    if not cp.exists():
        cp.mkdir(0o755, parents=True)
    c = Config.decode_yaml((cp / "config.yaml").read_bytes())
    load_with(c)


def load_with(c: Config):
    config.CM_API_VER = c.cm.api_ver
    config.CM_AUTH = c.cm.auth
    config.CM_CLUSTER_NAME = c.cm.cluster_name
    config.FILE_BROWSER_PATH = c.cm.file_browser_path
    config.CM_HOST = c.cm.host

    config.HDFS_LANDING_PATH = c.hdfs.landing_path
    config.HDFS_NAMENODE_HOST = c.hdfs.namenode_host
    config.HDFS_REBALANCE_STATUS = c.hdfs.rebalance_status

    config.FOUNDATION_SCHEMA = c.hive.foundation_schema
    config.HIVE_URL = c.hive.url

    config.HUEQP_HOST = c.hue.qp_host
    config.HUE_USER = c.hue.username

    config.RANGER_HOST = c.ranger.host
    config.SPARK_HISTORY_HOST = c.spark.history_host
    config.YARN_RM_HOST = c.yarn.rm_host
