import logging
from pathlib import Path

from msgspec import yaml

from cdp_metric_collector.cm_lib import config

from .structs import Config

TYPE_CHECKING = False
if TYPE_CHECKING:
    from cdp_metric_collector.cm_lib.cm import CMAuth

CONFIG_PATH = Path.home() / ".config" / "cdp_metric_collector" / "config.yaml"

logger = logging.getLogger(__name__)


def load_all():
    try:
        c = Config.decode_yaml(CONFIG_PATH.read_bytes())
        load_with(c)
    except Exception as e:
        logger.warning("not loading any config due to error: %s", e)


def load_with(c: Config):
    config._CONFIG = c

    config.CM_API_VER = c.cm.api_ver
    config.CM_AUTH = c.cm.auth
    config.CM_CLUSTER_NAME = c.cm.cluster_name
    config.FILE_BROWSER_PATH = c.cm.file_browser_path
    config.CM_HOST = c.cm.host

    config.HDFS_LANDING_PATH = c.hdfs.landing_path
    config.HDFS_NAMENODE_HOST = c.hdfs.namenode_host
    config.HDFS_REBALANCE_PATH = c.hdfs.rebalance_path
    config.HDFS_REBALANCE_ROLE = c.hdfs.rebalance_role
    config.HDFS_REBALANCE_STATUS = c.hdfs.rebalance_status

    config.FOUNDATION_SCHEMA = c.hive.foundation_schema
    config.HIVE_URL = c.hive.url

    config.HUEQP_HOST = c.hue.qp_host
    config.HUE_USER = c.hue.username

    config.RANGER_HOST = c.ranger.host
    config.SPARK_HISTORY_HOST = c.spark.history_host
    config.YARN_RM_HOST = c.yarn.rm_host


def save_all():
    try:
        CONFIG_PATH.write_bytes(yaml.encode(config._CONFIG))
    except Exception as e:
        logger.warning("unable to save config due to error: %s", e)


def save_auth(auth: "CMAuth"):
    config._CONFIG.cm.auth = auth
    config.save_all()
