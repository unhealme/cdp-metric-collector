__all__ = (
    "PATH",
    "load_all",
    "load_with",
    "save_all",
    "save_auth",
)


from .loader import CONFIG_PATH as PATH
from .loader import load_all, load_with, save_all, save_auth

TYPE_CHECKING = False
if TYPE_CHECKING:
    from cdp_metric_collector.cm_lib.cm import CMAuth

    from .structs import Config

_CONFIG: "Config"

# CM
CM_API_VER: int
CM_AUTH: "CMAuth | None" = None
CM_CLUSTER_NAME: str
FILE_BROWSER_PATH: str
CM_HOST: str

# HDFS
HDFS_LANDING_PATH: str
HDFS_NAMENODE_HOST: list[str]
HDFS_REBALANCE_PATH: str
HDFS_REBALANCE_ROLE: str
HDFS_REBALANCE_STATUS: str

# HIVE
FOUNDATION_SCHEMA: list[str]
HIVE_URL: str

# HUE
HUEQP_HOST: str
HUE_USER: str

# RANGER
RANGER_HOST: str

# SPARK
SPARK_HISTORY_HOST: str

# YARN
YARN_RM_HOST: list[str]
