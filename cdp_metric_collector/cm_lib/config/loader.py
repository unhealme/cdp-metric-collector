import logging
from pathlib import Path
from typing import Annotated, get_origin

from msgspec import UNSET, Struct, yaml
from msgspec.structs import fields

from cdp_metric_collector.cm_lib import config

from .structs import CMConfig, Config

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
    set_all(c)


def set_all(c: Struct):
    for f in fields(c):
        if (v := getattr(c, f.name)) is not UNSET:
            if get_origin(f.type) is not Annotated:
                set_all(v)
            else:
                setattr(config, f.type.__metadata__[0], v)


def save_all():
    try:
        CONFIG_PATH.write_bytes(yaml.encode(config._CONFIG))
    except Exception as e:
        logger.warning("unable to save config due to error: %s", e)


def save_cm_auth(auth: "CMAuth"):
    if fp := auth.path:
        Path(fp).write_bytes(yaml.encode(auth.creds))
        return
    if config._CONFIG.cm is UNSET:
        config._CONFIG.cm = CMConfig()
    config._CONFIG.cm.auth = auth.creds
    config.save_all()
