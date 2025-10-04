__version__ = "u2025.07.31-0"

import logging

from cdp_metric_collector.cm_lib.utils import ARGSWithAuthBase

logger = logging.getLogger(__name__)
prog: str | None = None


class Arguments(ARGSWithAuthBase):
    pass
