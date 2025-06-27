from logging import DEBUG, INFO, Formatter, Logger, StreamHandler, getLogger
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable

logFormat = Formatter(
    r"%(asctime)s: %(module)s.%(funcName)s: %(levelname)s: %(message)s"
)
logHandler = StreamHandler()
logHandler.setFormatter(logFormat)


def setup_logging(loggers: "Iterable[Logger | str]", debug: bool = False):
    for logger in loggers:
        if isinstance(logger, str):
            logger = getLogger(logger)
        logger.addHandler(logHandler)
        logger.setLevel(DEBUG if debug else INFO)
