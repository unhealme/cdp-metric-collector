__version__ = "b2025.06.26-0"


import logging
import sys
from argparse import ArgumentParser, RawTextHelpFormatter
from datetime import datetime
from enum import Enum
from pathlib import Path

from msgspec import UNSET, Struct, UnsetType, field

from cdp_metric_collector.cm_lib import config
from cdp_metric_collector.cm_lib.cm import CMAPIClientBase, CMAuth
from cdp_metric_collector.cm_lib.errors import HTTPNotOK
from cdp_metric_collector.cm_lib.utils import (
    ARGSWithAuthBase,
    ensure_api_ver,
    parse_auth,
    setup_logging,
)

TYPE_CHECKING = False
if TYPE_CHECKING:
    from collections.abc import Sequence
    from typing import Any

logger = logging.getLogger(__name__)


class ContentType(Enum):
    JSON = "application/json"
    CSV = "text/csv"


class RollupType(Enum):
    RAW = "RAW"
    TEN_MINUTELY = "TEN_MINUTELY"
    HOURLY = "HOURLY"
    SIX_HOURLY = "SIX_HOURLY"
    DAILY = "DAILY"
    WEEKLY = "WEEKLY"

    @classmethod
    def _missing_(cls, value: "str | Any"):
        for member in cls:
            if member.name.lower() == value.lower():
                return member
        return None

    def __str__(self) -> str:
        return self.name


class Arguments(ARGSWithAuthBase):
    parser: ArgumentParser
    verbose: bool
    content_type: ContentType
    rollup: RollupType | None
    query: str | None
    query_file: Path | None
    from_dt: datetime | None
    to_dt: datetime | None
    output: Path | int


class TimeSeriesPayload(Struct):
    query: str
    from_dt: str | UnsetType = field(name="from", default=UNSET)
    to_dt: str | UnsetType = field(name="to", default=UNSET)
    contentType: str | UnsetType = UNSET
    desiredRollup: str | UnsetType = UNSET
    mustUseDesiredRollup: bool | UnsetType = UNSET


class CMMetricsClient(CMAPIClientBase):
    async def timeseries(
        self,
        query: str,
        *,
        from_dt: datetime | None = None,
        to_dt: datetime | None = None,
        content_type: ContentType | None = None,
        rollup: RollupType | None = None,
        force_rollup: bool | None = None,
    ):
        data = TimeSeriesPayload(query)
        if from_dt:
            data.from_dt = from_dt.isoformat()
        if to_dt:
            data.to_dt = to_dt.isoformat()
        if content_type:
            data.contentType = content_type.value
        if rollup:
            data.desiredRollup = rollup.value
        if force_rollup is not None:
            data.mustUseDesiredRollup = force_rollup
        logger.debug("sending payload %s", data)
        async with self._client.post(
            f"/api/v{config.CM_API_VER}/timeseries",
            json=data,
            ssl=False,
        ) as resp:
            if resp.status >= 400:
                logger.error(
                    "got response code %s with header: %s",
                    resp.status,
                    resp.headers,
                )
                raise HTTPNotOK(await resp.text())
            return await resp.read()


async def main(_args: "Sequence[str] | None" = None):
    args = parse_args(_args)
    setup_logging((logger, "cm_lib"), debug=args.verbose)
    logger.debug("got args %s", args)

    config.load_all()
    auth = args.get_auth()
    if not auth:
        args.parser.error("No auth mechanism is passed")

    if args.query_file:
        query = args.query_file.read_text("utf-8")
    elif not args.query:
        args.parser.error("No query is passed")
    elif args.query == "-":
        with open(sys.stdin.fileno(), "r", encoding="utf-8") as f_in:
            query = f_in.read()
    else:
        query = args.query

    ensure_api_ver(11, config.CM_API_VER)
    async with CMMetricsClient(config.CM_HOST, auth) as client:
        with open(args.output, "wb") as f_out:
            f_out.write(
                await client.timeseries(
                    query,
                    from_dt=args.from_dt,
                    to_dt=args.to_dt,
                    content_type=args.content_type,
                    rollup=args.rollup,
                )
            )


def parse_args(args: "Sequence[str] | None" = None):
    parser = ArgumentParser(
        add_help=False,
        formatter_class=RawTextHelpFormatter,
    )
    parser.set_defaults(parser=parser, content_type=ContentType.CSV)
    misc = parser.add_argument_group()
    misc.add_argument("-h", "--help", action="help", help="print this help and exit")
    misc.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="enable verbose mode",
        dest="verbose",
    )
    misc.add_argument(
        "--version",
        action="version",
        help="print version",
        version=f"%(prog)s {__version__}",
    )
    parser.add_argument(
        "query",
        action="store",
        metavar="QUERY",
        type=str,
        default=None,
        nargs="?",
    )
    parser.add_argument(
        "-q",
        action="store",
        help="use query from FILE",
        metavar="FILE",
        type=Path,
        default=None,
        dest="query_file",
    )
    parser.add_argument(
        "-o",
        action="store",
        help="dump result to FILE instead of stdout",
        metavar="FILE",
        type=Path,
        default=sys.stdout.fileno(),
        dest="output",
    )
    param = parser.add_argument_group("query params")
    param.add_argument(
        "--from",
        action="store",
        metavar="ISO_TIME",
        type=datetime.fromisoformat,
        default=None,
        dest="from_dt",
    )
    param.add_argument(
        "--to",
        action="store",
        metavar="ISO_TIME",
        type=datetime.fromisoformat,
        default=None,
        dest="to_dt",
    )
    param.add_argument(
        "--rollup",
        action="store",
        type=RollupType,
        choices=tuple(RollupType),
        default=None,
        dest="rollup",
    )
    param.add_argument(
        "--json",
        action="store_const",
        help="get metrics as json",
        const=ContentType.JSON,
        dest="content_type",
    )
    param.add_argument(
        "--csv",
        action="store_const",
        help="get metrics as CSV (default)",
        const=ContentType.CSV,
        dest="content_type",
    )
    auth = parser.add_argument_group("authentication")
    auth.add_argument(
        "-c",
        "--config",
        action="store",
        help="authentication config file path",
        metavar="FILE",
        type=CMAuth.from_path,
        default=None,
        dest="auth_config",
    )
    auth.add_argument(
        "-u",
        action="store",
        metavar="USER:PASS",
        type=parse_auth,
        default=None,
        dest="auth_basic",
    )
    auth.add_argument(
        "-s",
        action="store",
        metavar="SESSION_ID",
        type=str,
        default=None,
        dest="auth_session",
    )
    auth.add_argument(
        "-t",
        action="store",
        metavar="BASE64_TOKEN",
        type=str,
        default=None,
        dest="auth_header",
    )
    return parser.parse_args(args, Arguments())


def __main__():
    import asyncio

    asyncio.run(main())
