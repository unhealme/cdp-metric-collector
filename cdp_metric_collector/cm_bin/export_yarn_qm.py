__version__ = "r2025.06.26-0"


import logging
import sys
from argparse import ArgumentParser, RawTextHelpFormatter
from pathlib import Path
from typing import TYPE_CHECKING, Literal, overload

from cdp_metric_collector.cm_lib import config
from cdp_metric_collector.cm_lib.cm import CMAPIClientBase, CMAuth
from cdp_metric_collector.cm_lib.errors import HTTPNotOK
from cdp_metric_collector.cm_lib.structs.yqm import YarnQMResponse
from cdp_metric_collector.cm_lib.utils import (
    ARGSWithAuthBase,
    parse_auth,
    setup_logging,
    wrap_async,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)
prog: str | None = None


class Arguments(ARGSWithAuthBase):
    verbose: bool
    parser: ArgumentParser
    json_file: Path | None
    output: Path | int
    as_json: bool


class YQMCLient(CMAPIClientBase):
    async def initialize(self):
        await self._get_cookies()
        self._client.headers.update(
            {
                "Accept": "application/json, text/plain, */*",
                "Referer": f"{config.CM_HOST}/cmf/clusters/{config.CM_CLUSTER_NAME}/"
                "queue-manager/",
            }
        )

    @overload
    async def get_data(self, raw: Literal[True]) -> bytes: ...
    @overload
    async def get_data(self, raw: bool = False) -> YarnQMResponse: ...
    async def get_data(self, raw: bool = False):
        async with self._client.get(
            f"/cmf/clusters/{config.CM_CLUSTER_NAME}/queue-manager-api/api/v1/environments/dev"
            f"/clusters/{config.CM_CLUSTER_NAME}/resources/scheduler/partitions/default/queues",
            ssl=False,
        ) as resp:
            if resp.status >= 400:
                logger.error(
                    "got response code %s with header: %s",
                    resp.status,
                    resp.headers,
                )
                raise HTTPNotOK(await resp.text())
            data = await resp.read()
        if raw:
            return data
        return await wrap_async(YarnQMResponse.decode_json, data)


async def main(_args: "Sequence[str] | None" = None):
    args = parse_args(_args)
    setup_logging((logger, "cm_lib"), debug=args.verbose)
    logger.debug("got args %s", args)
    if args.json_file:
        data = YarnQMResponse.decode_json(args.json_file.read_bytes())
    else:
        config.load_all()
        auth = args.get_auth()
        if not auth:
            args.parser.error("No auth mechanism is passed")
        async with YQMCLient(config.CM_HOST, auth) as client:
            if args.as_json:
                with open(args.output, "wb") as fo:
                    fo.write(await client.get_data(raw=True))
                return
            data = await client.get_data()
    data.serialize_to_csv(args.output)


def parse_args(args: "Sequence[str] | None" = None):
    parser = ArgumentParser(
        prog=prog,
        add_help=False,
        formatter_class=RawTextHelpFormatter,
    )
    parser.set_defaults(parser=parser)
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
        "json_file",
        action="store",
        help="use FILE as json input instead of fetch from API",
        metavar="FILE",
        type=Path,
        default=None,
        nargs="?",
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
    parser.add_argument(
        "--json",
        action="store_true",
        help="format result as JSON instead of CSV",
        dest="as_json",
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
