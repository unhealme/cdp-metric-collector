__version__ = "b2025.06.26-0"


import csv
import logging
import sys
from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser
from pathlib import Path

from cdp_metric_collector.cm_lib import config
from cdp_metric_collector.cm_lib.cm import CMAPIClientBase, CMAuth
from cdp_metric_collector.cm_lib.errors import HTTPNotOK
from cdp_metric_collector.cm_lib.structs.cm import HealthIssues
from cdp_metric_collector.cm_lib.utils import (
    ARGSWithAuthBase,
    parse_auth,
    setup_logging,
    wrap_async,
)

TYPE_CHECKING = False
if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)


class Arguments(ARGSWithAuthBase):
    parser: ArgumentParser
    verbose: bool
    output: Path | int
    health_file: Path | None


class CMMetricsClient(CMAPIClientBase):
    async def health_issues(self):
        async with self._client.get(
            "/cmf/healthIssues.json",
            ssl=False,
        ) as resp:
            if resp.status >= 400:
                logger.error(
                    "got response code %s with header: %s",
                    resp.status,
                    resp.headers,
                )
                raise HTTPNotOK(await resp.text())
            return await wrap_async(HealthIssues.decode_json, await resp.read())


async def fetch_health_issues(auth: CMAuth):
    async with CMMetricsClient(config.CM_HOST, auth) as client:
        return await client.health_issues()


async def main(_args: "Sequence[str] | None" = None):
    args = parse_args(_args)
    setup_logging((logger, "cm_lib"), debug=args.verbose)
    logger.debug("got args %s", args)

    config.load_all()
    match args.get_auth(), args.health_file:
        case CMAuth() as auth, None:
            health_issues = await fetch_health_issues(auth)
        case _, Path() as health_file:
            health_issues = HealthIssues.decode_json(health_file.read_bytes())
        case _:
            args.parser.error("No auth mechanism is passed")

    with open(args.output, "w", encoding="utf-8", newline="") as outf:
        fw = csv.writer(outf)
        fw.writerow(
            (
                "Test ID",
                "Health Issue",
                "Status",
                "Entity",
                "Type",
                "Hostname",
                "Cluster",
            )
        )
        fw.writerows(health_issues)


def parse_args(args: "Sequence[str] | None" = None):
    parser = ArgumentParser(
        add_help=False,
        formatter_class=ArgumentDefaultsHelpFormatter,
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
        "--health-file",
        action="store",
        help="load health issues from FILE",
        metavar="FILE",
        type=Path,
        default=None,
        dest="health_file",
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
