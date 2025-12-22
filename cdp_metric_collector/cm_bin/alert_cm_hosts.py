__version__ = "b2025.11.28-0"

import csv
import logging
import sys
from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser
from datetime import datetime
from ipaddress import ip_address, ip_network
from socket import gethostbyname

from cdp_metric_collector.cm_lib import config
from cdp_metric_collector.cm_lib.cm import CMAPIClient, CMAuth
from cdp_metric_collector.cm_lib.utils import (
    ARGSWithAuthBase,
    parse_auth,
    setup_logging,
)

TYPE_CHECKING = False
if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)
prog: str | None = None

HeaderField = ("Time", "Hostname", "Current IP", "Actual IP")


class Arguments(ARGSWithAuthBase):
    parser: ArgumentParser
    verbose: bool


async def main(_args: "Sequence[str] | None" = None):
    args = parse_args(_args)
    setup_logging(("cdp_metric_collector",), debug=args.verbose)
    logger.debug("got args %s", args)

    config.load_all()
    if (auth := args.get_auth()) is None:
        args.parser.error("No auth mechanism is passed")

    network = ip_network(config.CM_SUBNET)
    now = datetime.now()
    async with CMAPIClient(config.CM_HOST, auth) as c:
        hosts = await c.hosts()
    with (
        open(sys.stdout.fileno(), "w", encoding="utf-8") as f,
        open("hadoop-topologies.log", "a", encoding="utf-8", newline="") as flog,
    ):
        fw = csv.writer(flog)
        if flog.tell() == 0:
            fw.writerow(HeaderField)
        for host in hosts.items:
            if ip_address(host.ipAddress) not in network:
                fw.writerow(
                    (
                        now.isoformat(" ", "milliseconds"),
                        host.hostname,
                        host.ipAddress,
                        gethostbyname(host.hostname),
                    )
                )
                f.write(host.hostname + "\n")


def parse_args(args: "Sequence[str] | None" = None):
    parser = ArgumentParser(
        prog=prog,
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
