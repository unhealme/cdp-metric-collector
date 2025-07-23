__version__ = "r2025.06.26-0"


import csv
import logging
import sys
from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser
from pathlib import Path

from msgspec import UNSET

from cdp_metric_collector.cm_lib import config
from cdp_metric_collector.cm_lib.cm import CMAPIClientBase, CMAuth
from cdp_metric_collector.cm_lib.errors import HTTPNotOK
from cdp_metric_collector.cm_lib.structs.cm import Hosts
from cdp_metric_collector.cm_lib.utils import (
    ARGSWithAuthBase,
    parse_auth,
    pretty_size,
    setup_logging,
    wrap_async,
)

TYPE_CHECKING = False
if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)
prog: str | None = None


class Arguments(ARGSWithAuthBase):
    parser: ArgumentParser
    verbose: bool
    output: Path | int
    hosts_file: Path | None


class CMMetricsClient(CMAPIClientBase):
    async def hosts(self):
        async with self._client.get(
            f"/api/v{config.CM_API_VER}/hosts",
            params="view=FULL",
            ssl=False,
        ) as resp:
            if resp.status >= 400:
                logger.error(
                    "got response code %s with header: %s",
                    resp.status,
                    resp.headers,
                )
                raise HTTPNotOK(await resp.text())
            return await wrap_async(Hosts.decode_json, await resp.read())


async def fetch_hosts(auth: CMAuth):
    async with CMMetricsClient(config.CM_HOST, auth) as client:
        return await client.hosts()


async def main(_args: "Sequence[str] | None" = None):
    args = parse_args(_args)
    setup_logging(("cdp_metric_collector",), debug=args.verbose)
    logger.debug("got args %s", args)

    config.load_all()
    match args.get_auth(), args.hosts_file:
        case CMAuth() as auth, None:
            hosts = await fetch_hosts(auth)
        case _, Path() as hosts_file:
            hosts = Hosts.decode_json(hosts_file.read_bytes())
        case _:
            args.parser.error("No auth mechanism is passed")

    with open(args.output, "w", encoding="utf-8", newline="") as outf:
        fw = csv.writer(outf)
        fw.writerow(
            (
                "Cluster",
                "Hostname",
                "IP",
                "Rack",
                "CPU",
                "Memory",
                "OS",
                "Commission",
                "Service Name",
                "Role Name",
                "Role Status",
                "Class",
            )
        )
        for host in hosts.items:
            for role in host.roleRefs:
                if role.clusterName is not UNSET:
                    fw.writerow(
                        (
                            str(host.clusterRef or ""),
                            host.hostname,
                            host.ipAddress,
                            host.rackId,
                            host.coreSpec,
                            pretty_size(host.totalPhysMemBytes),
                            str(host.distribution or ""),
                            host.commissionState,
                            role.serviceName,
                            role.roleNameStrip,
                            role.roleStatus or "",
                            str(host.hostClass),
                        )
                    )


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
        "--hosts-file",
        action="store",
        help="load hosts from FILE",
        metavar="FILE",
        type=Path,
        default=None,
        dest="hosts_file",
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
