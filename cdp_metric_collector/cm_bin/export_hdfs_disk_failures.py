__version__ = "r2025.07.18-0"


import csv
import logging
import sys
from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser
from datetime import datetime
from pathlib import Path

from msgspec import UNSET

from cdp_metric_collector.cm_lib import config
from cdp_metric_collector.cm_lib.errors import HTTPNotOK
from cdp_metric_collector.cm_lib.kerberos import KerberosClientBase
from cdp_metric_collector.cm_lib.structs.hdfs import DFSHealth
from cdp_metric_collector.cm_lib.utils import ARGSBase, setup_logging, wrap_async

TYPE_CHECKING = False
if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)
prog: str | None = None


class Arguments(ARGSBase):
    file: Path | None
    output: Path | None
    verbose: bool


class NameNodeClient(KerberosClientBase):
    nn_hosts: list[str]

    def __init__(self, urls: list[str]) -> None:
        super().__init__("")
        self.nn_hosts = urls

    async def health_status(self):
        body = b""
        for n, host in enumerate(self.nn_hosts):
            async with self._client.stream(
                "GET", f"{host}/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo"
            ) as resp:
                body = await resp.aread()
                if resp.status_code >= 400:
                    logger.error(
                        "got response code %s with header: %s using host: %s",
                        resp.status_code,
                        resp.headers,
                        host,
                    )
                    continue
                if n > 0:
                    self.nn_hosts.insert(0, self.nn_hosts.pop(n))
                return await wrap_async(DFSHealth.decode_json, body)
        raise HTTPNotOK(body.decode())


async def fetch_health():
    async with NameNodeClient(config.HDFS_NAMENODE_HOST) as c:
        return await c.health_status()


async def main(_args: "Sequence[str] | None" = None):
    args = parse_args(_args)
    setup_logging((logger, "cm_lib"), debug=args.verbose)
    logger.debug("got args %s", args)

    if args.file:
        h = DFSHealth.decode_json(args.file.read_bytes())
    else:
        config.load_all()
        h = await fetch_health()
    with open(
        args.output or sys.stdout.fileno(),
        "w",
        encoding="utf-8",
        newline="",
    ) as f:
        fw = csv.writer(f)
        fw.writerow(
            (
                "Hostname",
                "IP",
                "Status",
                "Last Contact",
                "Volume Failures",
                "Last Failure Date",
            )
        )
        now = datetime.now().timestamp()
        for i in h.beans:
            for n, s in i.LiveNodes.items():
                if s.volfails is not UNSET and s.volfails > 0:
                    last_contact = datetime.fromtimestamp(
                        now - s.lastContact
                    ).isoformat(" ", "seconds")
                    last_failure = (
                        datetime.fromtimestamp(
                            s.lastVolumeFailureDate / 1000
                        ).isoformat(" ", "milliseconds")
                        if s.lastVolumeFailureDate is not UNSET
                        else ""
                    )
                    fw.writerow(
                        (
                            n.host,
                            s.xferaddr.host,
                            "Live",
                            last_contact,
                            (
                                "\n".join(s.failedStorageIDs)
                                if s.failedStorageIDs is not UNSET
                                else ""
                            ),
                            last_failure,
                        )
                    )
            for n, s in i.DeadNodes.items():
                last_contact = datetime.fromtimestamp(now - s.lastContact).isoformat(
                    " ", "seconds"
                )
                fw.writerow((n.host, s.xferaddr.host, "Dead", last_contact))


def parse_args(args: "Sequence[str] | None" = None):
    parser = ArgumentParser(
        prog=prog,
        add_help=False,
        formatter_class=ArgumentDefaultsHelpFormatter,
    )
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
        "-o",
        action="store",
        help="dump result to FILE instead of stdout",
        metavar="FILE",
        type=Path,
        default=None,
        dest="output",
    )
    parser.add_argument(
        "--from-file",
        action="store",
        help="load dfs health status from FILE",
        metavar="FILE",
        type=Path,
        default=None,
        dest="file",
    )
    return parser.parse_args(args, Arguments())
