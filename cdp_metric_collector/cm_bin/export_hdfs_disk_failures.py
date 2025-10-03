__version__ = "r2025.10.01-0"


import csv
import logging
import sys
from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser
from datetime import datetime
from pathlib import Path

from msgspec import UNSET

from cdp_metric_collector.cm_lib import config
from cdp_metric_collector.cm_lib.hdfs import DFSHealth, NameNodeClient
from cdp_metric_collector.cm_lib.utils import ARGSBase, setup_logging

TYPE_CHECKING = False
if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)
prog: str | None = None


class Arguments(ARGSBase):
    file: Path | None
    output: Path | None
    verbose: bool


async def fetch_health():
    async with NameNodeClient(config.HDFS_NAMENODE_HOST) as c:
        return await c.health_status()


async def main(_args: "Sequence[str] | None" = None):
    args = parse_args(_args)
    setup_logging(("cdp_metric_collector",), debug=args.verbose)
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
