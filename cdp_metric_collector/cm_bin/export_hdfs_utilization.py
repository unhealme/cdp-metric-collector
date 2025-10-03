__version__ = "r2025.10.01-0"


import csv
import logging
import sys
from argparse import ArgumentParser, RawTextHelpFormatter
from enum import Enum
from io import TextIOWrapper
from pathlib import Path
from urllib.parse import urlparse

from cdp_metric_collector.cm_lib import config
from cdp_metric_collector.cm_lib.hdfs import HDFSClient
from cdp_metric_collector.cm_lib.hive import HiveClient
from cdp_metric_collector.cm_lib.utils import ARGSBase, setup_logging

TYPE_CHECKING = False
if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)
prog: str | None = None


class R(Enum):
    LANDING = 0
    SCHEMA = 1


class Arguments(ARGSBase):
    verbose: bool
    mode: R
    hive_url: str | None
    output: Path | int


def fetch_schema(hive: HiveClient, hdfs: HDFSClient):
    for db in hive.databases(expand=True):
        logger.debug("getting data for schema %s", db)
        db_loc = urlparse(db.location).path
        content = hdfs.content(db_loc)
        yield (
            db.name,
            db_loc,
            str(content.fileCount + content.directoryCount),
            content.spaceQuota_hr,
            content.spaceConsumed_hr,
            content.spaceConsumed_perc,
            "Foundation" if db.name in config.FOUNDATION_SCHEMA else "Sandbox",
        )


def fetch_landing(hdfs: HDFSClient):
    for fp in hdfs.list(config.HDFS_LANDING_PATH):
        logger.debug("getting data for path %s", fp)
        content = hdfs.content(fp)
        yield (
            fp,
            str(content.fileCount + content.directoryCount),
            content.spaceQuota_hr,
            content.spaceConsumed_hr,
            content.spaceConsumed_perc,
        )


def main(_args: "Sequence[str] | None" = None):
    args = parse_args(_args)
    setup_logging(("cdp_metric_collector",), debug=args.verbose)
    logger.debug("got args %s", args)

    config.load_all()
    with TextIOWrapper(
        open(args.output, "wb", 0),
        newline="",
        encoding="utf-8",
        write_through=True,
    ) as f:
        fw = csv.writer(f)
        hdfs = HDFSClient(";".join(config.HDFS_NAMENODE_HOST))
        match args.mode:
            case R.SCHEMA:
                with HiveClient(args.hive_url or config.HIVE_URL) as hive:
                    fw.writerow(
                        (
                            "Database",
                            "Location",
                            "File Count",
                            "Quota",
                            "Usage",
                            "Percentage",
                            "Type",
                        )
                    )
                    fw.writerows(fetch_schema(hive, hdfs))
            case R.LANDING:
                fw.writerow(
                    (
                        "Path",
                        "File Count",
                        "Quota",
                        "Usage",
                        "Percentage",
                    )
                )
                fw.writerows(fetch_landing(hdfs))


def parse_args(args: "Sequence[str] | None" = None):
    parser = ArgumentParser(
        prog=prog,
        add_help=False,
        formatter_class=RawTextHelpFormatter,
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
    subparser = parser.add_subparsers(required=True, metavar="mode")
    schema = subparser.add_parser("schema", help="export hive schema utilization")
    schema.set_defaults(mode=R.SCHEMA)
    schema.add_argument(
        "-u",
        action="store",
        metavar="HIVE_URL",
        default=None,
        dest="hive_url",
    )
    schema.add_argument(
        "-o",
        action="store",
        help="dump result to FILE instead of stdout",
        metavar="FILE",
        type=Path,
        default=sys.stdout.fileno(),
        dest="output",
    )
    landing = subparser.add_parser("landing", help="export hdfs landing utilization")
    landing.set_defaults(mode=R.LANDING)
    landing.add_argument(
        "-o",
        action="store",
        help="dump result to FILE instead of stdout",
        metavar="FILE",
        type=Path,
        default=sys.stdout.fileno(),
        dest="output",
    )
    return parser.parse_args(args, Arguments())
