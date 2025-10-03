__version__ = "b2025.10.01-0"


import csv
import logging
import sys
from argparse import ArgumentParser, RawTextHelpFormatter
from datetime import datetime
from io import TextIOWrapper
from pathlib import Path

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

HeaderField = (
    "Depth",
    "Mode",
    "Path",
    "Owner",
    "Group",
    "Last Access",
    "Last Modified",
    "Size",
    "Rounded Size",
    "Usage",
    "Rounded Usage",
    "File and Directory Count",
)
logger = logging.getLogger(__name__)
prog: str | None = None


class Arguments(ARGSWithAuthBase):
    verbose: bool
    parser: ArgumentParser
    path: list[str]
    date_older: datetime
    date_newer: datetime
    dir_only: bool
    max_level: int | float
    output: Path | int


async def main(_args: "Sequence[str] | None" = None):
    async def fetch_data(client: CMAPIClient, base_path: str):
        async for path in client.file_browser(base_path):
            level = len(Path(path.Path).parents) - first_level
            if level <= args.max_level:
                if path.is_dir():
                    if args.date_newer < path.LastModified < args.date_older:
                        yield (level, *path)
                    if level < args.max_level:
                        async for r in fetch_data(client, path.Path):
                            yield r
                elif not args.dir_only:
                    if args.date_newer < path.LastModified < args.date_older:
                        yield (level, *path)

    args = parse_args(_args)
    setup_logging(("cdp_metric_collector",), debug=args.verbose)
    logger.debug("got args %s", args)
    if args.max_level < 0:
        args.max_level = float("inf")
    config.load_all()
    auth = args.get_auth()
    if not auth:
        args.parser.error("No auth mechanism is passed")
    with TextIOWrapper(
        open(args.output, "wb", 0),
        encoding="utf-8",
        newline="",
        write_through=True,
    ) as f:
        out = csv.writer(f)
        out.writerow(HeaderField)
        async with CMAPIClient(config.CM_HOST, auth) as c:
            for p in args.path:
                if not Path(p).is_absolute():
                    logger.warning("skipping path: %s, path must be absolute", p)
                first_level = len(Path(p).parents)
                async for row in fetch_data(c, p):
                    out.writerow(row)


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
    parser.add_argument(
        "path",
        action="store",
        nargs="+",
        help="path to be parsed",
        metavar="PATH",
        type=str,
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
        "--older-than",
        action="store",
        dest="date_older",
        help="file is older than DATE (ISO format)",
        metavar="DATE",
        default=datetime.max,
        type=datetime.fromisoformat,
    )
    parser.add_argument(
        "--newer-than",
        action="store",
        dest="date_newer",
        help="file is newer than DATE (ISO format)",
        metavar="DATE",
        default=datetime.min,
        type=datetime.fromisoformat,
    )
    parser.add_argument(
        "--dir-only",
        action="store_true",
        dest="dir_only",
        help="only list directories, exclude files on output",
    )
    parser.add_argument(
        "--max-level",
        action="store",
        dest="max_level",
        help="maximum path level, default to unlimited",
        metavar="NUM",
        default=0,
        type=float,
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
    parser.set_defaults(parser=parser)
    return parser.parse_args(args, Arguments())
