__version__ = "b2025.06.26-0"


import asyncio
import csv
import logging
import sys
from argparse import ArgumentParser, RawTextHelpFormatter
from datetime import datetime
from io import StringIO, TextIOWrapper
from pathlib import Path
from urllib.parse import urlparse

from aiohttp import ClientError
from msgspec import convert, field

from cm_lib import config
from cm_lib.cm import CMAPIClientBase, CMAuth
from cm_lib.errors import HTTPNotOK
from cm_lib.structs import DTNoTZ
from cm_lib.utils import (
    ARGSWithAuthBase,
    encode_json_str,
    join_url,
    parse_auth,
    pretty_size,
    setup_logging,
)

TYPE_CHECKING = False
if TYPE_CHECKING:
    from collections.abc import Sequence

HeaderField = (
    "Level",
    "Path",
    "Owner",
    "Group",
    "Mode",
    "Last Access",
    "Last Modified",
    "Size",
    "Rounded Size",
    "Usage",
    "Rounded Usage",
    "File and Directory Count",
)
logger = logging.getLogger(__name__)


class Arguments(ARGSWithAuthBase):
    verbose: bool
    parser: ArgumentParser
    path: list[str]
    date_older: datetime
    date_newer: datetime
    dir_only: bool
    max_level: int | float
    output: Path | int


class RowPath(DTNoTZ):
    Path: str
    Owner: str
    Group: str
    Mode: str
    LastAccess: datetime = field(name="Last Access")
    LastModified: datetime = field(name="Last Modified")
    Size: str
    Usage: str = field(name="Total Size")
    Content: str = field(name="File and Directory Count")

    def is_dir(self) -> bool:
        return (int(self.Mode, 10) & 0o0170000) == 0o0040000

    def is_file(self) -> bool:
        return (int(self.Mode, 10) & 0o0170000) == 0o0100000

    def __iter__(self):
        yield self.Path
        yield self.Owner
        yield self.Group
        yield self.Mode
        yield self.LastAccess.isoformat(" ")
        yield self.LastModified.isoformat(" ")
        yield self.Size
        yield pretty_size(int(self.Size))
        yield self.Usage
        yield pretty_size(int(self.Usage))
        yield self.Content


class FileBrowserClient(CMAPIClientBase):
    api_path: str

    def __init__(self, url: str, auth: CMAuth) -> None:
        parsed = urlparse(url)
        self.api_path = parsed.path
        super().__init__(f"{parsed.scheme}://{parsed.netloc}", auth)

    async def iter_path(self, path: str):
        retry = 1
        while True:
            try:
                async with self._client.get(
                    self.api_path,
                    ssl=False,
                    params={
                        "limit": "0",
                        "offset": "0",
                        "format": "CSV",
                        "path": path,
                        "json": encode_json_str(
                            {
                                "terms": [
                                    {
                                        "fileSearchType": 12,
                                        "queryText": path,
                                        "negated": False,
                                    }
                                ]
                            }
                        ),
                        "sortBy": "FILENAME",
                        "sortReverse": "false",
                    },
                ) as resp:
                    if resp.status >= 400:
                        logger.error(
                            "got response code %s with header: %s",
                            resp.status,
                            resp.headers,
                        )
                        raise HTTPNotOK(await resp.text())
                    data = StringIO(await resp.text(), newline="")
                with data:
                    for row in csv.DictReader(data, restkey="_"):
                        yield convert(row, RowPath)
                break
            except ClientError:
                logger.warning("connection error retries %s", retry, exc_info=True)
                retry += 1
                await asyncio.sleep(5)


async def main(_args: "Sequence[str] | None" = None):
    async def fetch_data(client: FileBrowserClient, base_path: str):
        async for path in client.iter_path(base_path):
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
    setup_logging((logger, "cm_lib"), debug=args.verbose)
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
        async with FileBrowserClient(
            join_url(config.CM_HOST, config.FILE_BROWSER_PATH), auth
        ) as client:
            for p in args.path:
                if not Path(p).is_absolute():
                    logger.warning("skipping path: %s, path must be absolute", p)
                first_level = len(Path(p).parents)
                async for row in fetch_data(client, p):
                    out.writerow(row)


def parse_args(args: "Sequence[str] | None" = None):
    parser = ArgumentParser(
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


def __main__():
    asyncio.run(main())
