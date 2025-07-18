__version__ = "b2025.06.26-0"


import csv
import logging
import sys
from argparse import ArgumentParser, RawTextHelpFormatter
from enum import Enum
from io import TextIOWrapper
from pathlib import Path
from typing import TYPE_CHECKING, Literal, overload
from urllib.parse import urlparse

from msgspec import convert

from cdp_metric_collector.cm_lib import config
from cdp_metric_collector.cm_lib.hdfs import HDFSClientBase
from cdp_metric_collector.cm_lib.hive import HiveClientBase, ensure_init
from cdp_metric_collector.cm_lib.structs.hdfs import (
    ContentSummary,
    FileStatus,
    FileStatuses,
    FileStatusProperties,
    FileType,
)
from cdp_metric_collector.cm_lib.structs.hive import HiveDatabase
from cdp_metric_collector.cm_lib.utils import ARGSBase, setup_logging

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)


class R(Enum):
    LANDING = 0
    SCHEMA = 1


class Arguments(ARGSBase):
    verbose: bool
    mode: R
    hive_url: str | None
    output: Path | int


class HiveClient(HiveClientBase):
    @overload
    def get_all_databases(self) -> list[str]: ...
    @overload
    def get_all_databases(self, expand: Literal[True]) -> list[HiveDatabase]: ...
    @overload
    def get_all_databases(
        self, expand: bool = False
    ) -> list[HiveDatabase] | list[str]: ...
    @ensure_init
    def get_all_databases(self, expand: bool = False):
        self._hivecur.execute("show databases")
        if expand:
            return sorted(
                self.get_database(str(x[0])) for x in self._hivecur.fetchall()
            )
        return sorted(str(x[0]) for x in self._hivecur)

    @ensure_init
    def get_database(self, name: str):
        self._hivecur.execute(f"desc schema {name}")
        return convert(self._hivecur.fetchone(), HiveDatabase)


class HDFSClient(HDFSClientBase):
    def get_content(self, path: str):
        return ContentSummary.decode_json(
            self._get_content_summary(path, strict=True).content  # type: ignore
        ).ContentSummary

    def get_status(self, path: str):
        return FileStatus.decode_json(
            self._get_file_status(path, strict=True).content  # type: ignore
        ).FileStatus

    @overload
    def get_list(self, path: str) -> list[str]: ...
    @overload
    def get_list(
        self,
        path: str,
        status: Literal[True],
    ) -> list[tuple[str, FileStatusProperties]]: ...
    @overload
    def get_list(
        self,
        path: str,
        status: bool = False,
    ) -> list[str] | list[tuple[str, FileStatusProperties]]: ...
    def get_list(self, path: str, status: bool = False):
        full_path = self.resolve(path)
        statuses = FileStatuses.decode_json(
            self._list_status(full_path).content  # type: ignore
        ).FileStatuses.FileStatus
        match statuses:
            case [fs] if (
                not fs.pathSuffix or self.get_status(full_path).type is FileType.FILE
            ):
                err = f"{full_path} is not a directory"
                raise TypeError(err)
        if status:
            return [(f"{path}/{x.pathSuffix}", x) for x in statuses]
        return [f"{path}/{x.pathSuffix}" for x in statuses]


def fetch_schema(hive: HiveClient, hdfs: HDFSClient):
    for db in hive.get_all_databases(expand=True):
        logger.debug("getting data for schema %s", db)
        db_loc = urlparse(db.location).path
        content = hdfs.get_content(db_loc)
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
    for fp in hdfs.get_list(config.HDFS_LANDING_PATH):
        logger.debug("getting data for path %s", fp)
        content = hdfs.get_content(fp)
        yield (
            fp,
            str(content.fileCount + content.directoryCount),
            content.spaceQuota_hr,
            content.spaceConsumed_hr,
            content.spaceConsumed_perc,
        )


def main(_args: "Sequence[str] | None" = None):
    args = parse_args(_args)
    setup_logging((logger, "cm_lib"), debug=args.verbose)
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
