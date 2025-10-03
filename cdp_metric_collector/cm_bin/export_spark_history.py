__version__ = "b2025.10.01-0"


import argparse
import asyncio
import csv
import logging
import sys
from datetime import datetime
from io import TextIOWrapper
from pathlib import Path

from cdp_metric_collector.cm_lib import config
from cdp_metric_collector.cm_lib.hdfs import HDFSClient
from cdp_metric_collector.cm_lib.spark import (
    ApplicationEnvironment,
    ApplicationNotFoundError,
    AppStatus,
    SparkApplication,
    SparkHistoryClient,
)
from cdp_metric_collector.cm_lib.utils import (
    ARGSBase,
    ConvertibleToString,
    setup_logging,
)

TYPE_CHECKING = False
if TYPE_CHECKING:
    from collections.abc import Sequence


logger = logging.getLogger(__name__)
prog: str | None = None


Row = tuple[ConvertibleToString, ...]


async def process_app(
    spark: SparkHistoryClient,
    hdfs: HDFSClient,
    fast_mode: bool,
    app: SparkApplication,
) -> list[Row]:
    if not fast_mode:
        try:
            env = await spark.environment(app.id)
        except ApplicationNotFoundError:
            logger.debug("unable to get environment for app id %s", app.id)
            return []
    else:
        env = ApplicationEnvironment.new()
    result: list[Row] = []
    for attempt in app.attempts:
        sqlc = 0
        async for sql in hdfs.spark_sql(app.id):
            sqlc += 1
            result.append(
                (
                    app.id,
                    attempt.startTime.isoformat(" "),
                    attempt.endTime.isoformat(" "),
                    attempt.sparkUser,
                    env.get_yarn_queue(),
                    attempt.completed,
                    attempt.duration_parsed,
                    sql.executionId,
                    sql.physicalPlanDescription,
                )
            )
        if sqlc < 1:
            result.append(
                (
                    app.id,
                    attempt.startTime.isoformat(" "),
                    attempt.endTime.isoformat(" "),
                    attempt.sparkUser,
                    env.get_yarn_queue(),
                    attempt.completed,
                    attempt.duration_parsed,
                    "",
                    "",
                )
            )
        logger.debug("found %s sql in app id %s", sqlc, app.id)
    return result


async def main(_args: "Sequence[str] | None" = None):
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
        async with SparkHistoryClient(config.SPARK_HISTORY_HOST) as spark:
            fw.writerow(
                (
                    "Application ID",
                    "Start Time",
                    "End Time",
                    "User",
                    "Queue",
                    "Completed",
                    "Elapsed Time",
                    "Query No",
                    "Query Plan",
                )
            )
            apps = await spark.applications(
                args.status,
                args.min_date,
                args.max_date,
                limit=args.limit,
            )
            logger.debug("fetched %s applications", len(apps))
            for rows in asyncio.as_completed(
                process_app(spark, hdfs, args.fast_mode, x) for x in apps
            ):
                fw.writerows(await rows)


class Arguments(ARGSBase):
    verbose: bool
    status: AppStatus
    min_date: datetime | None
    max_date: datetime | None
    limit: int | None
    output: Path | int
    fast_mode: bool


def parse_args(args: "Sequence[str] | None" = None):
    parser = argparse.ArgumentParser(
        prog=prog,
        add_help=False,
        formatter_class=argparse.RawTextHelpFormatter,
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
        default=sys.stdout.fileno(),
        dest="output",
    )
    parser.add_argument(
        "--fast",
        action="store_true",
        help="enable fast mode (currently disable collection for field: Queue)",
        dest="fast_mode",
    )
    param = parser.add_argument_group("query params")
    param.set_defaults(status=AppStatus.COMPLETED)
    param.add_argument(
        "--from",
        action="store",
        metavar="ISO_TIME",
        type=datetime.fromisoformat,
        default=None,
        dest="min_date",
    )
    param.add_argument(
        "--to",
        action="store",
        metavar="ISO_TIME",
        type=datetime.fromisoformat,
        default=None,
        dest="max_date",
    )
    param.add_argument(
        "--limit",
        action="store",
        type=int,
        default=None,
        dest="limit",
    )
    param.add_argument(
        "--all",
        action="store_const",
        help="export running apps too",
        const=AppStatus.RUNNING,
        dest="status",
    )
    return parser.parse_args(args, Arguments())
