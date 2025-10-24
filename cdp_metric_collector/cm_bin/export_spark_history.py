__version__ = "r2025.10.24-4"


import argparse
import csv
import logging
import sys
from asyncio.locks import Semaphore
from asyncio.tasks import as_completed
from datetime import datetime
from io import TextIOWrapper

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


class Arguments(ARGSBase):
    verbose: bool
    status: AppStatus | None
    min_date: datetime | None
    max_date: datetime | None
    limit: int | None
    output: str | None
    fast_mode: bool
    with_sql: bool


async def process_app(
    spark: SparkHistoryClient,
    hdfs: HDFSClient,
    fast_mode: bool,
    with_sql: bool,
    app: SparkApplication,
) -> list[Row]:
    logger.debug("processing %s", app.id)
    if not fast_mode:
        try:
            env = await spark.environment(app.id)
        except ApplicationNotFoundError:
            logger.debug("unable to get environment for app id %s", app.id)
            return []
    else:
        env = ApplicationEnvironment.new()
    result: list[Row] = []
    for attempt_id, attempt in enumerate(app.attempts, 1):
        c = 0
        if with_sql:
            async for sql in hdfs.spark_sql(app.id):
                c += 1
                result.append(
                    (
                        attempt.appSparkMajorVersion,
                        app.id,
                        attempt_id,
                        attempt.startTime.isoformat(" "),
                        attempt.endTime.isoformat(" "),
                        attempt.sparkUser,
                        env.get_yarn_queue(),
                        attempt.completed,
                        attempt.durationParsed,
                        sql.executionId,
                        sql.physicalPlanDescription,
                    )
                )
            logger.debug("found %s sql in app id %s", c, app.id)
        if c < 1:
            result.append(
                (
                    attempt.appSparkMajorVersion,
                    app.id,
                    attempt_id,
                    attempt.startTime.isoformat(" "),
                    attempt.endTime.isoformat(" "),
                    attempt.sparkUser,
                    env.get_yarn_queue(),
                    attempt.completed,
                    attempt.durationParsed,
                    "",
                    "",
                )
            )
    return result


async def main(_args: "Sequence[str] | None" = None):
    args = parse_args(_args)
    setup_logging(("cdp_metric_collector",), debug=args.verbose)
    logger.debug("got args %s", args)
    config.load_all()
    sem = Semaphore(4)

    async def processor(app: SparkApplication):
        async with sem:
            return await process_app(spark, hdfs, args.fast_mode, args.with_sql, app)

    with TextIOWrapper(
        open(args.output or sys.stdout.fileno(), "wb", 0),
        newline="",
        encoding="utf-8",
        write_through=True,
    ) as f:
        fw = csv.writer(f)
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
        hdfs = HDFSClient(";".join(config.HDFS_NAMENODE_HOST))
        for host in config.SPARK_HISTORY_HOST:
            async with SparkHistoryClient(host) as spark:
                apps = await spark.applications(
                    args.status,
                    args.min_date,
                    args.max_date,
                    limit=args.limit,
                )
                logger.debug("fetched %s applications", len(apps))
                for rows in as_completed([processor(x) for x in apps]):
                    fw.writerows(await rows)


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
        dest="output",
    )
    parser.add_argument(
        "--fast",
        action="store_true",
        help="enable fast mode (currently disable collection for field: Queue)",
        dest="fast_mode",
    )
    parser.add_argument(
        "--no-sql",
        action="store_false",
        help="don't try to get query plan for job",
        dest="with_sql",
    )
    param = parser.add_argument_group("query params")
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
        "--running",
        action="store_const",
        help="only export running apps",
        const=AppStatus.RUNNING,
        dest="status",
    )
    param.add_argument(
        "--completed",
        action="store_const",
        help="only export completed apps",
        const=AppStatus.COMPLETED,
        dest="status",
    )
    return parser.parse_args(args, Arguments())
