__version__ = "b2025.06.26-0"


import csv
import logging
import sqlite3
import sys
from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path

from cm_lib import config
from cm_lib.cm import CMAuth
from cm_lib.structs.timeseries import TimeData
from cm_lib.utils import ARGSWithAuthBase, parse_auth, setup_logging, wrap_async

from .export_yarn_qm import YQMCLient
from .query_cm_metrics import CMMetricsClient as _CMMetricsClient
from .query_cm_metrics import ContentType, RollupType

TYPE_CHECKING = False
if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)


class Arguments(ARGSWithAuthBase):
    parser: ArgumentParser
    verbose: bool
    output: Path | None
    as_csv: bool
    metrics_file: Path | None
    yqm_file: Path | None


class CMMetricsClient(_CMMetricsClient):
    async def timedata(
        self,
        query: str,
        *,
        from_dt: datetime | None = None,
        to_dt: datetime | None = None,
        content_type: ContentType | None = None,
        rollup: RollupType | None = None,
        force_rollup: bool | None = None,
    ):
        return await wrap_async(
            TimeData.decode_json,
            await self.timeseries(
                query,
                from_dt=from_dt,
                to_dt=to_dt,
                content_type=content_type,
                rollup=rollup,
                force_rollup=force_rollup,
            ),
        )


async def fetch_metrics(auth: CMAuth):
    async with CMMetricsClient(config.CM_HOST, auth) as client:
        data = await client.timedata(
            "select allocated_vcores,allocated_memory_mb",
            from_dt=(datetime.now() - timedelta(days=1)).replace(
                hour=0, minute=0, second=0, microsecond=0
            ),
            content_type=ContentType.JSON,
            rollup=RollupType.HOURLY,
            force_rollup=True,
        )
        auth.session = client._session
        return data, auth


async def fetch_queues(auth: CMAuth):
    async with YQMCLient(config.CM_HOST, auth) as client:
        queues = await client.get_data()
    return {
        x.queuePath: (
            int(x.effectiveMaxResource.vcores, 10),
            int(x.effectiveMaxResource.memory, 10),
        )
        for x in queues.queues
    }


def fetch_queues_from_file(fp: Path | str):
    data: dict[str, tuple[int, int]] = {}
    with open(fp, "r", newline="", encoding="utf-8") as f:
        fr = csv.reader(f, delimiter="|")
        next(fr)
        for row in fr:
            vcore, mem = (x.partition(" ")[0] for x in row[7].split(", "))
            data[row[0]] = (int(vcore, 10), int(mem, 10))
    return data


@contextmanager
def open_db(fp: "Path | str"):
    with sqlite3.connect(fp, check_same_thread=False) as conn:
        cursor = conn.executescript(
            "PRAGMA optimize; PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;"
        )
        cursor.execute("""CREATE TABLE IF NOT EXISTS pool_hourly (
        `timestamp` DATETIME NOT NULL,
        pool TEXT NOT NULL,
        metric TEXT NOT NULL,
        value REAL,
        perc_value TEXT,
        min REAL,
        perc_min TEXT,
        at_min DATETIME,
        max REAL,
        perc_max TEXT,
        at_max DATETIME,
        aggregations INTEGER,
        CONSTRAINT PK PRIMARY KEY (`timestamp`,pool,metric))""")
        cursor.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS IDX ON pool_hourly (`timestamp`,pool,metric)"
        )
        try:
            yield cursor
        finally:
            cursor.close()
            conn.commit()


async def main(_args: "Sequence[str] | None" = None):
    args = parse_args(_args)
    setup_logging((logger, "cm_lib"), debug=args.verbose)
    logger.debug("got args %s", args)

    config.load_all()
    auth = args.get_auth()
    match auth, args.metrics_file, args.yqm_file:
        case CMAuth(), None, None:
            cm_metric, auth = await fetch_metrics(auth)
            cm_queues = await fetch_queues(auth)
        case CMAuth(), Path() as metrics_file, None:
            cm_metric = TimeData.decode_json(metrics_file.read_bytes())
            cm_queues = await fetch_queues(auth)
        case CMAuth(), None, Path() as yqm_file:
            cm_metric, auth = await fetch_metrics(auth)
            cm_queues = fetch_queues_from_file(yqm_file)
        case _, Path() as metrics_file, Path() as yqm_file:
            cm_metric = TimeData.decode_json(metrics_file.read_bytes())
            cm_queues = fetch_queues_from_file(yqm_file)
        case _:
            args.parser.error("No auth mechanism is passed")

    if args.as_csv:
        with open(
            args.output or sys.stdout.fileno(),
            "w",
            encoding="utf-8",
            newline="",
        ) as outf:
            fw = csv.writer(outf)
            fw.writerow(
                (
                    "Timestamp",
                    "Pool",
                    "Metric",
                    "Value",
                    "%Value",
                    "Min",
                    "%Min",
                    "@Min",
                    "Max",
                    "%Max",
                    "@Max",
                    "Aggregations",
                )
            )
            for data in cm_metric.join(cm_queues):
                if data.pool.count(".") >= 2:
                    fw.writerow(data)
    else:
        if not args.output:
            args.parser.error("No output file is specified")
        with open_db(args.output) as cursor:
            for data in cm_metric.join(cm_queues):
                if data.pool.count(".") >= 2:
                    cursor.execute(
                        "insert or ignore into pool_hourly (`timestamp`, pool, metric, value,"
                        " perc_value, min, perc_min, at_min, max, perc_max, at_max, "
                        "aggregations) values (?, ?, ?, ?, ?, ?, ? ,? ,? ,?, ?, ?)",
                        data,
                    )


def parse_args(args: "Sequence[str] | None" = None):
    parser = ArgumentParser(
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
    parser.add_argument(
        "-o",
        action="store",
        help="dump result to FILE",
        metavar="FILE",
        type=Path,
        default=None,
        dest="output",
    )
    parser.add_argument(
        "--csv",
        action="store_true",
        help="format result as CSV",
        dest="as_csv",
    )
    parser.add_argument(
        "--metrics-file",
        action="store",
        help="load metrics from FILE",
        metavar="FILE",
        type=Path,
        default=None,
        dest="metrics_file",
    )
    parser.add_argument(
        "--yqm-file",
        action="store",
        help="load yarn queues from FILE",
        metavar="FILE",
        type=Path,
        default=None,
        dest="yqm_file",
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


def __main__():
    import asyncio

    asyncio.run(main())
