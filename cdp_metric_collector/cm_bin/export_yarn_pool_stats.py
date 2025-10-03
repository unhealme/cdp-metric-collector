__version__ = "r2025.10.01-4"


import csv
import logging
import sqlite3
import sys
from argparse import (
    Action,
    ArgumentDefaultsHelpFormatter,
    ArgumentParser,
    BooleanOptionalAction,
    Namespace,
)
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path

from cdp_metric_collector.cm_lib import config
from cdp_metric_collector.cm_lib.cm import (
    CMAPIClient,
    CMAuth,
    MetricContentType,
    MetricRollupType,
    TimeData,
    YQMCLient,
)
from cdp_metric_collector.cm_lib.utils import (
    ARGSWithAuthBase,
    parse_auth,
    setup_logging,
)

TYPE_CHECKING = False
if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Sequence
    from typing import Any


logger = logging.getLogger(__name__)
prog: str | None = None


class Arguments(ARGSWithAuthBase):
    _tbl: str
    parser: ArgumentParser
    verbose: bool

    as_csv: bool
    meth: "Callable[[CMAuth], Awaitable[tuple[TimeData, CMAuth]]]"
    metrics_file: Path | None
    output: Path | None
    yqm_file: Path | None


class MethodParseAction(Action):
    def __call__(
        self,
        parser: ArgumentParser,
        namespace: Namespace,
        values: "str | Sequence[Any] | None",
        option_string: str | None = None,
    ):
        match values:
            case "HOURLY":
                meth = fetch_metrics_hourly
                tbl = "pool_hourly"
            case "10MIN":
                meth = fetch_metrics_10min
                tbl = "pool_10min"
            case _:
                err = f"invalid meta type: {values}"
                raise ValueError(err)
        setattr(namespace, self.dest, meth)
        namespace._tbl = tbl


async def fetch_metrics_hourly(auth: CMAuth):
    async with CMAPIClient(config.CM_HOST, auth) as c:
        data = await c.timedata(
            "select allocated_vcores,allocated_memory_mb,apps_pending,apps_running",
            from_dt=(datetime.now() - timedelta(days=1)).replace(
                hour=0, minute=0, second=0, microsecond=0
            ),
            content_type=MetricContentType.JSON,
            rollup=MetricRollupType.HOURLY,
            force_rollup=True,
        )
        return data, c.auth


async def fetch_metrics_10min(auth: CMAuth):
    async with CMAPIClient(config.CM_HOST, auth) as c:
        data = await c.timedata(
            "select allocated_vcores,allocated_memory_mb,apps_pending,apps_running",
            from_dt=(datetime.now() - timedelta(days=1)).replace(
                hour=0, minute=0, second=0, microsecond=0
            ),
            content_type=MetricContentType.JSON,
            rollup=MetricRollupType.TEN_MINUTELY,
            force_rollup=True,
        )
        return data, c.auth


async def fetch_queues(auth: CMAuth):
    """{pool_name: (core, mem, max_apps)}"""
    async with YQMCLient(config.CM_HOST, auth) as c:
        queues = await c.get_config()
    return {
        x.queuePath: (
            int(x.effectiveMaxResource.vcores, 10),
            int(x.effectiveMaxResource.memory, 10),
            int(x.properties.maxApplications or "0", 10),
        )
        for x in queues.queues
        if x.queuePath.count(".") >= 2
    }


def fetch_queues_from_file(fp: Path | str):
    """{pool_name: (core, mem, max_apps)}"""
    data: dict[str, tuple[int, int, int]] = {}
    with open(fp, "r", newline="", encoding="utf-8") as f:
        fr = csv.reader(f, delimiter="|")
        next(fr)
        for row in fr:
            if row[3]:
                vcore, mem = (x.partition(" ")[0] for x in row[7].split(", "))
                data[row[0]] = (int(vcore, 10), int(mem, 10), int(row[13] or "0", 10))
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
        cursor.execute("""CREATE TABLE IF NOT EXISTS pool_10min (
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
        try:
            yield cursor
        finally:
            cursor.close()
            conn.commit()


async def main(_args: "Sequence[str] | None" = None):
    args = parse_args(_args)
    setup_logging(("cdp_metric_collector",), debug=args.verbose)
    logger.debug("got args %s", args)

    config.load_all()
    auth = args.get_auth()
    match auth, args.metrics_file, args.yqm_file:
        case CMAuth(), None, None:
            cm_metric, auth = await args.meth(auth)
            cm_queues = await fetch_queues(auth)
        case CMAuth(), Path() as metrics_file, None:
            cm_metric = TimeData.decode_json(metrics_file.read_bytes())
            cm_queues = await fetch_queues(auth)
        case CMAuth(), None, Path() as yqm_file:
            cm_metric, auth = await args.meth(auth)
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
                    fw.writerow(data.to_row())
    else:
        if not args.output:
            args.parser.error("No output file is specified")
        with open_db(args.output) as cursor:
            cursor.executemany(
                f"insert or ignore into {args._tbl} values "
                "(?, ?, ?, ?, ?, ?, ? ,? ,? ,?, ?, ?)",
                (
                    data
                    for data in cm_metric.join(cm_queues)
                    if data.pool.count(".") >= 2
                ),
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
    parser.add_argument(
        "-m",
        action=MethodParseAction,
        help="mode to use",
        choices=("HOURLY", "10MIN"),
        type=lambda s: s.strip().upper(),
        default="HOURLY",
        dest="meth",
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
        action=BooleanOptionalAction,
        default=False,
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
