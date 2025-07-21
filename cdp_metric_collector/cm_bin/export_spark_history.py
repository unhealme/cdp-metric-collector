__version__ = "b2025.06.26-0"


import argparse
import asyncio
import csv
import logging
import shlex
import sys
from datetime import datetime, timedelta
from enum import Enum
from io import BytesIO, TextIOWrapper
from pathlib import Path
from typing import ClassVar, Literal, cast

from hdfs.util import HdfsError
from msgspec import Struct, ValidationError, json

from cdp_metric_collector.cm_lib import config
from cdp_metric_collector.cm_lib.errors import HTTPNotOK
from cdp_metric_collector.cm_lib.hdfs import HDFSClientBase
from cdp_metric_collector.cm_lib.kerberos import KerberosClientBase
from cdp_metric_collector.cm_lib.structs import Decodable
from cdp_metric_collector.cm_lib.utils import (
    ARGSBase,
    ConvertibleToString,
    setup_logging,
    wrap_async,
)

TYPE_CHECKING = False
if TYPE_CHECKING:
    from collections.abc import Sequence

    from urllib3 import HTTPResponse

logger = logging.getLogger(__name__)
prog: str | None = None


class AppStatus(Enum):
    COMPLETED = 0
    RUNNING = 1

    def __str__(self) -> str:
        return f"{self.__class__.name}.{self.name}"


class ApplicationAttempt(Struct):
    sparkUser: str
    duration: int
    completed: bool
    startTimeEpoch: int
    endTimeEpoch: int

    @property
    def startTime(self):
        return datetime.fromtimestamp(self.startTimeEpoch / 1000)

    @property
    def endTime(self):
        return datetime.fromtimestamp(self.endTimeEpoch / 1000)

    @property
    def duration_parsed(self):
        return str(timedelta(seconds=self.duration / 1000))


class SparkApplication(Struct):
    id: str
    name: str
    attempts: list[ApplicationAttempt]


class SparkProperties(Struct, array_like=True):
    name: str
    value: str

    def as_bool(self):
        return self.value.lower() == "true"

    def as_int(self):
        return int(self.value, 10)

    def as_float(self):
        return float(self.value)

    def as_params(self):
        return shlex.split(self.value)


class ApplicationEnvironment(Decodable):
    sparkProperties: list[SparkProperties]

    @classmethod
    def new(cls):
        return cls([])

    def get_yarn_queue(self):
        for p in self.sparkProperties:
            if p.name == "spark.yarn.queue":
                return p.value
        return ""


class ApplicationNotFoundError(ValueError):
    pass


class SparkHistoryClient(KerberosClientBase):
    app_dec: ClassVar[json.Decoder[list[SparkApplication]]] = json.Decoder(
        list[SparkApplication]
    )

    async def applications(
        self,
        status: AppStatus | None = None,
        minDate: datetime | None = None,
        maxDate: datetime | None = None,
        minEndDate: datetime | None = None,
        maxEndDate: datetime | None = None,
        limit: int | None = None,
    ):
        params = {
            "status": status,
            "minDate": minDate,
            "maxDate": maxDate,
            "minEndDate": minEndDate,
            "maxEndDate": maxEndDate,
            "limit": limit,
        }
        for k, v in list(params.items()):
            match v:
                case AppStatus():
                    params[k] = v.value
                case datetime():
                    params[k] = "{:%Y-%m-%dT%H:%M:%S}.{:.0f}GMT".format(
                        v, v.microsecond / 1000
                    )
                case None:
                    del params[k]
        async with self._client.stream(
            "GET",
            "api/v1/applications",
            params=params,
        ) as resp:
            body = await resp.aread()
            if resp.status_code >= 400:
                logger.error(
                    "got response code %s with header: %s",
                    resp.status_code,
                    resp.headers,
                )
                raise HTTPNotOK(body.decode())
            return await wrap_async(self.app_dec.decode, body)

    async def environment(self, app_id: str):
        retry = 1
        while True:
            try:
                async with self._client.stream(
                    "GET",
                    f"api/v1/applications/{app_id}/environment",
                    timeout=None,
                ) as resp:
                    body = await resp.aread()
                    if resp.status_code == 404:
                        raise ApplicationNotFoundError(body.decode())
                    elif resp.status_code >= 400:
                        logger.error(
                            "got response code %s with header: %s",
                            resp.status_code,
                            resp.headers,
                        )
                        raise HTTPNotOK(body.decode())
                    return await wrap_async(ApplicationEnvironment.decode_json, body)
            except Exception:
                if retry < 3:
                    logger.info("connection error retries %s", retry)
                    logger.debug("", exc_info=True)
                    retry += 1
                    await asyncio.sleep(5)
                else:
                    logger.exception("maximum retries reached")
                    raise


class SparkListenerSQLExecutionStart(Decodable):
    Event: Literal["org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart"]
    executionId: int
    physicalPlanDescription: str


class HDFSClient(HDFSClientBase):
    async def aread(self, path: str):
        with await wrap_async(self.read, path) as f:
            f = cast("HTTPResponse", f)
            return BytesIO(await wrap_async(f.read))

    async def iter_spark_sql(self, app_id: str):
        try:
            with await self.aread(f"/user/spark/applicationHistory/{app_id}") as buf:
                for i in buf:
                    try:
                        yield await wrap_async(
                            SparkListenerSQLExecutionStart.decode_json, i
                        )
                    except ValidationError:
                        pass
        except HdfsError as e:
            if "not found." not in e.message:
                raise
            logger.warning(e.message)


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
        async for sql in hdfs.iter_spark_sql(app.id):
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
