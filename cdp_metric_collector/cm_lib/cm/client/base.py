import csv
import logging
from asyncio.tasks import sleep as asleep
from datetime import datetime
from enum import Enum
from io import StringIO

from aiohttp import ClientError
from msgspec import convert

from cdp_metric_collector.cm_lib import config
from cdp_metric_collector.cm_lib.cm.api import CMAPIClientBase
from cdp_metric_collector.cm_lib.cm.structs import (
    APICommand,
    AuthRoles,
    Commands,
    FileBrowserPath,
    HealthIssues,
    Hosts,
    TimeData,
    TimeSeriesPayload,
)
from cdp_metric_collector.cm_lib.utils import encode_json_str, wrap_async

TYPE_CHECKING = False
if TYPE_CHECKING:
    from typing import Any

logger = logging.getLogger(__name__)


class MetricContentType(Enum):
    JSON = "application/json"
    CSV = "text/csv"


class MetricRollupType(Enum):
    RAW = "RAW"
    TEN_MINUTELY = "TEN_MINUTELY"
    HOURLY = "HOURLY"
    SIX_HOURLY = "SIX_HOURLY"
    DAILY = "DAILY"
    WEEKLY = "WEEKLY"

    @classmethod
    def _missing_(cls, value: "str | Any"):
        for member in cls:
            if member.name.lower() == value.lower():
                return member
        return None

    def __str__(self) -> str:
        return self.name


class CMAPIClient(CMAPIClientBase):
    async def command(self, id: int):
        async with self.request(
            "GET",
            f"/api/v{config.CM_API_VER}/commands/{id}",
            ssl=False,
        ) as r:
            return APICommand.decode_json(await r.read())

    async def file_browser(self, path: str):
        rt = 1
        while True:
            try:
                async with self.request(
                    "GET",
                    config.FILE_BROWSER_PATH,
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
                ) as r:
                    data = StringIO(await r.text(), newline="")
                with data:
                    for row in csv.DictReader(data, restkey="_"):
                        yield convert(row, FileBrowserPath)
                break
            except ClientError:
                logger.warning("connection error retries %s", rt, exc_info=True)
                rt += 1
                await asleep(5)

    async def health_issues(self):
        async with self.request(
            "GET",
            "/cmf/healthIssues.json",
            ssl=False,
        ) as r:
            return await wrap_async(HealthIssues.decode_json, await r.read())

    async def hosts(self):
        async with self.request(
            "GET",
            f"/api/v{config.CM_API_VER}/hosts",
            params="view=FULL",
            ssl=False,
        ) as r:
            return await wrap_async(Hosts.decode_json, await r.read())

    async def rebalance_start(self):
        async with self.request(
            "POST",
            f"{config.HDFS_REBALANCE_PATH}/do",
            data="confirm=on&command=Rebalance",
            headers={
                "Referer": f"{config.CM_HOST}/",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            ssl=False,
        ):
            pass
        async with self.request(
            "GET",
            f"/api/v{config.CM_API_VER}/clusters/{config.CM_CLUSTER_NAME}/"
            f"services/hdfs/roles/{config.HDFS_REBALANCE_ROLE}/commands",
            ssl=False,
        ) as r:
            cmd = Commands.decode_json(await r.read())
        return next(i for i in cmd.items if i.name == "Rebalance")

    async def rebalance_stop(self, id: int):
        async with self.request(
            "POST",
            f"/api/v{config.CM_API_VER}/commands/{id}/abort",
            ssl=False,
        ) as r:
            return APICommand.decode_json(await r.read())

    async def roles(self):
        async with self.request(
            "GET",
            f"/api/v{config.CM_API_VER}/authRoles",
            params="view=FULL",
            ssl=False,
        ) as r:
            return await wrap_async(AuthRoles.decode_json, await r.read())

    async def timeseries(
        self,
        query: str,
        *,
        from_dt: datetime | None = None,
        to_dt: datetime | None = None,
        content_type: MetricContentType | None = None,
        rollup: MetricRollupType | None = None,
        force_rollup: bool | None = None,
    ):
        data = TimeSeriesPayload(query)
        if from_dt:
            data.from_dt = from_dt.isoformat()
        if to_dt:
            data.to_dt = to_dt.isoformat()
        if content_type:
            data.contentType = content_type.value
        if rollup:
            data.desiredRollup = rollup.value
        if force_rollup is not None:
            data.mustUseDesiredRollup = force_rollup
        logger.debug("sending payload %s", data)
        async with self.request(
            "POST",
            f"/api/v{config.CM_API_VER}/timeseries",
            json=data,
            ssl=False,
        ) as r:
            return await r.read()

    async def timedata(
        self,
        query: str,
        *,
        from_dt: datetime | None = None,
        to_dt: datetime | None = None,
        content_type: MetricContentType | None = None,
        rollup: MetricRollupType | None = None,
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
