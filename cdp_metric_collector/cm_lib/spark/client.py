import logging
from asyncio.tasks import sleep as asleep
from datetime import datetime
from enum import Enum

from msgspec import json

from cdp_metric_collector.cm_lib.errors import HTTPNotOK
from cdp_metric_collector.cm_lib.kerberos import KerberosClientBase
from cdp_metric_collector.cm_lib.utils import (
    wrap_async,
)

from .errors import ApplicationNotFoundError
from .structs import ApplicationEnvironment, SparkApplication

TYPE_CHECKING = False
if TYPE_CHECKING:
    from typing import ClassVar

logger = logging.getLogger(__name__)


class AppStatus(Enum):
    COMPLETED = 0
    RUNNING = 1

    def __str__(self) -> str:
        return f"{self.__class__.name}.{self.name}"


class SparkHistoryClient(KerberosClientBase):
    app_dec: "ClassVar[json.Decoder[list[SparkApplication]]]" = json.Decoder(
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
        async with self.http.stream(
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
                async with self.http.stream(
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
                    await asleep(5)
                else:
                    logger.exception("maximum retries reached")
                    raise
