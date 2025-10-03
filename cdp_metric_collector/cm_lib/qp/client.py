import logging

from aiohttp import ClientSession

from cdp_metric_collector.cm_lib import config
from cdp_metric_collector.cm_lib.cm import APIClientBase
from cdp_metric_collector.cm_lib.errors import HTTPNotOK
from cdp_metric_collector.cm_lib.utils import encode_json_str, wrap_async

from .structs import QueryExtendedInfo, QuerySearchResult

logger = logging.getLogger(__name__)


class HUEQPClient(APIClientBase):
    base_url: str

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url
        self.http = ClientSession(
            base_url,
            json_serialize=encode_json_str,
        )

    async def initialize(self):
        self.http.headers.update({"x-do-as": config.HUE_USER})

    async def query_detail(self, query_id: str):
        async with self.http.get(
            "/api/hive/query",
            ssl=False,
            params={
                "queryId": query_id,
                "extended": "true",
            },
        ) as r:
            if r.status >= 400:
                logger.error(
                    "got response code %s with header: %s",
                    r.status,
                    r.headers,
                )
                raise HTTPNotOK(r.status, r.headers, await r.text())
            return await wrap_async(QueryExtendedInfo.decode_json, await r.read())

    async def search_query(
        self,
        starttime: int,
        endtime: int,
        limit: int = 100,
        offset: int = 0,
        text: str = "",
    ):
        async with self.http.post(
            "/api/query/search",
            ssl=False,
            json={
                "search": {
                    "endTime": endtime,
                    "limit": limit,
                    "offset": offset,
                    "facets": [],
                    "text": text,
                    "sortText": "startTime:DESC",
                    "startTime": starttime,
                    "type": "BASIC",
                }
            },
        ) as r:
            if r.status >= 400:
                logger.error(
                    "got response code %s with header: %s",
                    r.status,
                    r.headers,
                )
                raise HTTPNotOK(r.status, r.headers, await r.text())
            return await wrap_async(QuerySearchResult.decode_json, await r.read())
