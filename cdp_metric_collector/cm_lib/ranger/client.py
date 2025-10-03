import logging
from datetime import date

from aiohttp import BasicAuth, ClientSession, ClientTimeout

from cdp_metric_collector.cm_lib.cm import APIClientBase
from cdp_metric_collector.cm_lib.errors import HTTPNotOK
from cdp_metric_collector.cm_lib.utils import encode_json_str, wrap_async

from .structs import RangerAccessAudit, RangerPolicyList, RangerUsers

logger = logging.getLogger(__name__)


class RangerClient(APIClientBase):
    base_url: str

    def __init__(self, base_url: str, user: str, passw: str) -> None:
        self.base_url = base_url
        self.http = ClientSession(
            base_url,
            auth=BasicAuth(user, passw),
            timeout=ClientTimeout(total=None),
        )

    async def access_audit(
        self,
        start_date: date | None,
        end_date: date | None,
        service_name: str,
        limit: int = 10000,
        index: int = 0,
    ):
        params = {
            "repoName": service_name,
            "pageSize": limit,
            "startIndex": index,
            "excludeServiceUser": "false",
            "sortBy": "eventTime",
            "sortType": "desc",
        }
        if start_date:
            params["startDate"] = start_date.strftime(r"%m/%d/%Y")
        if end_date:
            params["endDate"] = end_date.strftime(r"%m/%d/%Y")
        logger.debug("sending data %s", encode_json_str(params))
        async with self.http.get(
            "/service/assets/accessAudit",
            ssl=False,
            params=params,
        ) as r:
            if r.status >= 400:
                logger.error(
                    "got response code %s with header: %s",
                    r.status,
                    r.headers,
                )
                raise HTTPNotOK(r.status, r.headers, await r.text())
            return await wrap_async(RangerAccessAudit.decode_json, await r.read())

    async def policies(
        self,
        service_type: str,
        limit: int = 10000,
        index: int = 0,
        **filters: str,
    ):
        params = {
            "policyType": 0,
            "serviceType": service_type,
            "pageSize": limit,
            "startIndex": index,
            **filters,
        }
        logger.debug("sending data %s", encode_json_str(params))
        async with self.http.get(
            "/service/plugins/policies",
            params=params,
            ssl=False,
        ) as r:
            if r.status >= 400:
                logger.error(
                    "got response code %s with header: %s",
                    r.status,
                    r.headers,
                )
                raise HTTPNotOK(r.status, r.headers, await r.text())
            return await wrap_async(RangerPolicyList.decode_json, await r.read())

    async def users(
        self,
        source: int,
        role_list: list[str],
        page: int = 0,
        limit: int = 1000,
        index: int = 0,
    ):
        params = {
            "page": page,
            "pageSize": limit,
            "startIndex": index,
            "userRoleList": role_list,
            "userSource": source,
        }
        logger.debug("sending data %s", encode_json_str(params))
        async with self.http.get(
            "/service/xusers/users",
            params=params,
            ssl=False,
        ) as r:
            if r.status >= 400:
                logger.error(
                    "got response code %s with header: %s",
                    r.status,
                    r.headers,
                )
                raise HTTPNotOK(r.status, r.headers, await r.text())
            return await wrap_async(RangerUsers.decode_json, await r.read())
