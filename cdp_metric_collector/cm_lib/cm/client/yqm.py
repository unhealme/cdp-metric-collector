import logging
from enum import Enum
from typing import Literal, overload

from msgspec import Struct

from cdp_metric_collector.cm_lib import config
from cdp_metric_collector.cm_lib.cm.api import CMAPIClientBase
from cdp_metric_collector.cm_lib.cm.structs import (
    YarnQMResponse,
    YQMConfigPayload,
    YQMConfigProp,
)
from cdp_metric_collector.cm_lib.utils import wrap_async

logger = logging.getLogger(__name__)


class YQMOperator(Enum):
    ADD = 0
    REM = 1


class YQMQueueACL(Struct):
    name: str
    op: YQMOperator


class YQMCLient(CMAPIClientBase):
    async def initialize(self):
        await self.get_cookies()
        self.http.headers.update(
            {
                "Accept": "application/json, text/plain, */*",
                "Referer": f"{config.CM_HOST}/cmf/clusters/{config.CM_CLUSTER_NAME}/"
                "queue-manager/",
            }
        )

    @overload
    async def get_config(self, raw: Literal[True]) -> bytes: ...
    @overload
    async def get_config(self, raw: bool = False) -> YarnQMResponse: ...
    async def get_config(self, raw: bool = False):
        async with self.request(
            "GET",
            f"/cmf/clusters/{config.CM_CLUSTER_NAME}/queue-manager-api/api/v1/environments/dev"
            f"/clusters/{config.CM_CLUSTER_NAME}/resources/scheduler/partitions/default/queues",
            ssl=False,
        ) as r:
            data = await r.read()
        if raw:
            return data
        return await wrap_async(YarnQMResponse.decode_json, data)

    async def update_config(
        self,
        pool: str,
        last_state: str,
        users: list[YQMQueueACL],
        groups: list[YQMQueueACL],
    ):
        user, _, group = last_state.partition(" ")
        acls = "%s %s" % (
            ",".join(parse_acl([u for u in user.split(",") if u], users)),
            ",".join(parse_acl([g for g in group.split(",") if g], groups)),
        )
        if acls == last_state:
            logger.info("no changes to update from last state")
            return
        logger.info("setting acls %r to pool %s", acls, pool)
        payload = YQMConfigPayload(
            [
                YQMConfigProp("acl_submit_applications", acls),
                YQMConfigProp("acl_administer_queue", acls),
            ],
            f"Changed properties of {pool} by automation",
        )
        logger.debug("sending payload %s", payload)
        async with self.request(
            "PUT",
            f"/cmf/clusters/{config.CM_CLUSTER_NAME}/queue-manager-api/api/v1/environments/dev"
            f"/clusters/{config.CM_CLUSTER_NAME}/resources/scheduler/partitions/default/queues/{pool}",
            json=payload,
            headers={"Content-Type": "application/json"},
            ssl=False,
        ):
            pass


def parse_acl(last: list[str], acls: list[YQMQueueACL]):
    for acl in acls:
        match acl:
            case YQMQueueACL(op=YQMOperator.ADD, name=name):
                if name not in last:
                    last.append(name)
            case YQMQueueACL(op=YQMOperator.REM, name=name):
                try:
                    last.remove(name)
                except ValueError:
                    pass
    return last
