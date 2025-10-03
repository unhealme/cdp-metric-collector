import logging

from cdp_metric_collector.cm_lib.errors import HTTPNotOK
from cdp_metric_collector.cm_lib.kerberos import KerberosClientBase
from cdp_metric_collector.cm_lib.utils import wrap_async

from .structs import YARNApplicationResponse

logger = logging.getLogger(__name__)


class YARNRMClient(KerberosClientBase):
    rm_hosts: list[str]

    def __init__(self, urls: list[str]):
        super().__init__("")
        self.rm_hosts = urls

    async def get_application(self, appid: str):
        body = b""
        for n, host in enumerate(self.rm_hosts):
            async with self.http.stream(
                "GET", f"{host}/ws/v1/cluster/apps/{appid}"
            ) as resp:
                body = await resp.aread()
                if resp.status_code >= 400:
                    logger.error(
                        "got response code %s with header: %s using host: %s",
                        resp.status_code,
                        resp.headers,
                        host,
                    )
                    continue
                if n > 0:
                    self.rm_hosts.insert(0, self.rm_hosts.pop(n))
                return await wrap_async(YARNApplicationResponse.decode_json, body)
        raise HTTPNotOK(body.decode())
