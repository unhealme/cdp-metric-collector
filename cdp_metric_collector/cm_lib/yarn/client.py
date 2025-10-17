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
        status_code = -1
        headers = {}
        for n, host in enumerate(self.rm_hosts):
            async with self.http.stream(
                "GET", f"{host}/ws/v1/cluster/apps/{appid}"
            ) as r:
                body = await r.aread()
                if r.status_code >= 400:
                    logger.error(
                        "got response code %s with header: %s using host: %s",
                        r.status_code,
                        r.headers,
                        host,
                    )
                    status_code = r.status_code
                    headers = r.headers
                    continue
                if n > 0:
                    self.rm_hosts.insert(0, self.rm_hosts.pop(n))
                return await wrap_async(YARNApplicationResponse.decode_json, body)
        raise HTTPNotOK(status_code, headers, body.decode())
