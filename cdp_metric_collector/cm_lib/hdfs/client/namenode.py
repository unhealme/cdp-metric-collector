import logging

from cdp_metric_collector.cm_lib.errors import HTTPNotOK
from cdp_metric_collector.cm_lib.hdfs.structs import DFSHealth
from cdp_metric_collector.cm_lib.kerberos import KerberosClientBase
from cdp_metric_collector.cm_lib.utils import wrap_async

logger = logging.getLogger(__name__)


class NameNodeClient(KerberosClientBase):
    nn_hosts: list[str]

    def __init__(self, urls: list[str]) -> None:
        super().__init__("")
        self.nn_hosts = urls

    async def health_status(self):
        body = b""
        for n, host in enumerate(self.nn_hosts):
            async with self.http.stream(
                "GET", f"{host}/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo"
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
                    self.nn_hosts.insert(0, self.nn_hosts.pop(n))
                return await wrap_async(DFSHealth.decode_json, body)
        raise HTTPNotOK(body.decode())
