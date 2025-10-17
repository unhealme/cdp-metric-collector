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
        status_code = -1
        headers = {}
        for n, host in enumerate(self.nn_hosts):
            async with self.http.stream(
                "GET", f"{host}/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo"
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
                    self.nn_hosts.insert(0, self.nn_hosts.pop(n))
                return await wrap_async(DFSHealth.decode_json, body)
        raise HTTPNotOK(status_code, headers, body.decode())
