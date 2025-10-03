from httpx import AsyncClient
from httpx_gssapi import HTTPSPNEGOAuth

from cdp_metric_collector.cm_lib.utils import ABC, abstractmethod

TYPE_CHECKING = False
if TYPE_CHECKING:
    from typing import Any


class KerberosClientABC(ABC):
    http: AsyncClient

    @abstractmethod
    def __init__(self) -> None: ...

    async def __aenter__(self):
        self.http = await self.http.__aenter__()
        await self.initialize()
        return self

    async def __aexit__(self, *exc: "Any"):
        await self.http.__aexit__(*exc)

    async def initialize(self) -> None: ...


class KerberosClientBase(KerberosClientABC):
    base_url: str

    def __init__(self, base_url: str, **kwargs: "Any") -> None:
        self.http = AsyncClient(
            auth=HTTPSPNEGOAuth(delegate=True),
            base_url=base_url,
            verify=False,
            follow_redirects=True,
            **kwargs,
        )
        self.base_url = base_url
