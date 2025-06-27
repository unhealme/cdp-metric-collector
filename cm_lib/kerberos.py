from httpx import AsyncClient
from httpx_gssapi import HTTPSPNEGOAuth

from cm_lib.utils import ABC, abstractmethod

TYPE_CHECKING = False
if TYPE_CHECKING:
    from typing import Any


class KerberosClientABC(ABC):
    _client: AsyncClient

    @abstractmethod
    def __init__(self) -> None: ...

    async def __aenter__(self):
        self._client = await self._client.__aenter__()
        await self.initialize()
        return self

    async def __aexit__(self, *_exc: "Any"):
        return await self._client.__aexit__(*_exc)

    async def initialize(self) -> None: ...


class KerberosClientBase(KerberosClientABC):
    base_url: str

    def __init__(self, base_url: str, **kwargs: "Any") -> None:
        self._client = AsyncClient(
            auth=HTTPSPNEGOAuth(delegate=True),
            base_url=base_url,
            verify=False,
            follow_redirects=True,
            **kwargs,
        )
        self.base_url = base_url
