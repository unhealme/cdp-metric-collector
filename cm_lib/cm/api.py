import logging

from aiohttp import BasicAuth, ClientSession

from cm_lib.errors import HTTPNotOK
from cm_lib.utils import ABC, abstractmethod, encode_json_str

TYPE_CHECKING = False
if TYPE_CHECKING:
    from typing import Any

    from .auth import CMAuth

logger = logging.getLogger(__name__)


class APIClientBase(ABC):
    _client: ClientSession

    @abstractmethod
    def __init__(self) -> None: ...

    async def __aenter__(self):
        self._client = await self._client.__aenter__()
        await self.initialize()
        return self

    async def __aexit__(self, *_exc: "Any"):
        return await self._client.__aexit__(*_exc)

    async def initialize(self) -> None: ...


class CMAPIClientBase(APIClientBase):
    auth: "CMAuth"
    base_url: str
    _session: str | None

    def __init__(self, base_url: str, auth: "CMAuth", **kwargs: "Any") -> None:
        self._client = ClientSession(
            base_url,
            json_serialize=encode_json_str,
            **kwargs,
        )
        self.auth = auth
        self.base_url = base_url
        self._session = None

    async def initialize(self):
        await self._get_cookies()

    async def _get_cookies(self):
        payload: dict[str, Any] = {"ssl": False}
        if self.auth.session:
            logger.debug("using %r as session authentication", self.auth.session)
            self._client.cookie_jar.update_cookies({"SESSION": self.auth.session})
            return
        elif self.auth.header:
            logger.debug("using %r as token authentication", self.auth.header)
            payload["headers"] = {"Authorization": f"Basic {self.auth.header}"}
        else:
            logger.debug("using user and password authentication")
            payload["auth"] = BasicAuth(
                login=self.auth.username, password=self.auth.password
            )
        async with self._client.get("/api/v1/clusters", **payload) as resp:
            if resp.status >= 400:
                logger.error(
                    "got response code %s with header: %s",
                    resp.status,
                    resp.headers,
                )
                raise HTTPNotOK(await resp.text())
            if session := resp.cookies.get("SESSION"):
                self._session = session.coded_value
            logger.info("got session %r from cookies", self._session)
            self._client.cookie_jar.update_cookies(resp.cookies)
