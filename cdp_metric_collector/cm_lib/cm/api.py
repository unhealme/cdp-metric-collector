import logging
from contextlib import asynccontextmanager
from copy import copy

from aiohttp import BasicAuth, ClientSession

from cdp_metric_collector.cm_lib import config
from cdp_metric_collector.cm_lib.errors import HTTPNotOK
from cdp_metric_collector.cm_lib.utils import ABC, abstractmethod, encode_json_str

TYPE_CHECKING = False
if TYPE_CHECKING:
    from types import TracebackType
    from typing import Any, Unpack

    from aiohttp.client import _RequestOptions

    from .auth import CMAuth

logger = logging.getLogger(__name__)


class APIClientBase(ABC):
    http: ClientSession

    @abstractmethod
    def __init__(self) -> None: ...

    async def __aenter__(self):
        self.http = await self.http.__aenter__()
        await self.initialize()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: "TracebackType | None",
    ):
        await self.http.__aexit__(exc_type, exc_val, exc_tb)

    async def initialize(self) -> None: ...


class CMAPIClientBase(APIClientBase):
    auth: "CMAuth"
    base_url: str | None
    session_id: str | None

    def __init__(self, base_url: str | None, auth: "CMAuth", **kwargs: "Any"):
        self.http = ClientSession(
            base_url,
            json_serialize=encode_json_str,
            **kwargs,
        )
        self.auth = copy(auth)
        self.base_url = base_url
        self.session_id = None

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: "TracebackType | None",
    ):
        await super().__aexit__(exc_type, exc_val, exc_tb)
        if not (isinstance(exc_val, HTTPNotOK) and exc_val.status == 401):
            config.save_cm_auth(self.auth)

    async def initialize(self):
        await self.get_cookies()

    async def get_cookies(self):
        payload: dict[str, Any] = {"ssl": False}
        if session := self.auth.creds.session:
            logger.debug("using %r as session authentication", session)
            self.http.cookie_jar.update_cookies({"SESSION": session})
            logger.debug("checking session vailidity")
            async with self.request("GET", "/api/v1/clusters"):
                pass
            return
        elif header := self.auth.creds.header:
            logger.debug("using %r as token authentication", header)
            payload["headers"] = {"Authorization": f"Basic {header}"}
        else:
            logger.debug("using user and password authentication")
            payload["auth"] = BasicAuth(
                login=self.auth.creds.username, password=self.auth.creds.password
            )
        async with self.http.get("/api/v1/clusters", **payload) as r:
            if r.status >= 400:
                logger.error(
                    "got response code %s with header: %s",
                    r.status,
                    r.headers,
                )
                raise HTTPNotOK(r.status, r.headers, await r.text())
            if session := r.cookies.get("SESSION"):
                self.session_id = session.coded_value
            logger.debug("got session id %r from cookies", self.session_id)
            self.http.cookie_jar.update_cookies(r.cookies)
            self.auth.creds.session = self.session_id

    @asynccontextmanager
    async def request(self, method: str, url: str, **kwargs: "Unpack[_RequestOptions]"):
        retry = False
        async with self.http.request(method, url, **kwargs) as r:
            if r.status == 401:
                self.auth.creds.session = None
                self.http.cookie_jar.clear()
                retry = True
            elif r.status >= 400:
                logger.error(
                    "got response code %s with header: %s",
                    r.status,
                    r.headers,
                )
                raise HTTPNotOK(r.status, r.headers, await r.text())
            if not retry:
                yield r
                return
        await self.get_cookies()
        async with self.http.request(method, url, **kwargs) as r:
            if r.status >= 400:
                logger.error(
                    "got response code %s with header: %s",
                    r.status,
                    r.headers,
                )
                raise HTTPNotOK(r.status, r.headers, await r.text())
            yield r
