import contextlib
import logging
import re
from io import StringIO
from typing import TYPE_CHECKING, Any, Concatenate, ParamSpec, TypeVar, cast

from impala.dbapi import connect

from cdp_metric_collector.cm_lib.utils import ABC

if TYPE_CHECKING:
    from collections.abc import Callable

    from impala.hiveserver2 import HiveServer2Connection, HiveServer2Cursor

logger = logging.getLogger(__name__)


class HiveClientBase(ABC):
    _hivecon: "HiveServer2Connection"
    _hivecur: "HiveServer2Cursor"
    _initialized: bool
    hive_url: str
    url_group: tuple[str, ...]

    def __init__(self, hive_url: str) -> None:
        url_group = re.match(
            r"jdbc:(?:hive2|impala)://(.*?):(\d+)/.*?"
            r"(?<=principal=)(.*?)(?:/(.*?)@.*?)?(?=;|$).*?"
            r"((?<=ssl=).*?(?=;|$))?$",
            hive_url,
            re.I,
        )
        if url_group is None:
            err = "unable to parse hive url"
            raise ValueError(err)
        logger.debug("parsed url group: %s", url_group)
        self._initialized = False
        self.hive_url = hive_url
        self.url_group = url_group.groups()

    def __enter__(self):
        self.connect()
        self._hivecon = self._hivecon.__enter__()
        self._hivecur = self._hivecur.__enter__()
        return self

    def __exit__(self, *exc: Any):
        self._hivecur.__exit__(*exc)
        self._hivecon.__exit__(*exc)
        logger.debug("hive connection closed")
        self._initialized = False

    def connect(self) -> None:
        host, port, krb_name, krb_host, ssl = self.url_group
        port = int(port, 10)
        ssl = ssl.lower() == "true"
        logger.debug(
            "connecting to %r using: "
            "connect(%r, %r, auth_mechanism=%r, kerberos_service_name=%r, krb_host=%r, use_ssl=%r)",
            self.hive_url,
            host,
            port,
            "GSSAPI",
            krb_name,
            krb_host,
            ssl,
        )

        with (
            StringIO() as err,
            contextlib.redirect_stderr(err),
        ):
            try:
                self._hivecon = connect(
                    host,
                    port,
                    auth_mechanism="GSSAPI",
                    kerberos_service_name=krb_name,
                    krb_host=krb_host,
                    use_ssl=ssl,
                )
            except Exception as e:
                raise RuntimeError(err.getvalue()) from e
        self._hivecur = cast(
            "HiveServer2Cursor",
            self._hivecon.cursor(convert_types=False),
        )
        self._initialized = True
        logger.debug("connected to %r", self.hive_url)


_RT = TypeVar("_RT")
_PT = ParamSpec("_PT")
_Self = TypeVar("_Self", bound=HiveClientBase)


def ensure_init(
    func: "Callable[Concatenate[_Self,_PT], _RT]",
) -> "Callable[Concatenate[_Self,_PT], _RT]":
    def deco(self: _Self, *args: _PT.args, **kwargs: _PT.kwargs):
        if not self._initialized:
            err = f"{self} is not initialized"
            raise RuntimeError(err)
        return func(self, *args, **kwargs)

    return deco
