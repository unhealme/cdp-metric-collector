__all__ = ("abstractmethod",)


import sys as _sys
from abc import ABCMeta as _ABCMeta
from abc import abstractmethod
from itertools import chain as _chain
from typing import TYPE_CHECKING
from typing import Any as _Any
from typing import Protocol as _Protocol

if TYPE_CHECKING:
    from cdp_metric_collector.cm_lib.cm.auth import CMAuth


class ABCMeta(_ABCMeta):
    __repr_fields__: tuple[str, ...]
    __slots__: tuple[str, ...]

    def __new__(
        cls,
        name: str,
        bases: tuple[type, ...],
        namespace: "dict[str, _Any]",
        /,
        **kwds: "_Any",
    ):
        if "__slots__" in namespace:
            err = "__slots__ should not be defined"
            raise TypeError(err)

        if _sys.version_info >= (3, 14):
            from annotationlib import Format

            annotations = {}
            if callable(annotate := namespace.get("__annotate_func__", None)):
                annotations = annotate(Format.VALUE)
        else:
            annotations = namespace.get("__annotations__", {})
        slots = tuple(x for x in annotations if x not in namespace)
        namespace["__slots__"] = slots

        fields: list[str] = [
            f for b in bases for f in getattr(b, "__repr_fields__", ())
        ]
        if fields:
            namespace["__repr_fields__"] = tuple(dict.fromkeys(_chain(fields, slots)))
        else:
            namespace["__repr_fields__"] = slots
        return super().__new__(cls, name, bases, namespace, **kwds)


class ABC(metaclass=ABCMeta):
    pass


class NoAuthAvailableError(Exception):
    pass


class ARGSBase(ABC):
    def __iter_fields__(self):
        for f in sorted(self.__repr_fields__):
            try:
                yield f, getattr(self, f)
            except AttributeError:
                continue

    def __repr__(self):
        attr = ", ".join(["%s=%r" % f for f in self.__iter_fields__()])
        return f"{self.__class__.__name__}({attr})"


class ARGSWithAuthBase(ARGSBase):
    auth_config: "CMAuth | None"
    auth_basic: tuple[str, str] | None
    auth_session: str | None
    auth_header: str | None

    def get_auth(self):
        if all(
            x is None
            for x in (
                self.auth_config,
                self.auth_basic,
                self.auth_session,
                self.auth_header,
            )
        ):
            from cdp_metric_collector.cm_lib.cm.auth import CMAuth
            from cdp_metric_collector.cm_lib.config import CM_AUTH

            if CM_AUTH is None:
                return None
            return CMAuth(CM_AUTH)
        elif self.auth_config:
            return self.auth_config
        else:
            from cdp_metric_collector.cm_lib.cm.auth import CMAuth, Creds

            if self.auth_basic:
                user, passw = self.auth_basic
            else:
                user = passw = ""
            return CMAuth(Creds(self.auth_session, user, passw, self.auth_header))


class ConvertibleToString(_Protocol):
    def __str__(self) -> str: ...
