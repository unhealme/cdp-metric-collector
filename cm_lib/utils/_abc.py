__all__ = ("abstractmethod",)

from abc import ABCMeta as _ABCMeta
from abc import abstractmethod
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from cm_lib.cm.auth import CMAuth


class ABCMeta(_ABCMeta):
    __repr_fields__: tuple[str, ...]
    __slots__: tuple[str, ...]

    def __new__(
        mcls,
        name: str,
        bases: tuple["type | ABCMeta", ...],
        namespace: dict[str, Any],
        /,
        **kwargs: Any,
    ):
        if "__slots__" in namespace:
            raise TypeError("__slots__ shout not be defined")

        if "__annotations__" in namespace:
            namespace["__slots__"] = tuple(
                x for x in namespace["__annotations__"] if x not in namespace
            )
        else:
            namespace["__slots__"] = ()

        fields: list[str] = []
        for base in bases:
            try:
                fields.extend(base.__repr_fields__)
            except AttributeError:
                pass
        if fields:
            namespace["__repr_fields__"] = tuple(
                dict.fromkeys((*fields, *namespace["__slots__"]))
            )
        else:
            namespace["__repr_fields__"] = namespace["__slots__"]
        return super().__new__(mcls, name, bases, namespace, **kwargs)


class ABC(metaclass=ABCMeta):
    pass


class NoAuthAvailableError(Exception):
    pass


class ARGSBase(ABC):
    def __repr__(self) -> str:
        def iter_attr():
            for k in sorted(self.__repr_fields__):
                try:
                    yield k, getattr(self, k)
                except AttributeError:
                    continue

        return "%s(%s)" % (
            self.__class__.__name__,
            ", ".join(["%s=%r" % attr for attr in iter_attr()]),
        )


class ARGSWithAuthBase(ARGSBase):
    auth_config: "CMAuth | None"
    auth_basic: tuple[str, str] | None
    auth_session: str | None
    auth_header: str | None

    def get_auth(self) -> "CMAuth | None":
        if all(
            x is None
            for x in (
                self.auth_config,
                self.auth_basic,
                self.auth_session,
                self.auth_header,
            )
        ):
            from cm_lib.config import CM_AUTH

            if CM_AUTH is None:
                return None
            return CM_AUTH
        elif self.auth_config:
            return self.auth_config
        else:
            from cm_lib.cm.auth import CMAuth

            if self.auth_basic:
                user, passw = self.auth_basic
            else:
                user = passw = ""
            return CMAuth(
                self.auth_session,
                user,
                passw,
                self.auth_header,
            )


class ConvertibleToString(Protocol):
    def __str__(self) -> str: ...
