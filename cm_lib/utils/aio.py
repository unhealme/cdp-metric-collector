import asyncio
from functools import partial
from typing import TYPE_CHECKING, ParamSpec, TypeVar

if TYPE_CHECKING:
    from collections.abc import Callable

loop: asyncio.AbstractEventLoop

_P = ParamSpec("_P")
_T = TypeVar("_T")


async def wrap_async(
    func: "Callable[_P, _T]",
    /,
    *args: _P.args,
    **kwargs: _P.kwargs,
) -> _T:
    global loop
    if asyncio.iscoroutinefunction(func) or not callable(func):
        err = f"{func} is neither a callable or awaitable"
        raise TypeError(err)
    try:
        _loop = loop
    except NameError:
        _loop = loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, partial(func, *args, **kwargs))
