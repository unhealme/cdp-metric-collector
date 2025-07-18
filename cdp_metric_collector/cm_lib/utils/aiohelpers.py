import asyncio
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import TYPE_CHECKING, ParamSpec, TypeVar

if TYPE_CHECKING:
    from collections.abc import Callable

_P = ParamSpec("_P")
_T = TypeVar("_T")


loop: asyncio.AbstractEventLoop


async def wrap_async(
    func: "Callable[_P, _T]",
    /,
    *args: _P.args,
    **kwargs: _P.kwargs,
):
    if asyncio.iscoroutinefunction(func) or not callable(func):
        err = f"{func} is neither a callable or awaitable"
        raise TypeError(err)
    global loop
    try:
        _ = loop
    except NameError:
        loop = asyncio.get_running_loop()
    with ThreadPoolExecutor(1) as e:
        return await loop.run_in_executor(e, partial(func, *args, **kwargs))
