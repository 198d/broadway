from typing import Any, Callable

from . import events
from .process import Process, Pid


def spawn(fun: Callable[..., Any], *args: Any, **kwargs: Any) -> Pid:
    process = Process(fun, args, kwargs)
    return process.pid


async def send(destination: Pid, data: Any):
    await events.fire('message.send', destination, data)


__all__ = (
    "spawn",
    "send",
)
