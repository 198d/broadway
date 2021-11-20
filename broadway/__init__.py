from typing import Any, Callable

from . import events
from .process import Process, Pid


def spawn(fun: Callable[..., Any], *args: Any, **kwargs: Any) -> Pid:
    process = Process(fun, args, kwargs)
    return process.pid


async def send(pid: Pid, message: Any):
    await events.fire('message.send', pid, message)


__all__ = (
    "spawn",
    "send",
)
