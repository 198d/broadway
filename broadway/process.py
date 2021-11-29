import asyncio
import itertools
import logging
from asyncio import Task, Queue
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Tuple, Dict, Any, Callable, Optional

from yarl import URL

from . import events


logger = logging.getLogger(__name__)


PID_COUNTER = itertools.count(1)


class Mailbox(Queue):
    def __aiter__(self):
        return self

    async def __anext__(self):
        return await self.get()


@dataclass
class Process:
    fun: Callable[..., Any]
    args: Tuple[Any, ...]
    kwargs: Dict[str, Any]
    uri: URL = field(
        default_factory=lambda: URL(f'brdwy:/processes/{next(PID_COUNTER)}'))
    mailbox: Mailbox = field(default_factory=Mailbox)
    task: Optional[Task] = field(default=None)

    def __post_init__(self):
        token = context.set(self)
        self.task = asyncio.create_task(self.loop())
        context.reset(token)
        processes[self.uri] = self

    async def receive(self) -> Any:
        return await self.mailbox.get()

    async def deliver(self, message: Any) -> None:
        await self.mailbox.put(message)

    async def loop(self):
        await events.fire('process.started', self)
        try:
            await self.fun(*self.args, **self.kwargs)
        finally:
            await events.fire('process.exited', self)


processes: Dict[URL, Process] = {}
context: ContextVar[Process] = ContextVar('process_context')


async def spawn(fun: Callable[..., Any], *args: Any, **kwargs: Any) -> URL:
    process = Process(fun, args, kwargs)
    return process.uri


async def send(destination: URL, data: Any):
    await events.fire('message.send', destination, data)


async def receive() -> Any:
    return await context.get().receive()


def mailbox() -> Mailbox:
    return context.get().mailbox


def self() -> URL:
    return context.get().uri


@events.register('process.exited')
async def process_exited(process: Process):
    try:
        if process.task:
            await process.task
        logger.debug("Process exited: %s", process.uri)
    except Exception:
        logger.error("Process crashed: %s", process.uri, exc_info=True)
    del processes[process.uri]


@events.register('message.send')
async def message_send(uri: URL, message: Any):
    if uri in processes:
        await processes[uri].deliver(message)
