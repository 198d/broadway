import asyncio
import itertools
import logging
from asyncio import Task, Queue
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Tuple, Dict, Any, Callable, Optional

from . import events


PID_COUNTER = itertools.count(1)


class Pid(int):
    uri: Optional[str] = None

    def __new__(cls, *args, **kwargs):
        maybe_uri, *_ = args
        if isinstance(maybe_uri, str):
            new_object = super().__new__(cls, *args[1:], **kwargs)
            new_object.uri = maybe_uri
        else:
            new_object = super().__new__(cls, *args, **kwargs)
        return new_object

    def __repr__(self):
        if not self.uri:
            return f'Pid({super().__repr__()})'
        else:
            return f'Pid({self.uri},{super().__repr__()})'

    def __hash__(self):
        return hash((self.uri, int(self)))

    def __aiter__(self):
        return self

    async def __anext__(self):
        return await receive()


@dataclass
class Process:
    fun: Callable[..., Any]
    args: Tuple[Any, ...]
    kwargs: Dict[str, Any]
    pid: Pid = field(default_factory=lambda: Pid(next(PID_COUNTER)))
    mailbox: Queue = field(default_factory=Queue)
    task: Optional[Task] = field(default=None)

    def __post_init__(self):
        token = context.set(self)
        self.task = asyncio.create_task(self.loop())
        context.reset(token)
        processes[self.pid] = self

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


processes: Dict[Pid, Process] = {}
context: ContextVar[Process] = ContextVar('process_context')


async def spawn(fun: Callable[..., Any], *args: Any, **kwargs: Any) -> Pid:
    process = Process(fun, args, kwargs)
    return process.pid


async def send(destination: Pid, data: Any):
    await events.fire('message.send', destination, data)


async def receive() -> Any:
    return await context.get().receive()


def self() -> Pid:
    return context.get().pid


@events.register('process.exited')
async def process_exited(process: Process):
    try:
        if process.task:
            await process.task
        logging.debug("Process exited: %s", process.pid)
    except Exception:
        logging.error("Process crashed: %s", process.pid, exc_info=True)
    del processes[process.pid]


@events.register('message.send')
async def message_send(pid: Pid, message: Any):
    if pid in processes:
        await processes[pid].deliver(message)
