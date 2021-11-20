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
    def __new__(cls, *args, **kwargs):
        return super().__new__(cls, *args, **kwargs)

    def __str__(self):
        return f'Pid({super().__str__()})'


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


@events.register('process.started')
async def process_started(process: Process):
    processes[process.pid] = process


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


async def receive() -> Any:
    return await context.get().receive()


def self() -> Pid:
    return context.get().pid