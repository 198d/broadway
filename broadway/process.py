import asyncio
import itertools
from asyncio import Task, Queue
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Tuple, Dict, Any, Callable, Optional


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
        self.task = asyncio.create_task(self.fun(*self.args, **self.kwargs))
        context.reset(token)

    async def receive(self) -> Any:
        return await self.mailbox.get()

    async def deliver(self, message: Any) -> None:
        await self.mailbox.put(message)


context: ContextVar[Process] = ContextVar('process_context')


async def receive() -> Any:
    return await context.get().receive()


def self() -> Pid:
    return context.get().pid
