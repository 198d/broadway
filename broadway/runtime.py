import asyncio
import logging
from asyncio import Event
from typing import Dict, Any, Callable, Optional

from .process import Process, Pid


interrupt: Optional[Event] = None
processes: Dict[Pid, Process] = {}


def spawn(fun: Callable[..., Any], *args: Any, **kwargs: Any) -> Pid:
    process = Process(fun, args, kwargs)
    processes[process.pid] = process
    if interrupt:
        interrupt.set()
    return process.pid


async def send(pid: Pid, *message: Any):
    try:
        await processes[pid].deliver(message)
    except KeyError:
        logging.debug("%s does not exist", pid)


async def monitor():
    global interrupt

    async def waiter(process, task):
        try:
            await task
        except Exception:
            pass
        return process, task

    interrupt = Event()
    try:
        while True:
            done, _ = await asyncio.wait(
                [waiter(process, process.task)
                    for process in processes.values()] +
                [waiter(interrupt, interrupt.wait())],
                return_when=asyncio.FIRST_COMPLETED)

            for wait_task in done:
                value, task = await wait_task

                if value == interrupt:
                    value.clear()
                    continue

                logging.debug("Process exited: %s", value.pid)

                exc = task.exception()
                if exc:
                    exc_info = (exc.__class__, exc, exc.__traceback__)
                    logging.error(
                        "Process crashed: %s", value.pid, exc_info=exc_info)

                try:
                    del processes[value.pid]
                except KeyError:
                    pass
    except asyncio.CancelledError:
        pass
