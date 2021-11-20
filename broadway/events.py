import asyncio
import logging
from asyncio import Queue
from collections import defaultdict
from typing import Any, List, Mapping, Optional, Callable


handlers: Mapping[str, List[Callable]] = defaultdict(lambda: [])
queue: Optional[Queue] = None


def register(name: str):
    def _register(fun):
        handlers[name].append(fun)
        return fun
    return _register


async def fire(name: str, *args: Any):
    global queue

    if not queue:
        queue = Queue()

    await queue.put((name, *args))


async def loop():
    global queue

    if not queue:
        queue = Queue()

    while True:
        name, *args = await queue.get()
        if handlers[name]:
            done, _ = await asyncio.wait(
                [handler(*args) for handler in handlers[name]])
            for task in done:
                try:
                    await task
                except Exception:
                    logging.error(
                        "Event handler failed for %s", name, exc_info=True)
