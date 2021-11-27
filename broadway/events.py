import asyncio
import logging
from asyncio import Queue
from collections import defaultdict
from typing import Any, List, Mapping, Optional, Callable


logger = logging.getLogger(__name__)


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

    queue_task = asyncio.create_task(queue.get())
    running_handlers = set()

    while True:
        done, running_handlers = await asyncio.wait(
            running_handlers.union({queue_task}),
            return_when=asyncio.FIRST_COMPLETED)
        for done_task in done:
            if done_task == queue_task:
                name, *args = await queue_task
                logger.debug("Handling event %s: %s", name, args)
                if handlers[name]:
                    running_handlers = running_handlers.union({
                        asyncio.create_task(handler(*args))
                        for handler in handlers[name]})
                queue_task = asyncio.create_task(queue.get())
            else:
                try:
                    await done_task
                except Exception:
                    logger.error(
                        "Event handler failed for %s", name, exc_info=True)
