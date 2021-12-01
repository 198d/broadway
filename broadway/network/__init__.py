import asyncio
import logging
import os
from pathlib import Path
from typing import Optional, Any, Tuple, Dict, Callable

import aiohttp
from aiohttp import web
from yarl import URL

from . import handlers
from .connection import Connection
from .. import events


BROADWAY_SOCK_DIR = Path(os.getenv('BROADWAY_SOCK_DIR', os.getcwd()))


local_uri: Optional[URL] = None
server: Optional[Tuple[web.Application, web.AppRunner, web.BaseSite]] = None
connections: Dict[URL, Connection] = {}


logger = logging.getLogger(__name__)


async def listen(uri: str):
    global local_uri
    global server

    parsed = URL(uri)

    app = web.Application()
    app.add_routes([
        web.get('/', handlers.accept),
        web.post('/processes', handlers.spawn)])

    app['local_uri'] = parsed
    app['connections'] = connections

    runner = web.AppRunner(app)
    await runner.setup()

    site: Optional[web.BaseSite] = None
    if parsed.host:
        if '+unix' in parsed.scheme:
            site = web.UnixSite(runner, str(BROADWAY_SOCK_DIR / parsed.host))
        else:
            site = web.TCPSite(runner, parsed.host, parsed.port)

    if site:
        local_uri = parsed
        server = (app, runner, site)
        await site.start()


async def connect(remote_uri: str) -> Optional[Connection]:
    parsed = URL(remote_uri)

    if not local_uri or remote_uri == local_uri:
        return None
    if parsed in connections:
        return connections[parsed]

    logger.debug('Attempting to connect: %s', parsed)
    try:
        connection = Connection(local_uri, parsed)
        connections[parsed] = connection
        await connection.connect()
    except aiohttp.ClientConnectionError:
        return None

    return connection


async def spawn(remote_uri: str, fun: Callable[..., Any], *args: Any,
                **kwargs: Any) -> URL:
    connection = await connect(remote_uri)
    if connection:
        return await connection.spawn(fun, *args, **kwargs)
    raise ConnectionError(f'Could not connect: {remote_uri}')


@events.register('connection.established')
async def connection_established(remote_uri):
    for connected_uri in connections:
        if connected_uri != remote_uri:
            await connections[remote_uri].write(
                'connection.available', connected_uri)


@events.register('connection.closed')
async def connection_closed(remote_uri):
    try:
        connection = connections[remote_uri]
        if connection.tasks:
            for task in connection.tasks:
                task.cancel()
                await task
        logger.debug("Connection closed: %s", connection.remote_uri)
    except KeyError:
        return
    except asyncio.CancelledError:
        pass
    except Exception:
        logger.error(
            "Connection failed: %s", connection.remote_uri, exc_info=True)
    await connection.close()
    del connections[connection.remote_uri]


@events.register('message.send')
async def message_send(destination: URL, data: Any):
    connection_uri = destination.with_path('')
    if connection_uri.host:
        if connection_uri not in connections:
            await connect(connection_uri)
        await connections[connection_uri].write(
            'message.send', destination, data)


@events.register('connection.available')
async def connection_available(remote_uri):
    await connect(remote_uri)
