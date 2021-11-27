import asyncio
import logging
from typing import Optional, Any, Tuple, Dict, Callable
from urllib.parse import urlparse

import aiohttp
from aiohttp import web

from . import handlers
from .connection import Connection
from .. import events
from ..process import Pid


local_uri: Optional[str] = None
server: Optional[Tuple[web.Application, web.AppRunner, web.BaseSite]] = None
connections: Dict[str, Connection] = {}


logger = logging.getLogger(__name__)


async def listen(uri: str):
    global local_uri
    global server

    app = web.Application()
    app.add_routes([
        web.get('/', handlers.accept),
        web.post('/processes', handlers.spawn)])

    app['local_uri'] = uri
    app['connections'] = connections

    runner = web.AppRunner(app)
    await runner.setup()

    parsed = urlparse(uri)

    site: Optional[web.BaseSite] = None
    if parsed.scheme == 'tcp':
        site = web.TCPSite(runner, parsed.hostname, parsed.port)
    elif parsed.scheme == 'unix':
        site = web.UnixSite(runner, parsed.path)

    if site:
        local_uri = uri
        server = (app, runner, site)
        await site.start()


async def connect(remote_uri: str) -> Optional[Connection]:
    if not local_uri or remote_uri == local_uri:
        return None
    if remote_uri in connections:
        return connections[remote_uri]

    logger.debug('Attempting to connect: %s', remote_uri)
    try:
        connection = Connection(local_uri, remote_uri)
        connections[remote_uri] = connection
        await connection.connect()
    except aiohttp.ClientConnectionError:
        return None

    return connection


async def spawn(remote_uri: str, fun: Callable[..., Any], *args: Any,
                **kwargs: Any) -> Pid:
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
    connection = connections[remote_uri]
    try:
        if connection.task:
            await connection.task
        logger.debug("Connection closed: %s", connection.remote_uri)
    except Exception:
        logger.error(
            "Connection failed: %s", connection.remote_uri, exc_info=True)
    except asyncio.CancelledError:
        pass
    await connection.close()
    del connections[connection.remote_uri]


@events.register('message.send')
async def message_send(destination: Pid, data: Any):
    if destination.uri and destination.uri in connections:
        await connections[destination.uri].write(
            'message.send', destination, data)


@events.register('connection.available')
async def connection_available(remote_uri):
    await connect(remote_uri)
