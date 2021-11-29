import asyncio
import logging
import pickle

from aiohttp import web
from yarl import URL

from .connection import Connection
from .. import process


logger = logging.getLogger(__name__)


async def accept(request: web.Request) -> web.WebSocketResponse:
    sock = web.WebSocketResponse()
    await sock.prepare(request)

    connections = request.app['connections']
    remote_uri = URL(request.headers['X-BROADWAY-URI'])

    logger.debug('Accepting connection: %s', remote_uri)

    connection = Connection(
        request.app['local_uri'], remote_uri, socket=sock,
        task=asyncio.current_task())
    connections[remote_uri] = connection

    await connection.loop()
    return sock


async def spawn(request: web.Request) -> web.Response:
    connections = request.app['connections']
    fun, args, kwargs = pickle.loads(await request.read())
    pid = await process.spawn(fun, *args, **kwargs)
    connection = connections[URL(request.headers['X-BROADWAY-URI'])]
    return web.Response(body=connection.pickle(pid), status=201)
