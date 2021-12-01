import asyncio
import logging
import pickle

from aiohttp import web
from yarl import URL

from .connection import Connection
from .. import process


logger = logging.getLogger(__name__)


async def accept(request: web.Request) -> web.WebSocketResponse:
    connections = request.app['connections']
    remote_uri = URL(request.headers['X-BROADWAY-URI'])

    logger.debug('Accepting connection: %s', remote_uri)

    if remote_uri in connections:
        connection = connections[remote_uri]
    else:
        connection = Connection(request.app['local_uri'], remote_uri)
        connections[remote_uri] = connection

    sock = web.WebSocketResponse()
    await sock.prepare(request)

    connection.accept(sock)

    await connection.loop(sock)
    return sock


async def spawn(request: web.Request) -> web.Response:
    connections = request.app['connections']
    fun, args, kwargs = pickle.loads(await request.read())
    pid = await process.spawn(fun, *args, **kwargs)
    connection = connections[URL(request.headers['X-BROADWAY-URI'])]
    return web.Response(body=connection.pickle(pid), status=201)
