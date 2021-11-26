import asyncio
import logging
import pickle
from dataclasses import dataclass
from typing import Dict, Callable

from aiohttp import web

from .connection import Connection
from .. import process, events


def connect_middleware_factory(
        local_uri: str,
        connections: Dict[str, Connection]) -> Callable[
                [web.Request, Callable[[web.Request], web.Response]],
                web.Response]:
        @web.middleware
        async def connect_middleware(
                request: web.Request,
                handler: Callable[[web.Request], web.Response]) -> web.Response:
            remote_uri = request.headers['X-BROADWAY-URI']
            if remote_uri not in connections:
                connections[remote_uri] = Connection(local_uri, remote_uri)
            return await handler(request)
        return connect_middleware


async def accept(request: web.Request) -> web.WebSocketResponse:
    sock = web.WebSocketResponse()
    await sock.prepare(request)

    connections = request.app['connections']
    remote_uri = request.headers['X-BROADWAY-URI']

    logging.debug('Accepting connection: %s', remote_uri)

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
    connection = connections[request.headers['X-BROADWAY-URI']]
    return web.Response(body=connection.pickle(pid), status=201)


async def fire(request: web.Request) -> web.Response:
    name, *args = pickle.loads(await request.read())
    await events.fire(name, *args)
    return web.Response(status=202)
