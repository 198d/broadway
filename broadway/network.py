import asyncio
import logging
import io
import pickle
from asyncio import Task
from dataclasses import dataclass
from pickle import Pickler
from typing import Union, Optional, Any, Tuple, Dict
from urllib.parse import urlparse

import aiohttp
from aiohttp import web, WSMsgType

from . import events
from .process import Pid


uri: Optional[str] = None
server: Optional[Tuple[web.Application, web.AppRunner, web.BaseSite]] = None


@dataclass
class Connection:
    local_uri: str
    remote_uri: str
    socket: Union[web.WebSocketResponse, aiohttp.ClientWebSocketResponse]

    async def write(self, name: str, *args: Any):
        buff = io.BytesIO()
        pickler = Pickler(buff)
        pickler.dispatch_table = {
            Pid: self.pid_reducer
        }
        pickler.dump((name, *args))
        await self.socket.send_bytes(buff.getvalue())

    async def close(self):
        if not self.socket.closed:
            await self.socket.close()

    async def loop(self):
        await events.fire('connection.established', self.remote_uri)
        try:
            while True:
                message = await self.socket.receive()
                logging.debug(
                    "Received message from %s: %s", self.remote_uri, message)
                if message.type in (WSMsgType.CLOSE, WSMsgType.CLOSED):
                    break
                elif message.type == WSMsgType.BINARY:
                    await events.fire(*pickle.loads(message.data))
        finally:
            await events.fire('connection.closed', self.remote_uri)

    def pid_reducer(self, pid: Pid):
        if not pid.uri:
            return (Pid, (self.local_uri, int(pid)))
        elif pid.uri == self.remote_uri:
            return (Pid, (int(pid),))
        else:
            return (Pid, (pid.uri, int(pid)))


connections: Dict[str, Connection] = {}


@dataclass
class ServerConnection(Connection):
    socket: web.WebSocketResponse
    task: Optional[Task] = None


@dataclass
class ClientConnection(Connection):
    socket: aiohttp.ClientWebSocketResponse
    session: aiohttp.ClientSession
    task: Optional[Task] = None

    def __post_init__(self):
        self.task = asyncio.create_task(self.loop())

    async def close(self):
        await super().close()
        if not self.session.closed:
            await self.session.close()


async def listen(local_uri: str):
    global uri
    global server

    app = web.Application()
    app.add_routes([web.get('/', accept)])

    app['uri'] = local_uri

    runner = web.AppRunner(app)
    await runner.setup()

    parsed = urlparse(local_uri)

    site: Optional[web.BaseSite] = None
    if parsed.scheme == 'tcp':
        site = web.TCPSite(runner, parsed.hostname, parsed.port)
    elif parsed.scheme == 'unix':
        site = web.UnixSite(runner, parsed.path)

    if site:
        uri = local_uri
        server = (app, runner, site)
        await site.start()


async def accept(request: web.Request) -> web.WebSocketResponse:
    sock = web.WebSocketResponse()
    await sock.prepare(request)

    remote_uri = request.headers['X-BROADWAY-URI']

    connection = ServerConnection(
        request.app['uri'], remote_uri, sock, asyncio.current_task())
    connections[remote_uri] = connection

    await connection.loop()
    return sock


async def connect(remote_uri: str) -> Optional[Connection]:
    if not uri or remote_uri == uri or remote_uri in connections:
        return None

    parsed = urlparse(remote_uri)

    connector = None
    if parsed.scheme == 'unix':
        connector = aiohttp.UnixConnector(parsed.path)

    session = aiohttp.ClientSession(
        headers={'X-BROADWAY-URI': uri}, connector=connector)

    hostname = parsed.netloc if parsed.scheme == 'tcp' else 'localhost'

    try:
        sock = await session.ws_connect(f'http://{hostname}')
    except aiohttp.ClientConnectionError:
        logging.error("Failed to connect", exc_info=True)
        await session.close()
        return None

    connection = ClientConnection(uri, remote_uri, sock, session)
    connections[remote_uri] = connection

    return connection


@events.register('connection.established')
async def connection_established(remote_uri):
    new_connection = connections[remote_uri]
    for connection in connections.values():
        if connection.remote_uri != remote_uri:
            await connection.write('connection.available', remote_uri)
            await new_connection.write(
                'connection.available', connection.remote_uri)


@events.register('connection.closed')
async def connection_closed(remote_uri):
    connection = connections[remote_uri]
    try:
        if connection.task:
            await connection.task
        logging.debug("Connection closed: %s", connection.remote_uri)
    except Exception:
        logging.error(
            "Connection failed: %s", connection.remote_uri, exc_info=True)
    except asyncio.CancelledError:
        pass
    del connections[connection.remote_uri]


@events.register('message.send')
async def message_send(destination: Pid, data: Any):
    if destination.uri and destination.uri in connections:
        await connections[destination.uri].write(
            'message.send', destination, data)


@events.register('connection.available')
async def connection_available(remote_uri):
    await connect(remote_uri)
