import asyncio
import logging
import io
import pickle
from asyncio import Task, Queue
from dataclasses import dataclass, field
from pickle import Pickler
from typing import Union, Optional, Any, Callable, List

import aiohttp
from aiohttp import web, WSMsgType
from yarl import URL

from .. import events


logger = logging.getLogger(__name__)


@dataclass
class Connection:
    local_uri: URL
    remote_uri: URL
    sockets: List[
                Union[web.WebSocketResponse,
                      aiohttp.ClientWebSocketResponse]] = field(default_factory=list)
    session: Optional[aiohttp.ClientSession] = None
    tasks: List[Task] = field(default_factory=list)
    messages: Queue = field(default_factory=Queue)

    def __post_init__(self):
        logger.debug("Initializing connection: %s", self.remote_uri)

        if not self.session:
            connector = None
            if '+unix' in self.remote_uri.scheme:
                connector = aiohttp.UnixConnector(self.remote_uri.host)
            self.session = aiohttp.ClientSession(
                headers={'X-BROADWAY-URI': str(self.local_uri)},
                connector=connector)

    def accept(self, socket: web.WebSocketResponse):
        self.sockets.append(socket)
        self.tasks.append(asyncio.current_task())

    async def connect(self):
        if not self.sockets:
            try:
                self.sockets.append(await self.session.ws_connect(self.request_url()))
                self.tasks.append(asyncio.create_task(self.loop(self.sockets[-1])))
            except aiohttp.ClientConnectionError as exc:
                await events.fire('connection.closed', self.remote_uri)
                raise exc

    async def write(self, name: str, *args: Any):
        if self.sockets:
            await self.sockets[0].send_bytes(self.pickle((name, *args)))

    async def close(self):
        if self.sockets:
            for socket in self.sockets:
                if not socket.closed:
                    await socket.close()
        if self.session and not self.session.closed:
            await self.session.close()

    async def spawn(self, fun: Callable[..., Any], *args: Any,
                    **kwargs: Any) -> URL:
        response = await self.request(
            'POST', '/processes', data=self.pickle((fun, args, kwargs)))
        return pickle.loads(await response.read())

    async def loop(
            self, socket: Union[web.WebSocketResponse,
                                aiohttp.ClientWebSocketResponse]):
        await events.fire('connection.established', self.remote_uri)
        try:
            while True:
                message = await socket.receive()
                logger.debug(
                    "Received message from %s: %s", self.remote_uri, message)
                if message.type in (WSMsgType.CLOSE, WSMsgType.CLOSED):
                    break
                elif message.type == WSMsgType.BINARY:
                    await events.fire(*pickle.loads(message.data))
        finally:
            await events.fire('connection.closed', self.remote_uri)

    def request_url(self, path: Optional[str] = None) -> URL:
        scheme = 'https' if 'brdwys' in self.remote_uri.scheme else 'http'
        url = self.remote_uri.with_scheme(scheme)
        return url.with_path(path) if path else url

    async def request(self, method, path, *args, **kwargs):
        return await self.session.request(
            method, self.request_url(path), *args, **kwargs)

    def pickle(self, data: Any):
        buff = io.BytesIO()
        pickler = Pickler(buff)
        pickler.dispatch_table = {
            URL: self.url_reducer
        }
        pickler.dump(data)
        return buff.getvalue()

    def url_reducer(self, url: URL):
        if 'brdwy' in url.scheme:
            if not url.host:
                return self.local_uri.with_path(url.path).__reduce__()
            elif url.authority == self.remote_uri.authority:
                return URL(f'brdwy:{url.path}').__reduce__()
        return url.__reduce__()
