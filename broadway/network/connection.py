import asyncio
import logging
import io
import pickle
from asyncio import Task
from dataclasses import dataclass
from pickle import Pickler
from typing import Union, Optional, Any, Callable

import aiohttp
from aiohttp import web, WSMsgType
from yarl import URL

from .. import events


logger = logging.getLogger(__name__)


@dataclass
class Connection:
    local_uri: URL
    remote_uri: URL
    socket: Optional[
                Union[web.WebSocketResponse,
                      aiohttp.ClientWebSocketResponse]] = None
    session: Optional[aiohttp.ClientSession] = None
    task: Optional[Task] = None

    def __post_init__(self):
        logger.debug("Initializing connection: %s", self.remote_uri)

        if not self.session:
            connector = None
            if '+unix' in self.remote_uri.scheme:
                connector = aiohttp.UnixConnector(self.remote_uri.host)
            self.session = aiohttp.ClientSession(
                headers={'X-BROADWAY-URI': str(self.local_uri)},
                connector=connector)

    async def connect(self):
        if not self.socket:
            try:
                self.socket = await self.session.ws_connect(self.request_url())
            except aiohttp.ClientConnectionError as exc:
                await events.fire('connection.closed', self.remote_uri)
                raise exc

        if not self.task:
            self.task = asyncio.create_task(self.loop())

    async def write(self, name: str, *args: Any):
        if self.socket:
            await self.socket.send_bytes(self.pickle((name, *args)))

    async def close(self):
        if self.socket and not self.socket.closed:
            await self.socket.close()
        if self.session and not self.session.closed:
            await self.session.close()

    async def spawn(self, fun: Callable[..., Any], *args: Any,
                    **kwargs: Any) -> URL:
        response = await self.request(
            'POST', '/processes', data=self.pickle((fun, args, kwargs)))
        return pickle.loads(await response.read())

    async def loop(self):
        await events.fire('connection.established', self.remote_uri)
        try:
            while True:
                message = await self.socket.receive()
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
