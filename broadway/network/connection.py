import asyncio
import logging
import io
import pickle
from asyncio import Task
from dataclasses import dataclass
from pickle import Pickler
from typing import Union, Optional, Any, Callable
from urllib.parse import urlparse

import aiohttp
from aiohttp import web, WSMsgType

from .. import events
from ..process import Pid


@dataclass
class Connection:
    local_uri: str
    remote_uri: str
    base_url: Optional[str] = None
    socket: Optional[
                Union[web.WebSocketResponse,
                      aiohttp.ClientWebSocketResponse]] = None
    session: Optional[aiohttp.ClientSession] = None
    task: Optional[Task] = None

    def __post_init__(self):
        logging.debug("Initializing connection: %s", self.remote_uri)
        parsed = urlparse(self.remote_uri)
        hostname = parsed.netloc if parsed.scheme == 'tcp' else 'localhost'

        self.base_url = f'http://{hostname}'

        if not self.session:
            connector = None
            if parsed.scheme == 'unix':
                connector = aiohttp.UnixConnector(parsed.path)

            self.session = aiohttp.ClientSession(
                headers={'X-BROADWAY-URI': self.local_uri},
                connector=connector)

    async def connect(self):
        pass
        # if not self.socket:
        #     try:
        #         self.socket = await self.session.ws_connect(self.base_url)
        #     except aiohttp.ClientConnectionError as exc:
        #         await events.fire('connection.closed', self.remote_uri)
        #         raise exc

        # if not self.task:
        #     self.task = asyncio.create_task(self.loop())

    async def fire(self, name:str, *args: Any):
        if self.session:
            await self.request('POST', '/events', data=self.pickle((name, *args)))

    async def write(self, name: str, *args: Any):
        if self.socket:
            await self.socket.send_bytes(self.pickle((name, *args)))

    async def close(self):
        if self.socket and not self.socket.closed:
            await self.socket.close()
        if self.session and not self.session.closed:
            await self.session.close()

    async def spawn(self, fun: Callable[..., Any], *args: Any,
                    **kwargs: Any) -> Pid:
        response = await self.request(
            'POST', '/processes', data=self.pickle((fun, args, kwargs)))
        return pickle.loads(await response.read())

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

    async def request(self, method, path, *args, **kwargs):
        return await self.session.request(
            method, f'{self.base_url}{path}', *args, **kwargs)

    def pickle(self, data: Any):
        buff = io.BytesIO()
        pickler = Pickler(buff)
        pickler.dispatch_table = {
            Pid: self.pid_reducer
        }
        pickler.dump(data)
        return buff.getvalue()

    def pid_reducer(self, pid: Pid):
        if not pid.uri:
            return (Pid, (self.local_uri, int(pid)))
        elif pid.uri == self.remote_uri:
            return (Pid, (int(pid),))
        else:
            return (Pid, (pid.uri, int(pid)))
