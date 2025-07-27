import asyncio
import logging
import os

from abc import ABC
from uuid import uuid4
from typing import Type

from ..ipc import callback, ProtocolHeaders, ProtocolProxyPeer
from ..ipc.asyncio import AsyncioIPCConnector
from ..proxy import ProtocolProxy
from . import ProtocolProxyManager

_log = logging.getLogger(__name__)


class AsyncioProtocolProxyManager(ProtocolProxyManager, AsyncioIPCConnector, ABC):
    def __init__(self, proxy_class: Type[ProtocolProxy], **kwargs):
        super().__init__(proxy_class=proxy_class, **kwargs)

    async def wait_peer_ready(self, peer, timeout, func):
        def is_ready(peer):
            return peer.socket_params is not None

        # TODO:
        #  - if func, check periodically if a new peer has finished registration.
        #       - run func once the peer is ready.
        #  - remove the peer (and logs a warning) if it times out without registering.
        pass

    async def get_proxy(self, unique_remote_id: tuple, **kwargs) -> ProtocolProxyPeer:
        command, proxy_id, proxy_name = self._setup_proxy_process_command(unique_remote_id, **kwargs)  # , proxy_env
        if command:
            # TODO: proxy_env parameter was added with block to discuss in super._setup_proxy_process_command(). Remove if that is.
            _log.debug(f'ABOUT TO SEND COMMAND: "{command}"')
            proxy_process = await asyncio.create_subprocess_exec(*command, stdin=asyncio.subprocess.PIPE) #, env=proxy_env)
            # , stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            # TODO: Implement logging along lines of AIP.start_agent() (uncomment PIPES above too).
            _log.info(f"PPM: Created new ProtocolProxy {proxy_name} with ID {str(proxy_id)}, pid: {proxy_process.pid}")
            new_peer_token = uuid4()
            proxy_process.stdin.write(new_peer_token.hex.encode())
            proxy_process.stdin.write(self.token.hex.encode())
            _log.debug("PPM: Done writing to stdin, about to drain.")
            await proxy_process.stdin.drain()
            _log.debug("PPM: Done writing to stdin, after drain.")
            proxy_process.stdin.close()
            proxy_process.stdin = open(os.devnull)
            _log.debug("PPM: Stdin closed.")
            self.peers[proxy_id] = ProtocolProxyPeer(process=proxy_process, proxy_id=proxy_id, token=new_peer_token)
            # TODO: Why was atexit call removed from asyncio version of this method? (See gevent version) There is an asyncio version if it is needed.
            _log.debug('PPM: END OF GET_PROXY')
        return self.peers[proxy_id]


    @callback
    async def handle_peer_registration(self, headers: ProtocolHeaders, raw_message: bytes):
        return super().handle_peer_registration(headers, raw_message)

    # TODO: Is this override just for debugging?
    async def start(self, *_, **__):
        await super().start()
        _log.debug(f'AsyncioProtocolProxyManager started.')

    # TODO: This is not a select loop, and should not be called that.
    async def select_loop(self):
        while not getattr(self, '_stop', False):
            await asyncio.sleep(0.1)

__all__ = ['AsyncioProtocolProxyManager']
