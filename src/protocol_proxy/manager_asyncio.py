import asyncio
import json
import logging
import os
import sys
from subprocess import Popen, PIPE
from typing import Type
from uuid import uuid4, UUID
from weakref import WeakValueDictionary

from .ipc import callback, ProtocolHeaders, ProtocolProxyPeer, SocketParams
from .ipc.asyncio import AsyncioIPCConnector
from .proxy import ProtocolProxy

logging.basicConfig(filename='protoproxy.log', level=logging.DEBUG,
                    format='%(asctime)s - %(message)s')
_log = logging.getLogger(__name__)

class AsyncioProtocolProxyManager(AsyncioIPCConnector):
    managers = WeakValueDictionary()

    def __init__(self, proxy_class: Type[ProtocolProxy], proxy_name: str = 'Manager'):
        self.unique_ids = {}
        super().__init__(proxy_id=self.get_proxy_id('proxy_manager'), proxy_name=proxy_name, token=uuid4())
        self.proxy_class = proxy_class
        self.register_callback(self.handle_peer_registration, 'REGISTER_PEER', provides_response=True)

    def get_proxy_id(self, unique_remote_id: tuple | str) -> UUID:
        proxy_id = self.unique_ids.get(unique_remote_id)
        if not proxy_id:
            proxy_id = uuid4()
            self.unique_ids[unique_remote_id] = proxy_id
        return proxy_id

    async def get_proxy(self, unique_remote_id: tuple, **kwargs) -> ProtocolProxyPeer:
        _log.debug(f'UAI is: {unique_remote_id}')
        unique_remote_id = self.proxy_class.get_unique_remote_id(unique_remote_id)
        _log.debug(f'UAI is: {unique_remote_id}')
        proxy_id = self.get_proxy_id(unique_remote_id)
        proxy_name = str(unique_remote_id)
        if proxy_id not in self.peers:
            module, func = self.proxy_class.__module__, self.proxy_class.__name__
            protocol_specific_params = [i for pair in [(f"--{k.replace('_', '-')}", v) for k, v in kwargs.items()] for i in pair]
            proxy_env = os.environ.copy()
            src_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..', 'src'))
            proxy_env['PYTHONPATH'] = src_dir + os.pathsep + proxy_env.get('PYTHONPATH', '')
            proxy_process = Popen(
                [sys.executable, '-m', module, '--proxy-id', proxy_id.hex, '--proxy-name', proxy_name,
                 '--manager-id', self.proxy_id.hex, '--manager-address', self.inbound_params.address,
                 '--manager-port', str(self.inbound_params.port), *protocol_specific_params],
                stdin=PIPE,
                env=proxy_env
            )
            _log.info(f"PPM: Created new ProtocolProxy {proxy_name} with ID {str(proxy_id)}, pid: {proxy_process.pid}")
            new_peer_token = uuid4()
            proxy_process.stdin.write(new_peer_token.hex.encode())
            proxy_process.stdin.write(self.token.hex.encode())
            proxy_process.stdin.flush()
            proxy_process.stdin.close()
            proxy_process.stdin = open(os.devnull)
            self.peers[proxy_id] = ProtocolProxyPeer(process=proxy_process, proxy_id=proxy_id, token=new_peer_token)
        return self.peers[proxy_id]

    @callback
    async def handle_peer_registration(self, headers: ProtocolHeaders, raw_message: bytes):
        message = json.loads(raw_message.decode('utf8'))
        proxy: ProtocolProxyPeer = self.peers.get(headers.sender_id)
        if (address := message.get('address')) and (port := message.get('port')):
            proxy.socket_params = SocketParams(address=address, port=port)
            _log.info(f'PPM: Successfully registered peer: {proxy.proxy_id} @ {proxy.socket_params}')
            return json.dumps(True).encode('utf8')
        else:
            return json.dumps(False).encode('utf8')

    @classmethod
    def get_manager(cls, proxy_class: Type[ProtocolProxy]):
        if proxy_class.__name__ in cls.managers:
            manager = cls.managers[proxy_class.__name__]
        else:
            _log.info(f'Creating manager for new proxies of class: ({proxy_class.__name__}).')
            manager = cls(proxy_class)
            cls.managers[proxy_class.__name__] = manager
        return manager

    async def start(self, *_, **__):
        await super().start()
        _log.debug(f'AsyncioProtocolProxyManager started.')

    async def select_loop(self):
        while not getattr(self, '_stop', False):
            await asyncio.sleep(0.1)

__all__ = ['AsyncioProtocolProxyManager']
