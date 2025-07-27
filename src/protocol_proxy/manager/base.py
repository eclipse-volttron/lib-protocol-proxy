import abc
import json
import logging
import os
import sys

from abc import ABC
from typing import Type
from uuid import uuid4, UUID
from weakref import WeakValueDictionary

from ..ipc import callback, IPCConnector, ProtocolHeaders, ProtocolProxyPeer, SocketParams
from ..proxy import ProtocolProxy

_log = logging.getLogger(__name__)


class ProtocolProxyManager(IPCConnector, ABC):
    managers = WeakValueDictionary()

    def __init__(self, *, proxy_class: Type[ProtocolProxy], proxy_name: str = 'Manager', **kwargs):
        self.unique_ids = {}
        super().__init__(proxy_id=self.get_proxy_id('proxy_manager'), proxy_name=proxy_name, token=uuid4(), **kwargs)
        self.proxy_class = proxy_class
        self.register_callback(self.handle_peer_registration, 'REGISTER_PEER', provides_response=True)

    @abc.abstractmethod
    def wait_peer_ready(self, peer, timeout, func):
        # TODO:
        #  - if func, check periodically if a new peer has finished registration.
        #       - run func once the peer is ready.
        #  - remove the peer (and logs a warning) if it times out without registering.
        pass

    def _setup_proxy_process_command(self, unique_remote_id: tuple, **kwargs) -> tuple:
        _log.debug(f'UAI is: {unique_remote_id}')
        unique_remote_id = self.proxy_class.get_unique_remote_id(unique_remote_id)
        _log.debug(f'UAI is: {unique_remote_id}')
        proxy_id = self.get_proxy_id(unique_remote_id)
        proxy_name = str(unique_remote_id)
        if proxy_id not in self.peers:
            module, func = self.proxy_class.__module__, self.proxy_class.__name__
            protocol_specific_params = [i for pair in [(f"--{k.replace('_', '-')}", v)
                                                       for k, v in kwargs.items()] for i in pair]
            command = [sys.executable, '-m', module, '--proxy-id', proxy_id.hex, '--proxy-name', proxy_name,
                       '--manager-id', self.proxy_id.hex, '--manager-address', self.inbound_params.address,
                       '--manager-port', str(self.inbound_params.port), *protocol_specific_params]

            # # TODO: Discuss with Riley why/whether this block was necessary and/or helpful:
            # # Set PYTHONPATH so the proxy subprocess can import protocol_proxy
            # proxy_env = os.environ.copy()
            # src_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..', 'src'))
            # proxy_env['PYTHONPATH'] = src_dir + os.pathsep + proxy_env.get('PYTHONPATH', '')
            # # TODO: END block to discuss.
        else:
            command = None  #, proxy_env = None, None
        return command, proxy_id, proxy_name # , proxy_env

    @abc.abstractmethod
    def get_proxy(self, unique_remote_id: tuple, **kwargs) -> ProtocolProxyPeer:
        pass

    def get_proxy_id(self, unique_remote_id: tuple | str) -> UUID:
        """Lookup or create a UUID for the proxy server
         given a unique_remote_id and protocol-specific set of parameters."""
        proxy_id = self.unique_ids.get(unique_remote_id)
        if not proxy_id:
            proxy_id = uuid4()
            self.unique_ids[unique_remote_id] = proxy_id
        return proxy_id

    def handle_peer_registration(self, headers: ProtocolHeaders, raw_message: bytes):
        message = json.loads(raw_message.decode('utf8'))
        proxy: ProtocolProxyPeer = self.peers.get(headers.sender_id)
        if (address := message.get('address')) and (port := message.get('port')):
            proxy.socket_params = SocketParams(address=address, port=port)
            _log.info(f'PPM: Successfully registered peer: {proxy.proxy_id} @ {proxy.socket_params}')
            return json.dumps(True).encode('utf8')
        else:  # TODO: Is there any reasonable situation where this runs and returns false?
            return json.dumps(False).encode('utf8')

    @classmethod
    def get_manager(cls, proxy_class: Type[ProtocolProxy]):
        if proxy_class.__name__ in cls.managers:
            manager = cls.managers[proxy_class.__name__]
        else:
            _log.info(f'Creating manager of class "{cls.__name__}" for new proxies of class: ({proxy_class.__name__}).')
            manager = cls(proxy_class)
            cls.managers[proxy_class.__name__] = manager
        return manager