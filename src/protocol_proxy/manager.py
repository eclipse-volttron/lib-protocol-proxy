import atexit
import json
import logging
import os
import sys

from gevent.subprocess import Popen, PIPE
from typing import Type
from uuid import uuid4, UUID
from weakref import WeakValueDictionary

from .ipc import callback, ProtocolHeaders, ProtocolProxyPeer, SocketParams
from .ipc.gevent import GeventIPCConnector
from .proxy import ProtocolProxy

_log = logging.getLogger(__name__)


class ProtocolProxyManager(GeventIPCConnector):
    managers = WeakValueDictionary()

    def __init__(self, proxy_class: Type[ProtocolProxy], proxy_name: str = 'Manager'):
        self.unique_ids = {}
        super(ProtocolProxyManager, self).__init__(proxy_id=self.get_proxy_id('proxy_manager'), proxy_name=proxy_name,
                                                   token=uuid4())
        self.proxy_class = proxy_class
        self.register_callback(self.handle_peer_registration, 'REGISTER_PEER', provides_response=True)

    def wait_peer_ready(self, peer, timeout, func):
        # TODO:
        #  - if func, check periodically if a new peer has finished registration.
        #       - run func once the peer is ready.
        #  - remove the peer (and logs a warning) if it times out without registering.
        pass

    def get_proxy_id(self, unique_remote_id: tuple | str) -> UUID:
        """Lookup or create a UUID for the proxy server
         given a unique_remote_id and protocol-specific set of parameters."""
        proxy_id = self.unique_ids.get(unique_remote_id)
        if not proxy_id:
            from uuid import uuid4
            proxy_id = uuid4()
            self.unique_ids[unique_remote_id] = proxy_id
        return proxy_id

    def get_proxy(self, unique_remote_id: tuple, **kwargs) -> ProtocolProxyPeer:
        _log.debug(f'UAI is: {unique_remote_id}')
        unique_remote_id = self.proxy_class.get_unique_remote_id(unique_remote_id)
        _log.debug(f'UAI is: {unique_remote_id}')
        proxy_id = self.get_proxy_id(unique_remote_id)
        proxy_name = str(unique_remote_id)
        # _log.debug(f'IN MANAGER, GET_PROXY_ID GETS: {proxy_id} for {unique_remote_id}')
        if proxy_id not in self.peers:
            module, func = self.proxy_class.__module__, self.proxy_class.__name__
            protocol_specific_params = [i for pair in [(f"--{k.replace('_', '-')}", v)
                                                       for k, v in kwargs.items()] for i in pair]
            _log.debug([sys.executable, '-m', module, '--proxy-id', proxy_id.hex, '--proxy-name', proxy_name,
                        '--manager-id', self.proxy_id.hex, '--manager-address', self.inbound_params.address,
                        '--manager-port', str(self.inbound_params.port), *protocol_specific_params])
            # TODO: Discuss with Riley why/whether this block is necessary and/or helpful:
            proxy_process = Popen(
                [sys.executable, '-m', module, '--proxy-id', proxy_id.hex, '--proxy-name', proxy_name,
                 '--manager-id', self.proxy_id.hex, '--manager-address', self.inbound_params.address,
                        '--manager-port', str(self.inbound_params.port), *protocol_specific_params],
                stdin=PIPE
            ) #, stdout=PIPE, stderr=PIPE)
            # TODO: Implement logging along lines of AIP.start_agent() (uncomment PIPES above too).
            # TODO: Remove the following commented lines after verifying that new approach works.:
            #  proxy_process = Popen([sys.executable, '-c', f'from {module} import {func}; {func}({repr(unique_remote_id)})'])
            _log.info(f"PPM: Created new ProtocolProxy {proxy_name} with ID {str(proxy_id)}, pid: {proxy_process.pid}")
            new_peer_token = uuid4()
            proxy_process.stdin.write(new_peer_token.hex.encode())
            proxy_process.stdin.write(self.token.hex.encode())
            proxy_process.stdin.flush()
            proxy_process.stdin.close()
            proxy_process.stdin = open(os.devnull)
            self.peers[proxy_id] = ProtocolProxyPeer(process=proxy_process, proxy_id=proxy_id, token=new_peer_token)
            atexit.register(self.cleanup_proxy_process, proxy_process)
        return self.peers[proxy_id]

    @callback
    def handle_peer_registration(self, headers: ProtocolHeaders, raw_message: bytes):
        # _log.debug('IN HANDLE_PEER_REGISTRATION')
        message = json.loads(raw_message.decode('utf8'))
        proxy: ProtocolProxyPeer = self.peers.get(headers.sender_id)
        if (address := message.get('address')) and (port := message.get('port')):
            proxy.socket_params = SocketParams(address=address, port=port)
            _log.info(f'PPM: Successfully registered peer: {proxy.proxy_id} @ {proxy.socket_params}')
            return json.dumps(True).encode('utf8')
        else:  # TODO: Is there any reasonable situation where this runs and returns false?
            return json.dumps(False).encode('utf8')

    def cleanup_proxy_process(self, process: Popen, timeout: float = 5.0):
        # TODO: Waits may block. Check this and change to a method that yields if needed.
        if process.poll() is None:
            _log.info(f'Terminating {self.proxy_class.__name__} with PID: {process.pid}')
            process.terminate()
        if process.wait(timeout=timeout) is None:
            _log.info(f'Killing {self.proxy_class.__name__} with PID: {process.pid}')
            process.kill()
        if process.wait(timeout=timeout) is None:
            _log.warning(f'Failed to stop {self.proxy_class.__name__} with PID: {process.pid}')

    @classmethod
    def get_manager(cls, proxy_class: Type[ProtocolProxy]):
        if proxy_class.__name__ in cls.managers:
            manager = cls.managers[proxy_class.__name__]
        else:
            _log.info(f'Creating manager for new proxies of class: ({proxy_class.__name__}).')
            manager = cls(proxy_class)
            cls.managers[proxy_class.__name__] = manager
        return manager
