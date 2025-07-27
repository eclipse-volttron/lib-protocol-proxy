import atexit
import logging
import os

from abc import ABC
from gevent.subprocess import Popen, PIPE
from uuid import uuid4
from typing import Type

from ..ipc import callback, ProtocolHeaders, ProtocolProxyPeer
from ..ipc.gevent import GeventIPCConnector
from ..proxy import ProtocolProxy
from . import ProtocolProxyManager

_log = logging.getLogger(__name__)


class GeventProtocolProxyManager(ProtocolProxyManager, GeventIPCConnector, ABC):
    def __init__(self, proxy_class: Type[ProtocolProxy], **kwargs):
        super().__init__(proxy_class=proxy_class, **kwargs)

    def wait_peer_ready(self, peer, timeout, func):
        # TODO:
        #  - if func, check periodically if a new peer has finished registration.
        #       - run func once the peer is ready.
        #  - remove the peer (and logs a warning) if it times out without registering.
        pass

    def get_proxy(self, unique_remote_id: tuple, **kwargs) -> ProtocolProxyPeer:
        command, proxy_id, proxy_name = self._setup_proxy_process_command(unique_remote_id, **kwargs) # , proxy_env
        if command:
            # TODO: proxy_env parameter was added with block to discuss in super._setup_proxy_process_command(). Remove if that is.
            proxy_process = Popen(command, stdin=PIPE) #, env=proxy_env)
            # , stdout=PIPE, stderr=PIPE)
            # TODO: Implement logging along lines of AIP.start_agent() (uncomment PIPES above too).
            _log.info(f"PPM: Created new ProtocolProxy {proxy_name} with ID {str(proxy_id)}, pid: {proxy_process.pid}")
            new_peer_token = uuid4()
            proxy_process.stdin.write(new_peer_token.hex.encode())
            proxy_process.stdin.write(self.token.hex.encode())
            proxy_process.stdin.flush()
            proxy_process.stdin.close()
            proxy_process.stdin = open(os.devnull)
            self.peers[proxy_id] = ProtocolProxyPeer(process=proxy_process, proxy_id=proxy_id, token=new_peer_token)
            atexit.register(self.cleanup_proxy_process, proxy_process)
            # Do NOT send to the proxy until it has registered and socket_params is set!
            _log.debug(f"PPM: Proxy {proxy_id} created, waiting for registration before sending.")
        return self.peers[proxy_id]

    @callback
    def handle_peer_registration(self, headers: ProtocolHeaders, raw_message: bytes):
        return super().handle_peer_registration(headers, raw_message)

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

