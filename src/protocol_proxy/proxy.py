import abc
import json
import logging

from gevent import sleep, spawn
from gevent.event import AsyncResult
from uuid import UUID

from .ipc import IPCConnector, ProtocolProxyMessage, ProtocolProxyPeer, SocketParams

logging.basicConfig(filename='protoproxy.log', level=logging.DEBUG,
                    format='%(asctime)s - %(message)s')
_log = logging.getLogger(__name__)


class ProtocolProxy(IPCConnector):
    def __init__(self, manager_address: str, manager_port: int, manager_id: UUID, manager_token: UUID, token: UUID,
                 proxy_id: UUID, proxy_name: str = None, registration_retry_delay: float = 20.0, *args, **kwargs):
        """ Base class for protocols requiring a standalone process to handle incoming and outgoing requests.
        """
        #_log.debug(f'IN PROTOPROXY, BEFORE SUPER, get_proxy_id gets: {self.get_proxy_id(unique_remote_id)}')
        super(ProtocolProxy, self).__init__(proxy_id=proxy_id, token=token, proxy_name=proxy_name)  #self.get_proxy_id(unique_remote_id), token=token)
        # _log.debug(f'{self.proxy_name} IN PROTOPROXY AFTER SUPER')
        self.registration_retry_delay: float = registration_retry_delay
        manager_params = SocketParams(manager_address, manager_port)
        self.manager = manager_id
        self.peers[manager_id] = ProtocolProxyPeer(proxy_id=manager_id, socket_params=manager_params,
                                                   token=manager_token)
        spawn(self.send_registration, manager_params)
        # _log.debug(f'{self.proxy_name} IN PROTOPROXY, END OF INIT')

    def send_registration(self, remote: SocketParams):
        # _log.debug(f'{self.proxy_name}: IN SEND REGISTRATION')
        local_address, local_port = self.inbound_server_socket.getsockname()
        message = ProtocolProxyMessage(
            method_name='REGISTER_PEER',
            payload= json.dumps({'address': local_address, 'port':local_port, 'proxy_id': self.proxy_id.hex,
                                 'token': self.token.hex}).encode('utf8'),
            response_expected=True
        )
        # _log.debug(f'{self.proxy_name} IN SEND REGISTRATION, BEFORE SEND')
        # TODO: Inability to connect while on VPN, but is this related to MQTT or to local sockets.
        manager_response = self.send(remote, message)
        _log.debug(f'{self.proxy_name} IN SEND REGISTRATION, manager_response is a {type(manager_response)} (initially): {manager_response}, and self.response_results is: {list(self.response_results.items())} ')
        # _log.debug(f'{self.proxy_name} IN SEND REGISTRATION, AFTER SEND')
        # while not manager_response.ready():
        #     sleep(0.1)
        # TODO: This should be using a try block to catch the timeout. Is the return even useful?
        success = manager_response.get(timeout=5) if isinstance(manager_response, AsyncResult) else manager_response
        # _log.debug(f'{self.proxy_name} IN SEND REGISTRATION, AFTER GET')
        tries_remaining = 2
        if not success:
            _log.debug(f'{self.proxy_name} IN SEND REGISTRATION, NO SUCCESS, {tries_remaining} TRIES REMAINING')
            if tries_remaining > 0:
                sleep(self.registration_retry_delay)
                tries_remaining -= 1
            else:
                raise SystemExit(f"Unable to register with Proxy Manager @ {self.peers[self.manager]}. Exiting.")
        _log.debug(f'IN SEND REGISTRATION, SUCCESS WAS: {success}')

    @classmethod
    @abc.abstractmethod
    def get_unique_remote_id(cls, unique_remote_id: tuple) -> tuple:
        """Get a unique identifier for the proxy server
         given a unique_remote_id and protocol-specific set of parameters."""
        pass
