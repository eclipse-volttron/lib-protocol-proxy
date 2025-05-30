import abc
import json
import logging

from uuid import UUID

from ..ipc import IPCConnector, ProtocolProxyMessage, ProtocolProxyPeer, SocketParams

logging.basicConfig(filename='protoproxy.log', level=logging.DEBUG,
                    format='%(asctime)s - %(message)s')
_log = logging.getLogger(__name__)


# noinspection PyMissingConstructor
class ProtocolProxy(IPCConnector):
    def __init__(self, *, manager_address: str, manager_port: int, manager_id: UUID, manager_token: UUID,
                 registration_retry_delay: float = 20.0, **kwargs):
        """NOTE: Proxy implementations MUST:
            1. Subclass a multitasking subclass of IPCConnector (gevent, asyncio, etc.)
            2. Subclass this "ProtocolProxy" class.
            3. Call super first to IPCConnector parent then this ProtocolProxy parent.
            4. Call send_registration asynchronously in their constructor after super calls.
        """
        _log.debug('PP: IN INIT.')
        super(ProtocolProxy, self).__init__(**kwargs)
        self.registration_retry_delay: float = registration_retry_delay
        self.manager_params = SocketParams(manager_address, manager_port)
        self.manager = manager_id
        self.peers[manager_id] = ProtocolProxyPeer(proxy_id=manager_id, socket_params=self.manager_params,
                                                   token=manager_token)


    @abc.abstractmethod
    def get_local_socket_params(self) -> SocketParams:
        pass

    @classmethod
    @abc.abstractmethod
    def get_unique_remote_id(cls, unique_remote_id: tuple) -> tuple:
        """Get a unique identifier for the proxy server
         given a unique_remote_id and protocol-specific set of parameters."""
        pass

    @abc.abstractmethod
    def send_registration(self, remote: SocketParams):
        # _log.debug(f'{self.proxy_name}: IN SEND REGISTRATION')
        local_address, local_port = self.get_local_socket_params()
        message = ProtocolProxyMessage(
            method_name='REGISTER_PEER',
            payload= json.dumps({'address': local_address, 'port':local_port, 'proxy_id': self.proxy_id.hex,
                                 'token': self.token.hex}).encode('utf8'),
            response_expected=True
        )
        # _log.debug(f'{self.proxy_name} IN SEND REGISTRATION, BEFORE SEND')
        # TODO: Inability to connect while on VPN, but is this related to MQTT or to local sockets.
        return message
