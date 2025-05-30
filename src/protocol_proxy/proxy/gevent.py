import logging

from abc import ABC
from gevent import sleep, spawn
from gevent.event import AsyncResult
from uuid import UUID

from ..ipc.gevent import GeventIPCConnector, SocketParams
from . import ProtocolProxy

logging.basicConfig(filename='protoproxy.log', level=logging.DEBUG,
                    format='%(asctime)s - %(message)s')
_log = logging.getLogger(__name__)


class GeventProtocolProxy(GeventIPCConnector, ProtocolProxy, ABC):
    def __init__(self, *, proxy_id: UUID, token: UUID, manager_address: str, manager_port: int, manager_id: UUID,
                 manager_token: UUID, proxy_name: str = None, registration_retry_delay: float = 20.0, **kwargs):
        """ Base class for protocols requiring a standalone process to handle incoming and outgoing requests.
        """
        _log.debug('GPP: Before SUPER.')
        super(GeventProtocolProxy, self).__init__(proxy_id=proxy_id, token=token, proxy_name=proxy_name,
                                                  manager_address=manager_address, manager_port=manager_port,
                                                  manager_id=manager_id, manager_token=manager_token,
                                                  registration_retry_delay=registration_retry_delay, **kwargs)
        _log.debug('GPP: After SUPER.')
        spawn(self.send_registration, self.manager_params)
        _log.debug('GPP: END OF INIT (Just spawned SEND_REGISTRATION')

    def get_local_socket_params(self) -> SocketParams:
        _log.debug(f'INBOUND SERVER SOCKET NAME: {self.inbound_server_socket.getsockname()}')
        return self.inbound_server_socket.getsockname()

    def send_registration(self, remote: SocketParams):
        message = super(GeventProtocolProxy, self).send_registration(remote)
        manager_response = self.send(remote, message)
        _log.debug(
            f'{self.proxy_name} IN SEND REGISTRATION, manager_response is a {type(manager_response)} (initially): {manager_response}, and self.response_results is: {list(self.response_results.items())} ')
        # TODO: This should be using a try block to catch the timeout. Is the return even useful?
        success = manager_response.get(timeout=5) if isinstance(manager_response, AsyncResult) else manager_response
        # _log.debug(f'{self.proxy_name} IN SEND REGISTRATION, AFTER GET')
        tries_remaining = 2
        if not success:  # TODO: Shouldn't there be a loop here someplace?
            _log.debug(f'{self.proxy_name} IN SEND REGISTRATION, NO SUCCESS, {tries_remaining} TRIES REMAINING')
            if tries_remaining > 0:
                sleep(self.registration_retry_delay)
                tries_remaining -= 1
            else:
                raise SystemExit(f"Unable to register with Proxy Manager @ {self.peers[self.manager]}. Exiting.")
        _log.debug(f'IN SEND REGISTRATION, SUCCESS WAS: {success}')

