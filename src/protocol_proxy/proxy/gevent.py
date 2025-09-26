import logging

from abc import ABC
from gevent import sleep, spawn
from gevent.event import AsyncResult
from uuid import UUID

from ..ipc.gevent import GeventIPCConnector, GeventProtocolProxyPeer, SocketParams
from . import ProtocolProxy

_log = logging.getLogger(__name__)


class GeventProtocolProxy(GeventIPCConnector, ProtocolProxy, ABC):
    def __init__(self, *, proxy_id: UUID, token: UUID, manager_address: str, manager_port: int, manager_id: UUID,
                 manager_token: UUID, proxy_name: str = None, registration_retry_delay: float = 20.0, **kwargs):
        """ A gevent class for protocols requiring a standalone process to handle incoming and outgoing requests.
        """
        super(GeventProtocolProxy, self).__init__(proxy_id=proxy_id, token=token, proxy_name=proxy_name,
                                                  manager_address=manager_address, manager_port=manager_port,
                                                  manager_id=manager_id, manager_token=manager_token,
                                                  registration_retry_delay=registration_retry_delay, **kwargs)
        self.peers[manager_id] = GeventProtocolProxyPeer(proxy_id=manager_id, socket_params=self.manager_params,
                                                   token=manager_token)
        spawn(self.send_registration, self.peers[manager_id])

    def get_local_socket_params(self) -> SocketParams:
        return self.inbound_server_socket.getsockname()

    def send_registration(self, remote: GeventProtocolProxyPeer):
        message = self._get_registration_message()
        tries_remaining = 2
        while tries_remaining > 0:
            manager_response = self.send(remote, message)
            # TODO: This should be using a try block to catch the timeout. Is the return even useful?
            success = manager_response.get(timeout=5) if isinstance(manager_response, AsyncResult) else manager_response
            if success:
                _log.debug(f'IN SEND REGISTRATION, SUCCESS WAS: {success}')
                break
            else:
                _log.debug(f'{self.proxy_name} IN SEND REGISTRATION, NO SUCCESS, {tries_remaining} TRIES REMAINING')
                tries_remaining -= 1
                if tries_remaining > 0:
                    sleep(self.registration_retry_delay)
                else:
                    raise SystemExit(f"Unable to register with Proxy Manager @ {self.peers[self.manager]}. Exiting.")
