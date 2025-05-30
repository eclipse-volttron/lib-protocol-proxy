import asyncio
import json
import logging

from abc import ABC
from uuid import UUID

from ..ipc.asyncio import AsyncioIPCConnector, Future, SocketParams
from . import ProtocolProxy

logging.basicConfig(filename='protoproxy.log', level=logging.DEBUG,
                    format='%(asctime)s - %(message)s')
_log = logging.getLogger(__name__)


class AsyncioProtocolProxy(AsyncioIPCConnector, ProtocolProxy, ABC):
    def __init__(self, manager_address: str, manager_port: int, manager_id: UUID, manager_token: UUID, token: UUID,
                 proxy_id: UUID, proxy_name: str = None, registration_retry_delay: float = 20.0, **kwargs):
        """ Base class for protocols requiring a standalone process to handle incoming and outgoing requests.
        """
        super(AsyncioProtocolProxy, self).__init__(manager_address=manager_address, manager_port=manager_port,
                                                  manager_id=manager_id, manager_token=manager_token, proxy_id=proxy_id,
                                                  registration_retry_delay=registration_retry_delay,
                                                  token=token, proxy_name=proxy_name, **kwargs)

    def get_local_socket_params(self) -> SocketParams:
        return self.inbound_server.sockets[0].getsockname()

    async def send_registration(self, remote: SocketParams):
        _log.debug('ASYNCIO_PROXY: SEND REGISTRATION, BEFORE SUPER.')
        message = super(AsyncioProtocolProxy, self).send_registration(remote)
        _log.debug('ASYNCIO_PROXY: SEND REGISTRATION, AFTER SUPER, BEFORE SEND.')
        manager_response = await self.send(remote, message)
        _log.debug('ASYNCIO_PROXY: SEND REGISTRATION AFTER SEND.')
        _log.debug(
            f'{self.proxy_name} IN SEND REGISTRATION, manager_response is a {type(manager_response)}'
            f' (initially): {manager_response}, and self.response_results is: {list(self.response_results.items())} ')
        success_bytes = await manager_response if isinstance(manager_response, Future) else manager_response
        success = json.loads(success_bytes.decode('utf8'))
        _log.debug(f'{self.proxy_name} IN SEND REGISTRATION, FUTURE.RESULT() IS : {success}')
        # TODO: Implement error handling and failure (along lines of copy below from gevent version (but working):
        # tries_remaining = 2
        # if not success:
        #     _log.debug(f'{self.proxy_name} IN SEND REGISTRATION, NO SUCCESS, {tries_remaining} TRIES REMAINING')
        #     if tries_remaining > 0:
        #         sleep(self.registration_retry_delay)
        #         tries_remaining -= 1
        #     else:
        #         raise SystemExit(f"Unable to register with Proxy Manager @ {self.peers[self.manager]}. Exiting.")
        # _log.debug(f'IN SEND REGISTRATION, SUCCESS WAS: {success}')

    async def start(self):
        await super(AsyncioProtocolProxy, self).start()
        await self.send_registration(self.manager_params)
        while True:
            await asyncio.sleep(0.1)
