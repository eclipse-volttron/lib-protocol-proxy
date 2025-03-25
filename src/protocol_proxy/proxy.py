import abc
import logging

from uuid import UUID

from .ipc import IPCConnector, SocketParams

_log = logging.getLogger(__name__)


class ProtocolProxy:
    def __init__(self, unique_remote_id: tuple, token: UUID, *args, **kwargs):
        """ Base class for protocols requiring a standalone process to handle incoming and outgoing requests.
         TODO: Instantiate IPCConnector
        """
        print('PROTOCOL PROXY: IN INIT')
        print(kwargs)
        # TODO: How to get a token here secretly?
        self.ipc = IPCConnector(proxy_id=self.get_proxy_id(unique_remote_id), token=token,
                                remote_params=SocketParams(kwargs.get('manager_address'), kwargs.get('manager_port')))
        print('PROTOCOL PROXY: IPC HAS BEEN CREATED:')
        print(self.ipc)
        pass

    @classmethod
    @abc.abstractmethod
    def get_proxy_id(cls, unique_remote_id: tuple) -> str:
        """Get a unique ID for the proxy server given a unique_remote_id and protocol-specific set of parameters."""
        pass

#### Dummy Proxies for testing.
from gevent import sleep  # Base ProtocolProxy should not rely on gevent. There might be gevent and asyncio subclasses.
class ConcreteProxy(ProtocolProxy):
    def __init__(self, unique_remote_id, *args, **kwargs):
        print('CONCRETE PROXY: IN INIT')
        super(ConcreteProxy, self).__init__(unique_remote_id, *args, **kwargs)
        print('CONCRETE PROXY: AFTER SUPER')
        print(kwargs)
        while True:
            sleep(5)

    @classmethod
    def get_proxy_id(cls, unique_remote_id):
        return unique_remote_id


class CementProxy(ProtocolProxy):
    def __init__(self, unique_remote_id: tuple, *args, **kwargs):
        print("CEMENT PROXY: IN CEMENT PROXY")
        super(CementProxy, self).__init__(unique_remote_id, *args, **kwargs)
        print("CEMENT PROXY: STARTING LOOP")
        while True:
            sleep(5)
        print("CEMENT PROXY: AFTER LOOP")

    @classmethod
    def get_proxy_id(cls, unique_remote_id):
        return unique_remote_id

