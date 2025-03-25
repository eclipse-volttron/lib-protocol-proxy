import atexit
import json
import logging
import os
import sys

from dataclasses import dataclass
from gevent.subprocess import Popen, PIPE
from typing import Type
from uuid import uuid4, UUID
from weakref import WeakValueDictionary

from .ipc import IPCConnector, SocketParams, ProtocolHeaders
from .proxy import ProtocolProxy

_log = logging.getLogger(__name__)


@dataclass
class ProtocolProxyServer:
    process: Popen
    proxy_id: str
    token: UUID
    socket_params: SocketParams = None


def callback(func):
    def verify(self, headers: ProtocolHeaders, raw_message: any, async_result=None):
        print(f'PPM: ATTEMPTING TO VERIFY {headers.sender_id} of type: {type(headers.sender_id)}')
        print(f'PPM: SELF.PROXIES HAS: {list(self.proxies.keys())}')
        if proxy := self.proxies.get(headers.sender_id):
            if headers.sender_token == proxy.token:
                result = func(self, headers, raw_message)
                if async_result:
                    async_result.set(result)
            else:
                _log.warning(f'Unable to authenticate caller: {headers.sender_id}')
        else:
            _log.warning(f'Request from unknown party: {headers.sender_id}')
    return verify


class ProtocolProxyManager:
    managers = WeakValueDictionary()

    def __init__(self, proxy_class: Type[ProtocolProxy]):
        self.proxy_class = proxy_class
        self.proxies: dict[str, ProtocolProxyServer] = {}
        self.ipc = IPCConnector(proxy_id=f'proxy_manager', token=uuid4())
        self.ipc.register_callback(self.handle_registration, 'REGISTER_PROXY')

    def get_proxy(self, unique_remote_id: tuple) -> ProtocolProxyServer:
        print("PPM: IN GET PROXY")
        proxy_id = self.proxy_class.get_proxy_id(unique_remote_id)
        print(f"PPM: PROXY ID IS: {proxy_id}")
        if proxy_id not in self.proxies:
            print(f"PPM: {proxy_id} WAS NOT IN SELF.PROXIES")
            proxy_process = Popen([sys.executable, '-m', 'protocol_proxy.launch'], stdin=PIPE) #, stdout=PIPE, stderr=PIPE)
            # TODO: Implement logging along lines of AIP.start_agent() (uncomment PIPES above too).
            # TODO: Remove the following commented lines after verifying that new approach works.:
            #  module, func = self.proxy_class.__module__, self.proxy_class.__name__
            #  proxy_process = Popen([sys.executable, '-c', f'from {module} import {func}; {func}({repr(unique_remote_id)})'])
            print(f"PPM: CREATED PROXY PROCESS: {proxy_process.pid}")
            token = uuid4()
            print(f"PPM: CREATED TOKEN: {token}")
            proxy_process.stdin.write(token.hex.encode())
            proxy_process.stdin.flush()
            proxy_process.stdin.close()
            proxy_process.stdin = open(os.devnull)
            print("PPM: FINISHED WRITING TOKEN")
            self.proxies[proxy_id] = ProtocolProxyServer(process=proxy_process, proxy_id=proxy_id, token=token)
            atexit.register(self.cleanup_proxy_process, proxy_process)
            print(f"PPM: RETURNING PROXY CONTAINER: {self.proxies[proxy_id]}")
        return self.proxies[proxy_id]

    @callback
    def handle_registration(self, headers: ProtocolHeaders, raw_message: bytes):
        print(f"RAW MESSAGE IS: {len(raw_message)} IN LENGTH")
        message = json.loads(raw_message.decode('utf8'))
        proxy: ProtocolProxyServer = self.proxies.get(headers.sender_id)
        if (address := message.get('address')) and (port := message.get('port')):
            proxy.socket_params = SocketParams(address=address, port=port)
            print(f'PPM: AFTER REGISTRATION, PROXIES IS: {self.proxies}')
            return True
        else:
            return False

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
        print('IN GET_MANAGER')
        if proxy_class.__name__ in cls.managers:
            print('PROXY CLASS FOUND IN MANAGERS.')
            manager = cls.managers[proxy_class.__name__]
        else:
            print('DID NOT FIND PROXY CLASS IN MANAGERS. CREATING ONE.')
            manager = cls(proxy_class)
            cls.managers[proxy_class.__name__] = manager
            print('RETURNING MANAGER')
        return manager
