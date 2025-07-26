import abc
import logging

from collections.abc import Generator
from dataclasses import dataclass
from subprocess import Popen
from itertools import cycle
from psutil import net_connections
from typing import Awaitable, Callable, NamedTuple
from uuid import UUID

from . import callback, HeadersV1, ProtocolHeaders

_log = logging.getLogger(__name__)


class ProtocolProxyCallback(NamedTuple):
    method: Callable
    name: str
    provides_response: bool


@dataclass
class ProtocolProxyMessage:
    method_name: str
    payload:  bytes | Awaitable[bytes] | Callable[[any], bytes]
    request_id: int = None
    response_expected: bool = False


class SocketParams(NamedTuple):
    address: str = 'localhost'
    port: int = 22801

    def __repr__(self):
        return f'{self.address}:{self.port}'


@dataclass
class ProtocolProxyPeer:
    proxy_id: UUID
    token: UUID
    process: Popen = None
    socket_params: SocketParams = None


class IPCConnector:
    PROTOCOL_VERSION = {1: HeadersV1}

    def __init__(self, *, proxy_id, token, proxy_name: str = None, inbound_params: SocketParams = None,
                 chunk_size: int = 1024, encrypt: bool = False, min_port: int = 22801, max_port: int = 22899,
                 **kwargs):
        """ Handles socket communication between ProtocolProxy and ProtocolProxyManager.
         TODO: Mechanism for polling communication with remote (where push is not possible)?
         TODO: Implement encryption.
        """
        self.chunk_size: int = chunk_size
        self.encrypt: bool = encrypt
        self.inbound_params = inbound_params
        self.min_port: int = min_port
        self.max_port: int = max_port
        self.proxy_id = proxy_id
        self.proxy_name = proxy_name if proxy_name else str(self.proxy_id)
        self.token = token

        self.callbacks: dict[str, ProtocolProxyCallback] = {}
        self.peers: dict[UUID, ProtocolProxyPeer] = {}
        self.register_callback(self._handle_response, 'RESPONSE')
        self._request_id = cycle(range(1, 65535))

    @abc.abstractmethod
    def _get_ip_addresses(self, host_name: str) -> set[str]:
        pass

    @abc.abstractmethod
    @callback
    def _handle_response(self, headers: ProtocolHeaders, raw_message: bytes):
        pass

    @abc.abstractmethod
    def send(self, remote: SocketParams, message: ProtocolProxyMessage):
        """Send a message to the remote and return a bool, AsyncResult (gevent) or Future (asyncio)."""
        pass

    @abc.abstractmethod
    def _setup_inbound_server(self, socket_params: SocketParams = None):
        pass

    def unused_ports(self, address_info: set[str]) -> Generator[int]:
        used_ports = {nc.laddr.port for nc in net_connections() if
                      nc.laddr.ip in address_info}
        next_port = self.min_port
        for p in range(next_port, self.max_port + 1):
            if p not in used_ports:
                yield p

    @property
    def next_request_id(self):
        return next(self._request_id)

    def register_callback(self, cb_method, method_name, provides_response=False):
        _log.info(f'{self.proxy_name} registered callback: {method_name}')
        self.callbacks[method_name] = ProtocolProxyCallback(cb_method, method_name, provides_response)

    @abc.abstractmethod
    def start(self, *_, **__):
        pass

    @abc.abstractmethod
    def stop(self):
        pass
