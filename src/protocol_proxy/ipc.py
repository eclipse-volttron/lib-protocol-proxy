import logging
import struct

from dataclasses import dataclass
from gevent import select, sleep, spawn
from gevent.event import AsyncResult
from gevent.socket import socket, gethostbyname, AF_INET, SOCK_STREAM, SHUT_RDWR
from gevent.subprocess import Popen
from itertools import cycle
from psutil import net_connections
from typing import NamedTuple, Callable
from uuid import UUID
from weakref import WeakKeyDictionary, WeakValueDictionary

from .decorator import callback
from .headers import HeadersV1, ProtocolHeaders

logging.basicConfig(filename='protoproxy.log', level=logging.DEBUG,
                    format='%(asctime)s - %(message)s')
_log = logging.getLogger(__name__)


class ProtocolProxyCallback(NamedTuple):
    method: Callable
    name: str
    provides_response: bool


@dataclass
class ProtocolProxyMessage:
    method_name: str
    payload: bytes
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

    def __init__(self, proxy_id, token, proxy_name: str = None, inbound_params: SocketParams = None,
                 chunk_size: int = 1024, encrypt: bool = False, min_port: int = 22801, max_port: int = 22899):
        """ Handles socket communication between ProtocolProxy and ProtocolProxyManager.
         TODO: Mechanism for polling communication with remote (where push is not possible)?
         TODO: Implement encryption.
         TODO: Asyncio version.
        """
        self.chunk_size: int = chunk_size
        self.encrypt: bool = encrypt
        self.min_port: int = min_port
        self.max_port: int = max_port
        self.proxy_id = proxy_id
        self.proxy_name = proxy_name if proxy_name else str(self.proxy_id)
        self.token = token

        self.inbound_server_socket: socket = self.setup_inbound_socket(inbound_params)

        self.callbacks: dict[str, ProtocolProxyCallback] = {}
        self.register_callback(self._handle_response, 'RESPONSE')
        self.inbounds: set[socket] = {self.inbound_server_socket}   # Sockets from which we expect to read
        self.outbounds: set[socket] = set()                         # Sockets to which we expect to write
        self.outbound_messages: WeakKeyDictionary[socket, ProtocolProxyMessage] = WeakKeyDictionary()
        self.peers: dict[UUID, ProtocolProxyPeer] = {}
        self.response_results: WeakValueDictionary[int, AsyncResult] = WeakValueDictionary()
        self.last_request_id = 0
        self._request_id = cycle(range(1, 65535))

        self._stop = False

    @property
    def next_request_id(self):
        return next(self._request_id)

    def stop(self):
        self._stop = True

    def setup_inbound_socket(self, socket_params: SocketParams = None) -> socket:
        inbound_socket: socket = socket(AF_INET, SOCK_STREAM)
        inbound_socket.setblocking(False)
        if socket_params:
            try:
                inbound_socket.bind(socket_params)
                inbound_socket.listen(5)  # TODO: The default is "a reasonable value". Should this be left "reasonable"?
                return inbound_socket
            except (OSError, Exception) as e:
                _log.warning(f'Unable to bind to provided inbound socket {socket_params}. Trying next available. - {e}')
        else:
            socket_params = SocketParams()
        used_ports = {nc.laddr.port for nc in net_connections() if nc.laddr.ip == gethostbyname(socket_params.address)}
        next_port = self.min_port
        while True:
            try:
                next_port = next(p for p in range(next_port, self.max_port + 1) if p not in used_ports)
                inbound_socket.bind((socket_params.address, next_port))
                break
            except OSError:
                next_port += 1
            except StopIteration:
                _log.error(f'Unable to bind inbound socket to {socket_params.address}'
                           f' on any port in range: {self.min_port} - {self.max_port}.')
                break
        try:
            inbound_socket.listen(5)  # TODO: The default is "a reasonable value". Should this be left "reasonable"?
        except (OSError, Exception) as e:
            _log.warning(f'{self.proxy_name}: Socket error listening on {inbound_socket.getsockname()}: {e}')
        return inbound_socket

    def register_callback(self, callback, method_name, provides_response=False):
        _log.info(f'{self.proxy_name} registered callback: {method_name}')
        self.callbacks[method_name] = ProtocolProxyCallback(callback, method_name, provides_response)

    def send(self, remote: SocketParams, message: ProtocolProxyMessage) -> bool | AsyncResult:
        outbound = socket(AF_INET, SOCK_STREAM)
        outbound.setblocking(False)
        try:
            # _log.debug(f'{self.proxy_name}: IN SEND, GOT REMOTE: {remote}')
            if (error_code := outbound.connect_ex(remote)) != 115:
                _log.warning(f'{self.proxy_name} Connection to outbound socket returned code: {error_code}.')
        except (OSError, Exception) as e:
            _log.warning(f"{self.proxy_name}: Unexpected error connecting to {remote}: {e}")
            return False
        if message.request_id is None:
            message.request_id = self.next_request_id
        self.outbounds.add(outbound)
        self.outbound_messages[outbound] = message
        if message.response_expected:
            async_result = AsyncResult()
            self.response_results[message.request_id] = async_result
            return async_result
        else:
            return True

    def select_loop(self):
        # _log.debug(f'{self.proxy_name}: IN SELECT LOOP')
        while not self._stop:
            try:
                readable, writable, exceptional = select.select(self.inbounds, self.outbounds,
                                                                self.inbounds | self.outbounds, timeout=0.1)
            except (OSError, Exception) as e:
                _log.warning(f"{self.proxy_name}: An error occurred in select loop: {e}")
                _log.debug(f'INBOUND: {self.inbounds}')
                _log.debug(f'OUTBOUND: {self.outbounds}')
                sleep(100)
            else:
                for s in readable:  # Handle incoming sockets.
                    if s is self.inbound_server_socket:    # The server socket is ready to accept a connection
                        client_socket, client_address = s.accept()
                        client_socket.setblocking(0)
                        self.inbounds.add(client_socket)
                    else:
                        self.inbounds.discard(s)
                        spawn(self._receive_socket, s)
                for s in writable:  # Handle outgoing sockets.
                    self.outbounds.discard(s)
                    spawn(self._send_socket, s)
                for s in exceptional:   # Handle "exceptional conditions"
                    spawn(self._handle_exceptional_socket, s)
                sleep(0.1)
        for s in self.inbounds | self.outbounds:
            try:
                s.shutdown(SHUT_RDWR)
            except (OSError, Exception):
                pass  # Nothing to do. An error here no longer matters.
            finally:
                s.close()

    def _receive_headers(self, s: socket) -> (int, str):
        try:
            #_log.debug('START')
            received = s.recv(2)
            #_log.debug('RECEIVED 2')
            if len(received) == 0:
                #_log.debug('LEN(0) RECEIVED')
                _log.warning(f'{self.proxy_name} received closed socket from ({s.getpeername()}.')
                return None
            #_log.debug('BEFORE VERSION CHECK')
            if not (protocol := self.PROTOCOL_VERSION.get(struct.unpack('>H', received)[0])):
                raise NotImplementedError(f'Unknown protocol version ({protocol.VERSION})'
                                          f' received from: {s.getpeername()}')
            #_log.debug('BEFORE RECV(HEADER_LENGTH)')
            header_bytes = s.recv(protocol.HEADER_LENGTH)
            #_log.debug('BEFORE LEGNTH CHECK')
            if len(header_bytes) == protocol.HEADER_LENGTH:
                #_log.debug(f'BEFORE UNPACKING, BYTES ARE: "{header_bytes}"')
                return protocol.unpack(header_bytes)
            else:
                _log.warning(f'Failed to read headers. Received {len(header_bytes)} bytes: {header_bytes}')
        except (OSError, Exception) as e:
            _log.warning(f'{self.proxy_name}: Socket exception reading headers: {e}')

    def _receive_socket(self, s: socket):
        # _log.debug(f'{self.proxy_name}: IN RECEIVE SOCKET')
        headers = self._receive_headers(s)
        # _log.debug(f'GOT BACK HEADERS: {headers}')
        if headers is not None and (callback := self.callbacks.get(headers.method_name)):
            remaining = headers.data_length
            buffer = b''
            done = False
            while not done:  # TODO: This should not go on forever.
                try:
                    #_log.debug(f'BUFFER IS: {buffer}')
                    #_log.debug(f'REMAINING IS: {remaining}')
                    while chunk := s.recv(read_length := max(0, remaining if remaining < self.chunk_size else self.chunk_size)):
                        buffer += chunk
                        remaining -= read_length
                        # TODO: Should we sleep in this loop?
                    #_log.debug(f'CALLBACK IS: {callback}')
                    if callback.provides_response:
                        async_result = AsyncResult()
                        self.outbound_messages[s] = ProtocolProxyMessage(method_name='RESPONSE', payload=async_result,
                                                                         request_id=headers.request_id)
                        spawn(callback.method, self, headers, buffer, async_result)
                        self.outbounds.add(s)
                    else:
                        spawn(callback.method, self, headers, buffer)
                except BlockingIOError as e:
                    _log.info(f'BlockingIOError: {e}')
                    sleep(0.1)
                except (OSError, Exception) as e:
                    _log.warning(f'{self.proxy_name}: Socket exception reading payload: {e}')
                    s.close()
                    done = True
                else:
                    if not callback.provides_response:
                        s.shutdown(SHUT_RDWR)
                        s.close()
                    done = True

    def _send_headers(self, s: socket, data_length: int, request_id: int, response_expected: bool, method_name: str,
                      protocol_version: int = 1):
        if not (protocol := self.PROTOCOL_VERSION.get(protocol_version)):
            raise NotImplementedError(f'Unable to send with unknown proxy protocol version: {protocol_version}')
        # _log.debug(f'IN _SEND_HEADERS: ({data_length}, {method_name}, {request_id}, {self.proxy_id}, {self.token}, {response_expected})')
        header_bytes = protocol(data_length, method_name, request_id, self.proxy_id, self.token,
                                response_expected).pack()
        try:
            s.send(header_bytes)
        except (OSError, Exception) as e:
            _log.warning(f'{self.proxy_name}: Socket exception sending headers for {method_name}'
                             f' (request_id: {request_id}): {e}')

    def _send_socket(self, s: socket):
        _log.debug(f'{self.proxy_name}: IN SEND SOCKET')
        if not (message := self.outbound_messages.get(s)):
            _log.warning(f'Outbound socket to {s.getpeername()} was ready, but no outbound message was found.')
        elif isinstance(message.payload, AsyncResult) and not message.payload.ready():
            self.outbounds.add(s)
            _log.debug('IN SEND SOCKET, WAS ADDED BACK TO OUTBOUND BECAUSE ASYNC_RESULT WAS NOT READY.')
        else:
            # if isinstance(message.payload, AsyncResult):
                #_log.debug(f"PAYLOAD IS READY: {message.payload.ready()}")
            # _log.debug(f'{self.proxy_name}: IN SEND SOCKET message is: {message}')
            payload = message.payload.get() if isinstance(message.payload, AsyncResult) else message.payload
            #if isinstance(message.payload, AsyncResult):
                #_log.debug(f'{self.proxy_name}: IN SEND SOCKET, payload {id(message.payload)} is a {type(message.payload)} containing: {message.payload.get()}, payload is: {payload}')
            self._send_headers(s, len(payload), message.request_id, message.response_expected, message.method_name)
            try:
                s.sendall(payload)  # TODO: Should we send in chunks and sleep in between?
                if message.response_expected:
                    self.inbounds.add(s)
            except (OSError, Exception) as e:
                _log.warning(f'{self.proxy_name}: Socket exception sending {message.method_name}'
                             f' payload (request_id: {message.request_id}): {e}')
                s.close()
            else:
                if not message.response_expected:
                    s.shutdown(SHUT_RDWR)
                    s.close()
            finally:
                self.outbound_messages.pop(s)


    def _handle_exceptional_socket(self, s: socket):
        try:
            s.recv(1)  # Trigger the exception in order to log it.
        except (OSError, Exception) as e:
            _log.warning(f'{self.proxy_name}: Encountered exception on socket: {e}')
        else:
            s.shutdown(SHUT_RDWR)
            _log.warning(f'{self.proxy_name}: Unable to determine the exception on a socket marked exceptional by select.')
        finally:
            self.inbounds.discard(s)
            self.outbounds.discard(s)
            s.close()

    @callback
    def _handle_response(self, headers: ProtocolHeaders, raw_message: bytes):
        result = self.response_results.get(headers.request_id)
        if not result:
            _log.warning(f'Received response {headers.request_id} from {headers.sender_id} containing "{raw_message.decode()}",'
                         f' but result object is no longer available.')
        else:
            result.set(raw_message)
