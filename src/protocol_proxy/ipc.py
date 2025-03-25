import abc
import json
import logging
import struct

from ast import literal_eval
from gevent import select, spawn
from gevent.event import AsyncResult
from gevent.socket import socket, gethostbyname, AF_INET, SOCK_STREAM, SHUT_RDWR, error as SocketError
from itertools import cycle
from psutil import net_connections
from typing import NamedTuple, Callable
from uuid import UUID
from weakref import WeakKeyDictionary

_log = logging.getLogger(__name__)


class Callback(NamedTuple):
    method: Callable
    name: str
    provides_response: bool

class Message(NamedTuple):
    method_name: str
    payload: bytes
    request_id: int = None
    response_expected: bool = False

class SocketParams(NamedTuple):
    address: str = 'localhost'
    port: int = 22801


class ProtocolHeaders:
    #  The endian_indicator and version number are added where needed and not included in the portion of the format here.
    FORMAT = 'Q16sH16s16s'
    HEADER_LENGTH = struct.calcsize(FORMAT)
    VERSION = 0                                     # 2 byte (16 bit) integer (H)

    @abc.abstractmethod
    def __init__(self, data_length: int, method_name: str, request_id: int, sender_id, sender_token, **kwargs):
        if kwargs:
            _log.warning(f'Received extra kwargs for Proxy Protocol version {self.VERSION}: {list(kwargs.keys())}')
        self.data_length: int = data_length         # 8 byte (64 bit) integer (Q)
        self.method_name: str = method_name         # 16-byte (32 character) string (16s)
        self.request_id: int = request_id           # 16-byte (16 bit) integer (H)
        self.sender_id = sender_id                  # 16-byte UUID (16s)
        self.sender_token = sender_token            # 16-byte UUID (16s)
        # TODO: The token, and possibly sender_id should be an actual encrypted token based on those values, not plain text.

    @staticmethod
    def bitflag_is_set(bit_position, byte_value):
        print(f"IN BITFLAG IS SET: NEED BIT POSITION: {bit_position} FROM {byte_value}")
        return bool((byte_value & (1 << bit_position)) >> bit_position)

    @abc.abstractmethod
    def pack(self):
        # TODO: The sender_id is currently the proxy_id, which is a tuple. This should probably become a UUID?
        #  (Need to figure out how/where to map one to the other.)
        return struct.pack(f'>H{ProtocolHeaders.FORMAT}', self.VERSION, self.data_length,
                           self.method_name.encode('utf8'), self.request_id, str(self.sender_id).encode('utf8'),
                           self.sender_token.bytes)

    @abc.abstractmethod
    def unpack(self, header_bytes):
        pass


class HeadersV1(ProtocolHeaders):
    FORMAT = ProtocolHeaders.FORMAT + 'H'
    HEADER_LENGTH = struct.calcsize(FORMAT)  # TODO: Should we make this a property instead?
    RESPONSE_EXPECTED_BIT = 0
    VERSION = 1

    def __init__(self, data_length: int, method_name: str, request_id: int, sender_id, sender_token,
                 response_expected: bool = False, **kwargs):
        super(HeadersV1, self).__init__(data_length, method_name, request_id, sender_id, sender_token, **kwargs)
        self.response_expected: bool = response_expected  # Position 0 in bitflags within struct.

    def pack(self):
        bitflags = (int(self.response_expected) << self.RESPONSE_EXPECTED_BIT)  # 2 byte (16-bit) bitflags.
        return super(HeadersV1, self).pack() + struct.pack(f'>H', bitflags)

    @classmethod
    def unpack(cls, header_bytes):
        (data_length, method_bytes, request_id, sender_id_bytes, sender_token_bytes,
         bitflags) = struct.unpack(cls.FORMAT, header_bytes)
        response_expected = cls.bitflag_is_set(cls.RESPONSE_EXPECTED_BIT, bitflags)
        method = method_bytes.rstrip(b'\x00').decode('utf8')
        # TODO: Instead of converting back to a tuple, like this,
        #  we should probably be using a UUID in place of proxy_id over the wire.
        sender_id = literal_eval(sender_id_bytes.rstrip(b'\x00').decode('utf8'))
        print(f'SENDER_TOKEN_BYTES IS A: {type(sender_token_bytes)}')
        sender_token = UUID(bytes=sender_token_bytes)
        return cls(data_length, method, request_id, sender_id, sender_token, response_expected)


class IPCConnector:
    PROTOCOL_VERSION = {1: HeadersV1}

    def __init__(self, proxy_id, token, inbound_params: SocketParams = None, remote_params: SocketParams = None,
                 chunk_size: int = 1024, encrypt: bool = False, min_port: int = 22801, max_port: int = 22899):
        """ Handles socket communication between ProtocolProxy and ProtocolProxyManager.
         TODO: Mechanism for polling communication with remote (where push is not possible)?
         TODO: Implement encryption.
        """
        self.chunk_size: int = chunk_size
        self.min_port: int = min_port
        self.max_port: int = max_port
        self.proxy_id = proxy_id
        self.token = token
        print(f'{self.proxy_id} IPC: IN INIT')

        self.inbound_server_socket: socket = self.setup_inbound_socket(inbound_params)

        self.callbacks: dict[str, Callback] = {}
        self.inbounds: set[socket] = {self.inbound_server_socket}   # Sockets from which we expect to read
        self.outbounds: set[socket] = set()                         # Sockets to which we expect to write
        self.outbound_messages: WeakKeyDictionary[socket, Message] = WeakKeyDictionary()
        self.last_request_id = 0
        self._request_id = cycle(range(1, 4))

        if remote_params:
            print(f'{self.proxy_id} IPC: FOUND REMOTE_PARAMS, NEED TO REGISTER')
            self.send_registration(remote_params)
        self.remote_params = remote_params

        self.stop = False
        self.loop_greenlet = spawn(self.select_loop)

    @property
    def next_request_id(self):
        return next(self._request_id)

    def setup_inbound_socket(self, socket_params: SocketParams = None) -> socket:
        inbound_socket: socket = socket(AF_INET, SOCK_STREAM)
        inbound_socket.setblocking(False)
        if socket_params:
            try:
                inbound_socket.bind(socket_params)
                inbound_socket.listen(5)  # TODO: The default is "a reasonable value". Should this be left "reasonable"?
                return inbound_socket
            except SocketError as e:
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
            except SocketError:
                next_port += 1
            except StopIteration:
                _log.error(f'Unable to bind inbound socket to {socket_params.address}'
                           f' on any port in range: {self.min_port} - {self.max_port}.')
                break
        inbound_socket.listen(5)  # TODO: The default is "a reasonable value". Should this be left "reasonable"?
        return inbound_socket

    def register_callback(self, callback, method_name, provides_response=False):
        print(f'{self.proxy_id} IPC: REGISTER CALLBACK RECEIVED: {method_name}')
        self.callbacks[method_name] = Callback(callback, method_name, provides_response)

    def send(self, remote: SocketParams, message: Message):
        print(f'{self.proxy_id} IPC: IN SEND')
        outbound = socket(AF_INET, SOCK_STREAM)
        outbound.setblocking(False)
        try:
            outbound.connect_ex(remote)
        except SocketError as e:
            _log.warning(f"Unexpected error connecting to {remote}: {e}")
        self.outbounds.add(outbound)
        self.outbound_messages[outbound] = message

    def send_registration(self, remote: SocketParams):
        print(f'{self.proxy_id} IPC: IN SEND REGISTRATION')
        local_address, local_port = self.inbound_server_socket.getsockname()
        message = Message(
            method_name='REGISTER_PROXY',
            payload= json.dumps({'address': local_address, 'port':local_port, 'proxy_id': self.proxy_id,
                                 'token': self.token.hex}).encode('utf8')
        )
        print(f'{self.proxy_id} IPC: MESSAGE TO REGISTER: {message}')
        self.send(remote, message)
        # TODO: Should we get a confirmation? Say, a signed version of the token?

    def select_loop(self):
        while not self.stop:
            # TODO: Should the select statement be timing out to let the loop continue?
            readable, writable, exceptional = select.select(self.inbounds, self.outbounds,
                                                            self.inbounds | self.outbounds)
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
                self._handle_exceptional_socket(s)

        # Loop has existed: self.stop == True
        self.inbound_server_socket.shutdown(SHUT_RDWR)
        self.inbound_server_socket.close()
        for s in self.inbounds | self.outbounds:
            s.shutdown(SHUT_RDWR)
            s.close()

    def _send_headers(self, s: socket, data_length: int, request_id: int, response_expected: bool, method_name: str,
                      protocol_version: int = 1):
        if not (protocol := self.PROTOCOL_VERSION.get(protocol_version)):
            raise NotImplementedError(f'Unable to send with unknown proxy protocol version: {protocol_version}')
        header_bytes = protocol(data_length, method_name, request_id, self.proxy_id, self.token,
                                response_expected).pack()
        s.send(header_bytes)

    def _receive_headers(self, s: socket) -> (int, str):
        received = s.recv(2)
        print(f"IN _RECEIVE_HEADERS, GOT: {received}")
        if not (protocol := self.PROTOCOL_VERSION.get(struct.unpack('>H', received)[0])):
            raise NotImplementedError(f'Unknown protocol version ({protocol.VERSION}) received from: {s.getpeername()}')
        print(f"HEADER LENGTH IS: {protocol.HEADER_LENGTH}")
        header_bytes = s.recv(protocol.HEADER_LENGTH)
        print(f'GOT {len(header_bytes)} BYTES: {header_bytes}')
        if len(header_bytes) != protocol.HEADER_LENGTH:
            raise SocketError(f'Failed to read headers. Received {len(header_bytes)} bytes.')
        else:
            return protocol.unpack(header_bytes)

    def _receive_socket(self, s: socket):
        print(f'{self.proxy_id} IPC: _IN RECEIVE SOCKET GOT {s}')
        headers = self._receive_headers(s)
        if callback := self.callbacks.get(headers.method_name):
            remaining = headers.data_length
            buffer = b''
            while chunk := s.recv(read_length := max(0, remaining if remaining < self.chunk_size else self.chunk_size)):
                buffer += chunk
                remaining -= read_length
                # TODO: Should we sleep in this loop?
            if callback.provides_response:
                async_result = AsyncResult()
                spawn(callback.method, headers, buffer, async_result)
                self.outbound_messages[s] = async_result
                self.outbounds.add(s)
            else:
                spawn(callback.method, headers, buffer)
        s.shutdown(SHUT_RDWR)
        s.close()

    def _send_socket(self, s: socket):
        print(f'{self.proxy_id} IPC: IN _SEND SOCKET')
        if not (message := self.outbound_messages.get(s)):
            # self.outbounds.discard(s)
            _log.warning(f'Outbound socket to {s.getsockname()} was ready, but no outbound message was found.')
            print(f'{self.proxy_id} IPC: DISCARDING FROM OUTBOUNDS')
        elif isinstance(message.payload, AsyncResult) and not message.payload.ready():
                print(f'{self.proxy_id} IPC: SLEEPY_TIME (NOT READY)')
                self.outbounds.add(s)
        else:
            payload = message.payload.get() if isinstance(message.payload, AsyncResult) else message.payload
            request_id = message.request_id if message.request_id else self.next_request_id
            self._send_headers(s, len(payload), request_id, message.response_expected, message.method_name)
            s.sendall(payload)  # TODO: Should we send in chunks and sleep in between?
            if not message.response_expected:
                s.shutdown(SHUT_RDWR)
                s.close()
            # self.outbounds.discard(s)
            self.outbound_messages.pop(s)


    def _handle_exceptional_socket(self, s: socket):
        # TODO: We could probably use some logging here.
        print(f'{self.proxy_id} IPC: IN _HANDLE EXCEPTIONAL SOCKET')
        self.inbounds.discard(s)
        self.outbounds.discard(s)
        s.shutdown(SHUT_RDWR)
        s.close()

    def stop(self):
        self.stop = True

