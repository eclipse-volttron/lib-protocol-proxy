import json
import logging
import struct

from gevent import select, sleep, spawn
from gevent.event import AsyncResult
from gevent.greenlet import Greenlet
from gevent.socket import socket, getaddrinfo, AF_INET, SOCK_STREAM, SHUT_RDWR
from uuid import UUID
from weakref import WeakKeyDictionary, WeakValueDictionary

from . import callback, IPCConnector, ProtocolHeaders, ProtocolProxyMessage, SocketParams

logging.basicConfig(filename='protoproxy.log', level=logging.DEBUG,
                    format='%(asctime)s - %(message)s')
_log = logging.getLogger(__name__)


class GeventIPCConnector(IPCConnector):
    def __init__(self, *, proxy_id: UUID, token: UUID, proxy_name: str = None, inbound_params: SocketParams = None,
                 chunk_size: int = 1024, encrypt: bool = False, min_port: int = 22801, max_port: int = 22899,
                 **kwargs):
        self.inbound_server_socket: socket = None
        super(GeventIPCConnector, self).__init__(proxy_id=proxy_id, token=token, proxy_name=proxy_name,
                                                 inbound_params=inbound_params, chunk_size=chunk_size, encrypt=encrypt,
                                                 min_port=min_port, max_port=max_port, **kwargs)
        self.inbounds: set[socket] = set()      # Sockets from which we expect to read
        self.outbounds: set[socket] = set()     # Sockets to which we expect to write
        self.outbound_messages: WeakKeyDictionary[socket, ProtocolProxyMessage] = WeakKeyDictionary()
        self.response_results: WeakValueDictionary[int, AsyncResult] = WeakValueDictionary()
        self._stop = False

    def _get_ip_addresses(self, host_name: str) -> set[str]:
        return {ai[4][0] for ai in getaddrinfo(host_name, None)}

    def _setup_inbound_server(self, socket_params: SocketParams = None):
        inbound_socket: socket = socket(AF_INET, SOCK_STREAM)
        inbound_socket.setblocking(False)
        if socket_params:
            try:
                inbound_socket.bind(socket_params)
                inbound_socket.listen(5)  # TODO: The default is "a reasonable value". Should this be left "reasonable"?
                self.inbound_server_socket = inbound_socket
                return
            except (OSError, Exception) as e:
                _log.warning(f'Unable to bind to provided inbound socket {socket_params}. Trying next available. - {e}')
        else:
            socket_params = SocketParams()
        while True:
            try:
                next_port = next(self.unused_ports(self._get_ip_addresses(socket_params.address)))
                inbound_socket.bind((socket_params.address, next_port))
            except OSError:
                continue
            except StopIteration:
                _log.error(f'Unable to bind inbound socket to {socket_params.address}'
                           f' on any port in range: {self.min_port} - {self.max_port}.')
                break  # TODO: Should this return instead of break?
            else:
                self.inbound_params = SocketParams(*inbound_socket.getsockname())
                break
        try:
            inbound_socket.listen(5)  # TODO: The default is "a reasonable value". Should this be left "reasonable"?
        except (OSError, Exception) as e:
            _log.warning(f'{self.proxy_name}: Socket error listening on {self.inbound_params}: {e}')
        self.inbound_server_socket = inbound_socket
        self.inbounds.add(self.inbound_server_socket)
        return

    @callback
    def _handle_response(self, headers: ProtocolHeaders, raw_message: bytes):
        result = self.response_results.get(headers.request_id)
        if not result:
            _log.warning(f'Received response {headers.request_id} from {headers.sender_id} containing "{raw_message.decode()}",'
                         f' but result object is no longer available.')
        else:
            result.set(raw_message)

    def send(self, remote: SocketParams, message: ProtocolProxyMessage) -> bool | Greenlet:
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
        while not self._stop:
            try:
                readable, writable, exceptional = select.select(self.inbounds, self.outbounds,
                                                                self.inbounds | self.outbounds, timeout=0.1)
            except (OSError, Exception) as e:
                _log.warning(f"{self.proxy_name}: An error occurred in select loop: {e}")
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
            received = s.recv(2)
            if len(received) == 0:
                _log.warning(f'{self.proxy_name} received closed socket from ({s.getpeername()}.')
                return None
            if not (protocol := self.PROTOCOL_VERSION.get(struct.unpack('>H', received)[0])):
                raise NotImplementedError(f'Unknown protocol version ({protocol.VERSION})'
                                          f' received from: {s.getpeername()}')
            header_bytes = s.recv(protocol.HEADER_LENGTH)
            if len(header_bytes) == protocol.HEADER_LENGTH:
                return protocol.unpack(header_bytes)
            else:
                _log.warning(f'Failed to read headers. Received {len(header_bytes)} bytes: {header_bytes}')
        except (OSError, Exception) as e:
            _log.warning(f'{self.proxy_name}: Socket exception reading headers: {e}')

    def _receive_socket(self, s: socket):
        _log.debug(f'{self.proxy_name}: IN RECEIVE SOCKET')
        headers = self._receive_headers(s)
        if headers is not None and (cb_info := self.callbacks.get(headers.method_name)):
            remaining = headers.data_length
            buffer = b''
            done = False
            while not done:  # TODO: This should not go on forever.
                try:
                    while chunk := s.recv(read_length := max(0, remaining if remaining < self.chunk_size else self.chunk_size)):
                        buffer += chunk
                        remaining -= read_length
                        # TODO: Should we sleep in this loop?
                    result = spawn(cb_info.method, self, headers, buffer)
                    if cb_info.provides_response:
                        self.outbound_messages[s] = ProtocolProxyMessage(method_name='RESPONSE', payload=result,
                                                                         request_id=headers.request_id)
                        self.outbounds.add(s)
                except BlockingIOError as e:
                    _log.info(f'BlockingIOError: {e}')
                    sleep(0.1)
                except (OSError, Exception) as e:
                    _log.warning(f'{self.proxy_name}: Socket exception reading payload: {e}')
                    s.close()
                    done = True
                else:
                    if not cb_info.provides_response:
                        s.shutdown(SHUT_RDWR)
                        s.close()
                    done = True

    def _send_headers(self, s: socket, data_length: int, request_id: int, response_expected: bool, method_name: str,
                      protocol_version: int = 1):
        if not (protocol := self.PROTOCOL_VERSION.get(protocol_version)):
            raise NotImplementedError(f'Unable to send with unknown proxy protocol version: {protocol_version}')
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
            payload = message.payload.get() if isinstance(message.payload, Greenlet) else message.payload
            self._send_headers(s, len(payload), message.request_id, message.response_expected, message.method_name)
            try:
                _log.debug('REACHED SENDALL IN GEVENT IPC SEND')
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

    def start(self, *_, **__):
        self._setup_inbound_server(self.inbound_params)
        _log.debug(f'{self.proxy_name} STARTED.')

    def stop(self):
        self._stop = True

