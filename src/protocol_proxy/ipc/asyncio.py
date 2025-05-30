import asyncio
import logging

from asyncio import BufferedProtocol, Future, Transport
from asyncio.base_events import Server
from typing import Awaitable
from weakref import WeakValueDictionary

from . import callback, IPCConnector, ProtocolHeaders, ProtocolProxyMessage, SocketParams

logging.basicConfig(filename='protoproxy.log', level=logging.DEBUG,
                    format='%(asctime)s - %(message)s')
_log = logging.getLogger(__name__)


class InboundIPCProtocol(BufferedProtocol):
    def __init__(self, connector: IPCConnector, on_lost_connection, buffer_size=32768, minimum_read_size: int = 76,
                 outgoing_message=None, protocol_version: int = 1):
        _log.debug(f'{self.connector.proxy_name} INBOUND AIPC PROTOCOL: IN PROTOCOL INIT')
        self.buffer_size = buffer_size
        self.connector: IPCConnector = connector
        self.minimum_read_size = minimum_read_size  # TODO: Default is V1 header length. Is this appropriate?
        self.outgoing_message = outgoing_message
        self.on_con_lost = on_lost_connection
        self.protocol_version: int = protocol_version

        self.loop = asyncio.get_running_loop()
        self.received_data: memoryview = memoryview(bytearray(buffer_size))  # TODO: Set default buffer to 32k. Is this appropriate?
                                                                #  Can we track the utilization and adjust if full or never used?
        self.head, self.tail, self.count = 0, 0, 0
        self.protocol = self.connector.PROTOCOL_VERSION.get(self.protocol_version)
        if not self.protocol:  # TODO: There should probably be a max protocol exchange in initial handshake. Better than checking protocol of every message?
            raise NotImplementedError(f'Unable to send with unknown proxy protocol version: {self.protocol_version}')
        self.header_length = self.protocol.HEADER_LENGTH + 2
        self.transport: Transport | None = None
        _log.debug(f'{self.connector.proxy_name} INBOUND AIPC PROTOCOL: END OF PROTOCOL INIT')


    def data_received(self, data):
        _log.debug(f'{self.connector.proxy_name} RECEIVED DATA: {data}')

    def buffer_updated(self, n_bytes: int) -> None:
        # _log.debug(f'{self.connector.proxy_name} {self.proxy_name}: IN BUFFER_UPDATED')
        # TODO: This is probably missing error handling and may not close the transport if it errs?
        if not self.transport:
            _log.warning('Unable to locate transport for received buffer.')
        self.tail = (self.tail + n_bytes) % self.buffer_size
        self.count += n_bytes
        _log.debug(f'{self.connector.proxy_name} GOT {n_bytes} BYTES, COUNT IS NOW: {self.count}')
        # TODO: This needs to be replaced with a read FROM received data using head and tail positions.

        # TODO: How to mitigate the possibility of an overflow?
        # TODO: This block tested the version of each incoming frame.
        #  Is this better or worse than assuming version number stays the same?
        # version_end = 2
        # if len(self.received_data) > version_end:
        #     if not (protocol := self.connector.PROTOCOL_VERSION.get(struct.unpack('>H', self.received_data[:2])[0])):
        #         raise NotImplementedError(f'Unknown protocol version ({protocol.VERSION})'
        #                                   f' received from: {self.transport.get_extra_info("peername")}')
        #     header_end = version_end + protocol.HEADER_LENGTH

        if self.count >= self.header_length:
            header_end = self.head + self.header_length
            _log.debug(f'{self.connector.proxy_name} READING BUFFER (FOR HEADERS): [{self.head+2}:{header_end}]')
            header_bytes = self.received_data[self.head+2:header_end]
            headers = self.protocol.unpack(header_bytes)  # TODO: Should this be in try block?
            _log.debug(f'{self.connector.proxy_name} PROTOCOL -- HEADERS: {headers}')
            message_end = header_end + headers.data_length
            if self.head + self.count >= message_end:
                _log.debug(f'{self.connector.proxy_name} PROTOCOL -- Enough bytes received.')
                # TODO: This same wrapping logic is needed for reading headers too! Break into helper function.
                if not message_end < self.buffer_size:
                    message_end = message_end % self.buffer_size
                    data = self.received_data[header_end:self.buffer_size] + self.received_data[0:message_end]
                else:
                    _log.debug(f'{self.connector.proxy_name} READING BUFFER (FOR DATA): [{header_end}:{message_end}]')
                    data = self.received_data[header_end:message_end]
                _log.debug(f'{self.connector.proxy_name} {self.connector.proxy_name} PROTOCOL -- DATA: {data.tobytes()}')
                self.head = message_end
                self.count -= 2 + self.header_length + headers.data_length
                if cb_info := self.connector.callbacks.get(headers.method_name):
                    _log.debug(f'{self.connector.proxy_name} PROTOCOL -- CALLBACK IS: {cb_info}')
                    result = self.loop.create_task(cb_info.method(self.connector, headers, data.tobytes()))
                    if cb_info.provides_response:
                        _log.debug(f'{self.connector.proxy_name} PROTOCOL -- Callback should provide response')
                        self.transport.write(ProtocolProxyMessage(method_name='RESPONSE', payload=result,
                                                                  request_id=headers.request_id))
                        _log.debug(f'{self.connector.proxy_name} PROTOCOL -- Response written')
                    else:
                        _log.debug(f'{self.connector.proxy_name} PROTOCOL -- Callback should not provide response')

    def connection_made(self, transport: Transport):
        _log.debug(f'{self.connector.proxy_name} INBOUND AIPC PROTOCOL: IN CONNECTION_MADE')
        self.transport = transport
        if self.outgoing_message:
            _log.debug(f'{self.connector.proxy_name} INBOUND AIPC PROTOCOL: THERE IS AN OUTGOING MESSAGE.')
            if isinstance(self.outgoing_message.payload, Awaitable):
                _log.debug(f'{self.connector.proxy_name} INBOUND AIPC PROTOCOL: PAYLOAD IS A FUTURE.')
                payload = asyncio.ensure_future(self.outgoing_message.payload)
            else:
                _log.debug(f'{self.connector.proxy_name} INBOUND AIPC PROTOCOL: PAYLOAD IS NOT A FUTURE.')
                payload = self.outgoing_message.payload
            message_bytes = bytearray(self.protocol(len(payload), self.outgoing_message.method_name,
                                               self.outgoing_message.request_id, self.connector.proxy_id,
                                               self.connector.token, self.outgoing_message.response_expected).pack())
            message_bytes.extend(payload)  # TODO: Does payload still need to be encoded?
            _log.debug(f'{self.connector.proxy_name} ASYNCIO_IPC: ABOUT TO WRITE.')
            transport.write(message_bytes)  # TODO: Should we send in chunks and sleep in between?
            if not self.outgoing_message.response_expected:
                transport.close()
            _log.debug(f'{self.connector.proxy_name} ASYNCIO_IPC: WRITTEN')

    def connection_lost(self, exc):
        _log.debug(f'{self.connector.proxy_name} The server closed the connection. exc is: {exc}')
        _log.debug(f'{self.connector.proxy_name} Inbound server is serving: {self.connector.inbound_server.is_serving()}')
        _log.debug(f'{self.connector.proxy_name} Indbound server has socket: {self.connector.inbound_server.sockets[0]}')
        self.on_con_lost.set_result(True)

    def get_buffer(self, size_hint):
        _log.debug(f'{self.connector.proxy_name} GET BUFFER IS PROVIDING [{self.tail}:{min(max(size_hint, self.minimum_read_size), self.buffer_size)}]')
        return self.received_data[self.tail:min(max(size_hint, self.minimum_read_size), self.buffer_size)]

    def eof_received(self) -> bool | None:
        _log.debug("GOT EOF")
        _log.debug(f'{self.connector.proxy_name} IN EOF, TRANSPORT IS CLOSING: {self.transport.is_closing()}')
        return False

class OutboundIPCProtocol(BufferedProtocol):
    def __init__(self, connector: IPCConnector, on_lost_connection, buffer_size=32768, minimum_read_size: int = 76,
                 outgoing_message=None, protocol_version: int = 1):
        _log.debug(f'{connector.proxy_name} OUTBOUND AIPC PROTOCOL: IN PROTOCOL INIT')
        self.buffer_size = buffer_size
        self.connector: IPCConnector = connector
        self.minimum_read_size = minimum_read_size  # TODO: Default is V1 header length. Is this appropriate?
        self.outgoing_message = outgoing_message
        self.on_con_lost = on_lost_connection
        self.protocol_version: int = protocol_version

        self.loop = asyncio.get_running_loop()
        self.received_data: memoryview = memoryview(bytearray(buffer_size))  # TODO: Set default buffer to 32k. Is this appropriate?
                                                                #  Can we track the utilization and adjust if full or never used?
        self.head, self.tail, self.count = 0, 0, 0
        self.protocol = self.connector.PROTOCOL_VERSION.get(self.protocol_version)
        if not self.protocol:  # TODO: There should probably be a max protocol exchange in initial handshake. Better than checking protocol of every message?
            raise NotImplementedError(f'Unable to send with unknown proxy protocol version: {self.protocol_version}')
        self.header_length = self.protocol.HEADER_LENGTH + 2
        self.transport: Transport | None = None
        _log.debug(f'{self.connector.proxy_name} OUTBOUND AIPC PROTOCOL: END OF PROTOCOL INIT')


    def data_received(self, data):
        _log.debug(f'{self.connector.proxy_name} RECEIVED DATA: {data}')

    def buffer_updated(self, n_bytes: int) -> None:
        # _log.debug(f'{self.connector.proxy_name} {self.proxy_name}: IN BUFFER_UPDATED')
        # TODO: This is probably missing error handling and may not close the transport if it errs?
        if not self.transport:
            _log.warning('Unable to locate transport for received buffer.')
        self.tail = (self.tail + n_bytes) % self.buffer_size
        self.count += n_bytes
        _log.debug(f'{self.connector.proxy_name} GOT {n_bytes} BYTES, COUNT IS NOW: {self.count}')
        # TODO: This needs to be replaced with a read FROM received data using head and tail positions.

        # TODO: How to mitigate the possibility of an overflow?
        # TODO: This block tested the version of each incoming frame.
        #  Is this better or worse than assuming version number stays the same?
        # version_end = 2
        # if len(self.received_data) > version_end:
        #     if not (protocol := self.connector.PROTOCOL_VERSION.get(struct.unpack('>H', self.received_data[:2])[0])):
        #         raise NotImplementedError(f'Unknown protocol version ({protocol.VERSION})'
        #                                   f' received from: {self.transport.get_extra_info("peername")}')
        #     header_end = version_end + protocol.HEADER_LENGTH

        if self.count >= self.header_length:
            header_end = self.head + self.header_length
            _log.debug(f'READING BUFFER (FOR HEADERS): [{self.head+2}:{header_end}]')
            header_bytes = self.received_data[self.head+2:header_end]
            headers = self.protocol.unpack(header_bytes)  # TODO: Should this be in try block?
            _log.debug(f'{self.connector.proxy_name} PROTOCOL -- HEADERS: {headers}')
            message_end = header_end + headers.data_length
            if self.head + self.count >= message_end:
                _log.debug(f'{self.connector.proxy_name} PROTOCOL -- Enough bytes received.')
                # TODO: This same wrapping logic is needed for reading headers too! Break into helper function.
                if not message_end < self.buffer_size:
                    message_end = message_end % self.buffer_size
                    data = self.received_data[header_end:self.buffer_size] + self.received_data[0:message_end]
                else:
                    _log.debug(f'{self.connector.proxy_name} READING BUFFER (FOR DATA): [{header_end}:{message_end}]')
                    data = self.received_data[header_end:message_end]
                _log.debug(f'{self.connector.proxy_name} PROTOCOL -- DATA: {data.tobytes()}')
                self.head = message_end
                self.count -= 2 + self.header_length + headers.data_length
                if cb_info := self.connector.callbacks.get(headers.method_name):
                    _log.debug(f'{self.connector.proxy_name} PROTOCOL -- CALLBACK IS: {cb_info}')
                    result = self.loop.create_task(cb_info.method(self.connector, headers, data.tobytes()))
                    if cb_info.provides_response:
                        _log.debug(f'{self.connector.proxy_name} PROTOCOL -- Callback should provide response')
                        self.transport.write(ProtocolProxyMessage(method_name='RESPONSE', payload=result,
                                                                  request_id=headers.request_id))
                        _log.debug(f'{self.connector.proxy_name} PROTOCOL -- Response written')
                    else:
                        _log.debug(f'{self.connector.proxy_name} PROTOCOL -- Callback should not provide response')

    def connection_made(self, transport: Transport):
        _log.debug(f'{self.connector.proxy_name} OUTBOUND AIPC PROTOCOL: IN CONNECTION_MADE')
        self.transport = transport
        if self.outgoing_message:
            _log.debug(f'{self.connector.proxy_name} OUTBOUND AIPC PROTOCOL: THERE IS AN OUTGOING MESSAGE.')
            if isinstance(self.outgoing_message.payload, Awaitable):
                _log.debug(f'{self.connector.proxy_name} OUTBOUND AIPC PROTOCOL: PAYLOAD IS A FUTURE.')
                payload = asyncio.ensure_future(self.outgoing_message.payload)
            else:
                _log.debug(f'{self.connector.proxy_name} OUTBOUND AIPC PROTOCOL: PAYLOAD IS NOT A FUTURE.')
                payload = self.outgoing_message.payload
            message_bytes = bytearray(self.protocol(len(payload), self.outgoing_message.method_name,
                                               self.outgoing_message.request_id, self.connector.proxy_id,
                                               self.connector.token, self.outgoing_message.response_expected).pack())
            message_bytes.extend(payload)  # TODO: Does payload still need to be encoded?
            _log.debug(f'{self.connector.proxy_name} ASYNCIO_IPC: ABOUT TO WRITE.')
            transport.write(message_bytes)  # TODO: Should we send in chunks and sleep in between?
            if not self.outgoing_message.response_expected:
                transport.close()
            _log.debug(f'{self.connector.proxy_name} ASYNCIO_IPC: WRITTEN')

    def connection_lost(self, exc):
        _log.debug(f'{self.connector.proxy_name} The server closed the connection. exc is: {exc}')
        _log.debug(f'{self.connector.proxy_name} Inbound server is serving: {self.connector.inbound_server.is_serving()}')
        _log.debug(f'{self.connector.proxy_name} Indbound server has socket: {self.connector.inbound_server.sockets[0]}')
        self.on_con_lost.set_result(True)

    def get_buffer(self, size_hint):
        _log.debug(f'{self.connector.proxy_name} GET BUFFER IS PROVIDING [{self.tail}:{min(max(size_hint, self.minimum_read_size), self.buffer_size)}]')
        return self.received_data[self.tail:min(max(size_hint, self.minimum_read_size), self.buffer_size)]

    def eof_received(self) -> bool | None:
        _log.debug("GOT EOF")
        _log.debug(f'{self.connector.proxy_name} IN EOF, TRANSPORT IS CLOSING: {self.transport.is_closing()}')
        return False


class AsyncioIPCConnector(IPCConnector):
    def __init__(self, proxy_id, token, proxy_name: str = None, **kwargs):
        self.inbound_server: Server | None = None
        self.loop = asyncio.get_running_loop()
        super(AsyncioIPCConnector, self).__init__(proxy_id=proxy_id, token=token, proxy_name=proxy_name, **kwargs)
        self.response_results: WeakValueDictionary[int, Future] = WeakValueDictionary()

    async def _get_ip_addresses(self, host_name: str) -> set[str]:
        return {ai[4][0] for ai in await self.loop.getaddrinfo(host_name, None)}

    @callback
    async def _handle_response(self, headers: ProtocolHeaders, raw_message: bytes):
        result = self.response_results.get(headers.request_id)
        if not result:
            _log.warning(
                f'Received response {headers.request_id} from {headers.sender_id} containing "{raw_message.decode()}",'
                f' but result object is no longer available.')
        else:
            result.set_result(raw_message)

    async def send(self, remote: SocketParams, message: ProtocolProxyMessage) -> bool | Future:
        _log.debug(f'{self.proxy_name} ASYNCIO_IPC: IN SEND.')
        on_lost_connection = self.loop.create_future()
        if message.request_id is None:
            message.request_id = self.next_request_id
        if message.response_expected:
            result = self.loop.create_future()
            self.response_results[message.request_id] = result
        else:
            result = True
        connection_attempts = 5
        _log.debug(f'{self.proxy_name} ASYNCIO_IPC: IN SEND, BEFORE WHILE.')
        while connection_attempts:
            try:
                _log.debug(f'{self.proxy_name} ASYNCIO_IPC: IN SEND, CREATING CONNECTION TO: {remote}.')
                transport, _ = await self.loop.create_connection(
                    lambda: OutboundIPCProtocol(connector=self, on_lost_connection=on_lost_connection, outgoing_message=message),
                    *remote)
                _log.debug(f'{self.proxy_name} ASYNCIO_IPC: IN SEND, CONNECTION CREATED.')
                # Wait until the protocol signals that the connection is lost and close the transport.
                # await on_lost_connection
            except ConnectionError:
                connection_attempts -= 1
                await asyncio.sleep(1)
                _log.debug(f'{self.proxy_name} ASYNCIO_IPC: IN SEND, FAILED. {connection_attempts} ATTEMPTS REMAINING.')
                continue
            else:
                _log.debug(f'{self.proxy_name} ASYNCIO_IPC: IN SEND, ELSE.')
                break
            finally:
                _log.debug(f'{self.proxy_name} ASYNCIO_IPC: IN SEND, FINALLY.')
                pass
                # transport.close()
        return result

    async def _setup_inbound_server(self, socket_params: SocketParams = None):
        if socket_params:
            try:
                self.inbound_server = self.loop.create_server(InboundIPCProtocol, *socket_params, start_serving=True)
                return
            except (OSError, Exception) as e:
                _log.warning(f'Unable to bind to provided inbound socket {socket_params}. Trying next available. - {e}')
        else:
            socket_params = SocketParams()
        while True:
            try:
                next_port = next(self.unused_ports(await self._get_ip_addresses(socket_params.address)))
                self.inbound_server = await self.loop.create_server(InboundIPCProtocol, socket_params.address, next_port,
                                                                    start_serving=True)
                _log.debug(f'{self.proxy_name} AFTER START SERVING. Server is: {self.inbound_server}')
                break
            except OSError:
                continue
            except StopIteration:
                _log.error(f'Unable to bind inbound socket to {socket_params.address}'
                           f' on any port in range: {self.min_port} - {self.max_port}.')
                break

    async def start(self, *_, **__):
        await self._setup_inbound_server(self.inbound_params)
        _log.debug(f' {self.proxy_name} STARTED with INBOUND PARAMS SENT AS: {self.inbound_params}.')

    async def stop(self):
        pass
        # self.inbound_server.close()
        # await self.inbound_server.wait_closed()
