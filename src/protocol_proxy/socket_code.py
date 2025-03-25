import struct

from gevent import select
from gevent import socket
from gevent.queue import Empty, Queue

class FooSock:
    def __init__(self, listen_address, listen_port, chunk_size: int = 1024):
        self.chunk_size: int = chunk_size

        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setblocking(False)
        self.server.bind((listen_address, listen_port))
        self.server.listen(5)

        self.inputs = {self.server}     # Sockets from which we expect to read
        self.outputs = set()            # Sockets to which we expect to write
        self.message_queues = {}

        self.stop = False

    def select_loop(self):
        # TODO: Should this be in a separate greenlet/coroutine?
        while not self.stop:
            readable, writable, exceptional = select.select(self.inputs, self.outputs, self.inputs | self.outputs)
            for s in readable:          # Handle incoming sockets.
                if s is self.server:    # The server socket is ready to accept a connection
                    client_socket, client_address = s.accept()
                    client_socket.setblocking(0)
                    self.inputs.add(client_socket)
                else:
                    message = self._receive_socket(s, Queue())
                    # TODO: Need to make a callback with message.
            for s in writable:          # Handle outgoing sockets.
                self._send_socket(s, self.message_queues[s])
            for s in exceptional:       # Handle "exceptional conditions"
                self._handle_exceptional_socket(s)

    @staticmethod
    def send_length(s: socket, length: int):
        length_bytes = struct.pack(">Q", length)
        s.send(length_bytes)

    @staticmethod
    def receive_length(s: socket) -> int:
        length_bytes = s.recv(8)
        if not len(length_bytes) == 8:
            return 0  # Failed to read
        else:
            return struct.unpack(">Q", length_bytes)[0]

    def _receive_socket(self, s: socket, message_queue: Queue):
        remaining = self.receive_length(s)
        while buffer := s.recv(read_length := max(0, remaining if remaining < self.chunk_size else self.chunk_size)):
            message_queue.put(buffer)
            remaining -= read_length
        self.inputs.discard(s)
        s.close()
        return message_queue

    def _send_socket(self, s: socket, message_queue: Queue):
        try:
            message = message_queue.get_nowait()
        except Empty:                   # No messages waiting.
            self.outputs.discard(s)
        else:
            self.send_length(s, len(message))
            s.sendall(message)

    def _handle_exceptional_socket(self, s: socket):
        # TODO: We could probably use some logging here.
        self.inputs.discard(s)
        self.outputs.discard(s)
        s.shutdown(socket.SHUT_RDWR)
        s.close()
        del self.message_queues[s]
