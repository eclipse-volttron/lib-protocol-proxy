import sys

from argparse import ArgumentParser
from gevent import joinall, sleep, spawn
from typing import Type
from uuid import UUID

from .proxy import ProtocolProxy
from .launch import launch


#### Dummy Proxy for testing.
class ConcreteProxy(ProtocolProxy):
    def __init__(self, manager_address: str, manager_port: int, manager_token: UUID, token: UUID,
                 unique_remote_id: tuple, *args, **kwargs):
        super(ConcreteProxy, self).__init__(manager_address, manager_port, manager_token, token, unique_remote_id,
                                            *args, **kwargs)
        joinall([spawn(self.main_loop), spawn(self.select_loop)])


    def main_loop(self):
        while True:
            sleep(0.01)

    @classmethod
    def get_proxy_id(cls, unique_remote_id):
        return unique_remote_id


def launch_concrete(parser: ArgumentParser) -> (ArgumentParser, Type[ProtocolProxy]):
    parser.add_argument('--concrete-address', type=str, default='192.168.1.100',
                                 help='Address of the Concrete service.')
    parser.add_argument('--concrete-port', type=int, default=22701,
                                 help='Port of the Concrete Service')
    parser.add_argument('--test-message', type=str, default='Wowzers!',
                                 help='Thing to print for testing.')
    return parser, ConcreteProxy

if __name__ == '__main__':
    sys.exit(launch(launch_concrete))