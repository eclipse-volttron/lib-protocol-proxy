import sys

from argparse import ArgumentParser
from uuid import UUID

from .proxy import ConcreteProxy


def proxy_command_parser(parser: ArgumentParser = None):
    parser = parser if parser else ArgumentParser()
    print('IN proxy_command_parser')
    parser.add_argument('--manager-address', type=str, default='localhost',
                        help='Address of the outbound socket to the Proxy Manager.')
    parser.add_argument('--manager-port', type=int, default=22801,
                        help='Port of the outbound socket to the Proxy Manager.')
    parser.add_argument('--encrypt', type=bool, default=False,
                        help='Whether to use encryption on the socket connections with the Manager.')
    parser.add_argument('--inbound-address', type=str, default='localhost',
                        help='Address of the inbound socket from the Proxy Manager')
    parser.add_argument('--inbound-port', type=int, default=22802,
                        help='Port of the inbound socket from the Proxy Manager')
    return parser

def concrete_command_parser(parser: ArgumentParser = None):
    parser = proxy_command_parser(parser if parser else ArgumentParser())
    print('in concrete_command_parser')
    subparsers = parser.add_subparsers()
    parser_concrete = subparsers.add_parser('concrete', help='Concrete-Specific Commands')
    parser_concrete.add_argument('--concrete-address', type=str, default='192.168.1.100',
                                 help='Address of the Concrete service.')
    parser_concrete.add_argument('--concrete-port', type=int, default=22701,
                                 help='Port of the Concrete Service')
    parser_concrete.add_argument('--test-message', type=str, default='Wowzers!',
                                 help='Thing to print for testing.')
    return parser

if __name__ == '__main__':
    print("LAUNCH: IN LAUNCH.MAIN")
    from pprint import pprint
    parser = concrete_command_parser()
    opts = parser.parse_args()
    pprint(opts)
    token = UUID(hex=sys.stdin.read(32))
    print(f"LAUNCH: TOKEN RECIEVED AS: {token}")
    sys.exit(ConcreteProxy(unique_remote_id=('foo',), token=token, **vars(opts)))
    # TODO: How to make this into something usable in various proxy modules?
