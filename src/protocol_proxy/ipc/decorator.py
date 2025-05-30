import logging

from . import ProtocolHeaders

logging.basicConfig(filename='protoproxy.log', level=logging.DEBUG,
                    format='%(asctime)s - %(message)s')
_log = logging.getLogger(__name__)


# TODO: Do we need an Asyncio version of this?
# TODO: Did this work with the AsyncResult removed (just returns, possibly within greenlet)?
def callback(func):
    def verify(self, ipc, headers, raw_message: any):
        #_log.debug(f'CALLBACK: ATTEMPTING TO VERIFY {headers.sender_id} AGAINST PEERS: {list(ipc.peers.keys())}')
        if peer := ipc.peers.get(headers.sender_id):
            if headers.sender_token == peer.token:
                return func(self, headers, raw_message)
            else:
                _log.warning(f'Unable to authenticate caller: {headers.sender_id}')
        else:
            _log.warning(f'Request from unknown party: {headers.sender_id}')
    return verify
