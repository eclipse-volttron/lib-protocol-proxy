import logging

from .headers import ProtocolHeaders

logging.basicConfig(filename='protoproxy.log', level=logging.DEBUG,
                    format='%(asctime)s - %(message)s')
_log = logging.getLogger(__name__)


def callback(func):
    def verify(self, ipc, headers: ProtocolHeaders, raw_message: any, async_result=None):
        #_log.debug(f'CALLBACK: ATTEMPTING TO VERIFY {headers.sender_id} AGAINST PEERS: {list(ipc.peers.keys())}')
        if peer := ipc.peers.get(headers.sender_id):
            if headers.sender_token == peer.token:
                result = func(self, headers, raw_message)
                if async_result:
                    async_result.set(result)
            else:
                _log.warning(f'Unable to authenticate caller: {headers.sender_id}')
        else:
            _log.warning(f'Request from unknown party: {headers.sender_id}')
    return verify