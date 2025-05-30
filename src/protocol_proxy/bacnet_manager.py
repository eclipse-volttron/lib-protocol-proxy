import json
import logging

from gevent import joinall, sleep, spawn

from .ipc import ProtocolProxyMessage
from .manager import ProtocolProxyManager
from .bacnet_proxy import BACnetProxy

logging.basicConfig(filename='protoproxy.log', level=logging.DEBUG,
                    format='%(asctime)s - %(message)s')
_log = logging.getLogger(__name__)


class BACnetManager:
    def __init__(self):
        self.ppm = ProtocolProxyManager.get_manager(BACnetProxy)

    def run(self):
        self.ppm.start()
        self.ppm.get_proxy(('172.29.115.24', 0), local_device_address='172.29.115.24')
        joinall([spawn(self.main_loop), spawn(self.ppm.select_loop)])

    def main_loop(self):
        while not self.ppm._stop:
           sleep(60)
           _log.debug(f'BACMan: IN MAIN LOOP')
           proxy_id = self.ppm.get_proxy_id(('172.29.115.24', 0))
           _log.debug(f'BACMan: CALLING SEND TO proxy_id: {proxy_id}. Peers has: {self.ppm.peers[proxy_id]}')
           self.ppm.send(self.ppm.peers[proxy_id].socket_params,
                         ProtocolProxyMessage(
                             method_name='QUERY_DEVICE',
                             payload=json.dumps({'address': '130.20.24.157'
                                                }).encode('utf8'),
                            response_expected=True
                         ))

