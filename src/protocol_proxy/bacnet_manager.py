import asyncio
import json
import logging

from bacpypes3.primitivedata import ObjectIdentifier, ObjectType

from .ipc import ProtocolProxyMessage
from protocol_proxy.manager_asyncio import AsyncioProtocolProxyManager
from .bacnet_proxy import BACnetProxy

logging.basicConfig(filename='protoproxy.log', level=logging.DEBUG,
                    format='%(asctime)s - %(message)s')
_log = logging.getLogger(__name__)


class AsyncioBACnetManager:
    def __init__(self, local_device_address):
        self.ppm = AsyncioProtocolProxyManager.get_manager(BACnetProxy)
        self.local_device_address = local_device_address

    async def run(self):
        await self.ppm.start()
        await self.ppm.get_proxy((self.local_device_address, 0), local_device_address=self.local_device_address)
        await asyncio.gather(self.main_loop(), self.ppm.select_loop())

    async def main_loop(self):
        while not getattr(self.ppm, '_stop', False):
            await asyncio.sleep(10)
            _log.debug(f'BACMan: IN MAIN LOOP')
            proxy_id = self.ppm.get_proxy_id((self.local_device_address, 0))
            peer = self.ppm.peers.get(proxy_id)
            if not peer or not peer.socket_params:
                _log.warning(f'Proxy {proxy_id} not registered or missing socket_params, skipping iteration.')
                continue
            result = await self.ppm.send(peer.socket_params,
                         ProtocolProxyMessage(
                             method_name='QUERY_DEVICE',
                             payload=json.dumps({'address': '130.20.24.157'}).encode('utf8'),
                            response_expected=True
                         ))
            if result:
                result = json.loads(result.decode('utf8'))
                device_id = ObjectIdentifier(tuple(result))

                _log.debug(f'BACMan: The remote device has ID: {device_id}\n')

                result2 = await self.ppm.send(peer.socket_params,
                                      ProtocolProxyMessage(
                                          method_name='READ_PROPERTY',
                                          payload=json.dumps({
                                              'device_address': '130.20.24.157',
                                              'object_identifier': str(device_id),
                                              'property_identifier': 'object-list'
                                          }).encode('utf8'),
                                          response_expected=True
                                      )
                                    )
                _log.debug('The object list is: ')
                _log.debug([ObjectIdentifier(tuple(r)) for r in json.loads(result2.decode('utf8'))])
                _log.debug('\n\n')
