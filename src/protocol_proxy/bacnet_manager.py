import asyncio
import json
import logging

from bacpypes3.primitivedata import ObjectIdentifier, ObjectType

from protocol_proxy.manager.asyncio import AsyncioProtocolProxyManager
from .bacnet_proxy import BACnetProxy
from protocol_proxy.ipc import ProtocolProxyMessage

logging.basicConfig(filename='protoproxy.log', level=logging.DEBUG,
                    format='%(asctime)s - %(message)s')
_log = logging.getLogger(__name__)


class AsyncioBACnetManager:
    def __init__(self, local_device_address):
        self.ppm = AsyncioProtocolProxyManager.get_manager(BACnetProxy)
        self.local_device_address = local_device_address

    async def run(self):
        await self.ppm.start()
        _log.debug(f'BACMan: Proxy Manager started, about to get proxy.')
        await self.ppm.get_proxy((self.local_device_address, 0), local_device_address=self.local_device_address)
        _log.debug(f'BACMan: Proxy Manager started, now running main loop and select loop.')
        await asyncio.gather(self.main_loop(), self.ppm.select_loop())

    async def main_loop(self):
        while not getattr(self.ppm, '_stop', False):
            await asyncio.sleep(10)
            _log.debug(f'BACMan: IN MAIN LOOP')
            # TODO: Remove or guard the following block to avoid unsolicited BACnet queries before proxy is ready.
            # proxy_id = self.ppm.get_proxy_id((self.local_device_address, 0))
            # peer = self.ppm.peers.get(proxy_id)
            # if not peer or not peer.socket_params:
            #     _log.warning(f'Proxy {proxy_id} not registered or missing socket_params, skipping iteration.')
            #     continue
            # result = await self.ppm.send(peer.socket_params,
            #              ProtocolProxyMessage(
            #                  method_name='QUERY_DEVICE',
            #                  payload=json.dumps({'address': '192.168.1.101'}).encode('utf8'),
            #                 response_expected=True
            #              ))
            # if isinstance(result, asyncio.Future):
            #     result = await result
            # if isinstance(result, bytes):
            #     result = json.loads(result.decode('utf8'))
            # elif isinstance(result, str):
            #     result = json.loads(result)
            # elif result is None:
            #     continue
            # else:
            #     continue
            # device_id = ObjectIdentifier(tuple(result))

            # result2 = await self.ppm.send(peer.socket_params,
            #                           ProtocolProxyMessage(
            #                               method_name='READ_PROPERTY',
            #                               payload=json.dumps({
            #                                   'device_address': '192.168.1.101',
            #                                   'object_identifier': str(device_id),
            #                                   'property_identifier': 'object-list'
            #                               }).encode('utf8'),
            #                               response_expected=True
            #                           )
            #                         )
            # if isinstance(result2, asyncio.Future):
            #     result2 = await result2
            # if isinstance(result2, bytes):
            #     obj_list = json.loads(result2.decode('utf8'))
            # elif isinstance(result2, str):
            #     obj_list = json.loads(result2)
            # elif result2 is None:
            #     continue
            # else:
            #     continue
            #
            # The above block is commented out to prevent premature or repeated BACnet queries.
            # Ensure BACnet operations only occur after proxy is fully initialized with the correct address.
