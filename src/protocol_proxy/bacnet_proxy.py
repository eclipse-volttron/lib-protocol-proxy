import asyncio
import logging
import sys

from argparse import ArgumentParser
from math import floor
from typing import Type

from bacpypes3.app import Application
from bacpypes3.constructeddata import AnyAtomic
from bacpypes3.pdu import Address, PDUData
from bacpypes3.apdu import (ConfirmedPrivateTransferACK, ConfirmedPrivateTransferError, ConfirmedPrivateTransferRequest,
                            ErrorRejectAbortNack)
from bacpypes3.primitivedata import ClosingTag, Null, ObjectIdentifier, ObjectType, OpeningTag, Tag, TagList
from bacpypes3.vendor import get_vendor_info

from .proxy import launch
from .proxy.asyncio import AsyncioProtocolProxy

logging.basicConfig(filename='protoproxy.log', level=logging.DEBUG,
                    format='%(asctime)s - %(message)s')
_log = logging.getLogger(__name__)

class BACnetProxy(AsyncioProtocolProxy):
    def __init__(self, local_device_address, bacnet_network=0, vendor_id=999, object_name='VOLTTRON BACnet Proxy',
                 **kwargs):
        _log.debug('WELCOME BAC')
        _log.debug('BACNET_PROXY: BEFORE SUPER.')
        super(BACnetProxy, self).__init__(**kwargs)  # TODO: Where should super call really be?
        _log.debug('BACNET_PROXY: AFTER SUPER.')
        vendor_info = get_vendor_info(vendor_id)
        device_object_class = vendor_info.get_object_class(ObjectType.device)
        device_object = device_object_class(objectIdentifier=('device', vendor_id), objectName=object_name)
        network_port_object_class = vendor_info.get_object_class(ObjectType.networkPort)
        network_port_object = network_port_object_class(local_device_address,
                                                        objectIdentifier=("network-port", bacnet_network),
                                                        objectName="NetworkPort-1", networkNumber=bacnet_network,
                                                        networkNumberQuality="configured")
        self.loop = asyncio.get_event_loop()
        self.app = Application.from_object_list(
            [device_object, network_port_object],
            device_info_cache=None,  # TODO: If these should be passed in, add to args & launch.
            router_info_cache=None,
            aseID=None
        )
        _log.debug('BACNET PROXY: END OF INIT.')


    @classmethod
    def get_unique_remote_id(cls, unique_remote_id: tuple) -> tuple:
        """Get a unique identifier for the proxy server
         given a unique_remote_id and protocol-specific set of parameters."""
        return unique_remote_id[0:2]  # TODO: How can we know what the first two params really are?
                                      #  (Ideally they are address and port.)
                                      #  Consider named tuple?

    def query_device(self, address: str, property_name: str = 'object-identifier'):
        """Returns properties about the device at the given address.
            If a different property name is not given, this will be the object-id.
            This function allows unicast discovery.
        """
        # TODO: This can get everything from device if it is using read_property_multiple and ALL
        return self.read_property(address, 'device:4194303', property_name)

    async def read_property(self, device_address: str, object_identifier: str, property_identifier: str,
                   property_array_index: int | None = None):
        try:
            response = await self.app.read_property(
                Address(device_address),
                ObjectIdentifier(object_identifier),
                property_identifier,
                int(property_array_index) if property_array_index is not None else None
            )
        except ErrorRejectAbortNack as err:
            response = err
        if isinstance(response, AnyAtomic):
            response = response.get_value()
        return response

    async def write_property(self, device_address: str, object_identifier: str, property_identifier: str, value: any,
                    priority: int, property_array_index: int | None = None):
        value = Null(()) if value is None else value
        # TODO: Is additional casting required?
        try:
            return await self.app.write_property(
                Address(device_address),
                ObjectIdentifier(object_identifier),
                property_identifier,
                value,
                int(property_array_index) if property_array_index is not None else None,
                int(priority)
            )
        except ErrorRejectAbortNack as e:
            print(str(e))

    async def confirmed_private_transfer(self, address: Address, vendor_id: int, service_number: int,
                                         service_parameters: TagList = None) -> any:
        # TODO: Probably need one or more try blocks.
        # TODO: service_parameters probably needs to already be formatted, but how?
        cpt_request = ConfirmedPrivateTransferRequest(destination=address,
                                                      vendorID=vendor_id,
                                                      serviceNumber=service_number)
        if service_parameters:
            cpt_request.serviceParameters = service_parameters
        response = await self.app.request(cpt_request)
        if isinstance(response, ConfirmedPrivateTransferError):
            _log.warning(f'Error calling Confirmed Private Transfer Service: {response}')
        elif isinstance(response, ConfirmedPrivateTransferACK):
            return response
        else:
            _log.warning(f'Some other Error: {response}')  # TODO: Improve error handling.

    async def send_object_user_lock_time(self, address: Address, device_id: str, object_id: str,
                                         lock_interval: int):
        if lock_interval < 0:
            lock_interval_code = 0xFF
            lock_interval = 0
        elif lock_interval <= 60:
            lock_interval_code = 0
            lock_interval = floor(lock_interval)
        elif lock_interval <= 3600:
            lock_interval_code = 1
            lock_interval = floor(lock_interval / 60)
        elif lock_interval <= 86400:
            lock_interval_code = 2
            lock_interval = floor(lock_interval / 3600)
        elif lock_interval <= 22032000:
            lock_interval_code = 3
            lock_interval = floor(lock_interval / 86400)
        else:
            lock_interval_code = 0xFF
            lock_interval = 0
        response = await self.confirmed_private_transfer(address=Address(address), vendor_id=213, service_number=28,
                                                         service_parameters=TagList([
                                                             OpeningTag(2),
                                                             ObjectIdentifier(device_id, _context=0).encode(),
                                                             ObjectIdentifier(object_id, _context=0).encode(),
                                                             ObjectUserLockTime(lock_interval_code, lock_interval),
                                                             ClosingTag(2)
                                                            ])
                                                         )
        return response  # TODO: Improve error handling.

class ObjectUserLockTime(Tag):
    def __init__(self, interval_code, interval_value, *args):
        super(ObjectUserLockTime, self).__init__(*args)
        self.interval_code: int = interval_code
        self.interval_value: int = interval_value

    def encode(self) -> PDUData:
        pdu_data = PDUData()
        pdu_data.put(self.interval_code)
        pdu_data.put(self.interval_value)
        return pdu_data

async def run_proxy(local_device_address, **kwargs):
    bp = BACnetProxy(local_device_address, **kwargs)
    await bp.start()

def launch_bacnet(parser: ArgumentParser) -> (ArgumentParser, Type[AsyncioProtocolProxy]):
    parser.add_argument('--local-device-address', type=str, required=True,
                        help='Address on the local machine of this BACnet Proxy.')
    parser.add_argument('--bacnet-network', type=int, default=0,
                        help='The BACnet port as an offset from 47808.')
    parser.add_argument('--vendor-id', type=int, default=999,
                        help='The BACnet vendor ID to use for the local device of this BACnet Proxy.')
    parser.add_argument('--object-name', type=str, default='VOLTTRON BACnet Proxy',
                        help='The name of the local device for this BACnet Proxy.')
    return parser, run_proxy

if __name__ == '__main__':
    sys.exit(launch(launch_bacnet))
