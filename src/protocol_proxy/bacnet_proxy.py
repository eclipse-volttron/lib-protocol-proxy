import asyncio
import logging

from datetime import timedelta
from typing import Sequence

from bacpypes3.app import Application
from bacpypes3.constructeddata import AnyAtomic
from bacpypes3.pdu import Address, PDUData
from bacpypes3.apdu import (ConfirmedPrivateTransferACK, ConfirmedPrivateTransferError, ConfirmedPrivateTransferRequest,
                            ErrorRejectAbortNack)
from bacpypes3.primitivedata import ClosingTag, ObjectIdentifier, ObjectType, OpeningTag, Tag, TagList, Unsigned
from bacpypes3.vendor import get_vendor_info

#from volttron.driver.base.proxy import ProtocolProxy
from volttron.client.logs import setup_logging

setup_logging()
_log = logging.getLogger(__name__)

class BACnetProxy: #ProtocolProxy):
    def __init__(self, local_address, bacnet_network=0, vendor_id=999, object_name='Excelsior',
                 device_info_cache=None, router_info_cache=None, ase_id=None):
        # device_object = DeviceObject(objectIdentifier=123, objectName='MyDevice')
        # device_info = DeviceInfo(device_object, address=Address('192.168.1.4/24'))
        #     self.app = Application(device_info, device_info.device_address)
        # self.app.bind()
        # await app.run()

        vendor_info = get_vendor_info(vendor_id)
        device_object_class = vendor_info.get_object_class(ObjectType.device)
        device_object = device_object_class(objectIdentifier=('device', vendor_id), objectName=object_name)
        network_port_object_class = vendor_info.get_object_class(ObjectType.networkPort)
        network_port_object = network_port_object_class(local_address, objectIdentifier=("network-port", bacnet_network),
                                                        objectName="NetworkPort-1", networkNumber=bacnet_network,
                                                        networkNumberQuality="configured")
        # continue the build process
        self.app = Application.from_object_list(
            [device_object, network_port_object],
            device_info_cache=device_info_cache,
            router_info_cache=router_info_cache,
            aseID=ase_id
        )
        super(BACnetProxy, self).__init__()  # TODO: Where should super call really be?

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
                                         lock_interval_code: int, lock_interval: int):
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

async def create_proxy(address):
    return BACnetProxy(address)

async def main():
    proxy = BACnetProxy('192.168.1.4/24')
    await proxy.write('2001:1', 'analogValue:44', 'presentValue', 80, 8)
    await proxy.read('2001:1', 'analogValue:44', 'presentValue')

if __name__ == "__main__":
    asyncio.run(main())

# def from_object_list(
#     cls,
#     objects: List[Object],
#     device_info_cache: Optional[DeviceInfoCache] = None,
#     router_info_cache: Optional[RouterInfoCache] = None,
#     aseID=None,
# ) -> Application:
#     """
#     Create an instance of an Application given a list of objects.
#     """
#     if _debug:
#         Application._debug(
#             "from_object_list %s device_info_cache=%r aseID=%r",
#             repr(objects),
#             device_info_cache,
#             aseID,
#         )
#
#     # find the device object
#     device_object = None
#     for obj in objects:
#         if not isinstance(obj, DeviceObject):
#             continue
#         if device_object is not None:
#             raise RuntimeError("duplicate device object")
#         device_object = obj
#     if device_object is None:
#         raise RuntimeError("missing device object")
#
#     # create a base instance
#     app = cls(device_info_cache=device_info_cache, aseID=aseID)
#
#     # a application service access point will be needed
#     app.asap = ApplicationServiceAccessPoint(device_object, app.device_info_cache)
#
#     # a network service access point will be needed
#     app.nsap = NetworkServiceAccessPoint(router_info_cache=router_info_cache)
#
#     # give the NSAP a generic network layer service element
#     app.nse = NetworkServiceElement()
#     bind(app.nse, app.nsap)
#
#     # bind the top layers
#     bind(app, app.asap, app.nsap)
#
#     # add the objects
#     for obj in objects:
#         app.add_object(obj)
#
#     # return the built application
#     return app
#
# def from_args(
#     cls,
#     loggers=False,
#     debug=[],
#     color=None,
#     route_aware=None,
#     name='Excelsior',
#     instance=999,
#     network=0,
#     address='192.168.1.4/24',
#     vendoridentifier=999,
#     foreign=None,
#     ttl=30,
#     bbmd=None,
#     device_address='2001:1',
#     object_identifier='analogValue:44',
#     property_identifier='presentValue',
#     value='76',
#     priority='8',
#     device_info_cache = None,
#     router_info_cache = None,
#     aseID=None
# ) -> Application:
#
#     # get the vendor info for the provided identifier
#     vendor_info = get_vendor_info(vendoridentifier)
#     if vendor_info.vendor_identifier == 0:
#         raise RuntimeError(f"missing vendor info: {vendoridentifier}")
#
#     # get the device object class and make an instance
#     device_object_class = vendor_info.get_object_class(ObjectType.device)
#     if not device_object_class:
#         raise RuntimeError(
#             f"vendor indentifier {vendoridentifier} missing device object class"
#         )
#
#     device_object = device_object_class(
#         objectIdentifier=("device", int(instance)), objectName=name
#     )
#
#     # get the network port object class and make an instance
#     network_port_object_class = vendor_info.get_object_class(ObjectType.networkPort)
#     if not network_port_object_class:
#         raise RuntimeError(
#             f"vendor indentifier {vendoridentifier} missing network port object class"
#         )
#
#     # default address is 'host' or 'host:0' for a foreign device
#     address = address
#     if not address:
#         address = "host:0" if foreign else "host"
#
#     # make a network port object
#     network_port_object = network_port_object_class(
#         address,
#         objectIdentifier=("network-port", 1),
#         objectName="NetworkPort-1",
#         networkNumber=network,
#         networkNumberQuality="configured" if network else "unknown",
#     )
#
#     # continue the build process
#     return cls.from_object_list(
#         [device_object, network_port_object],
#         device_info_cache=device_info_cache,
#         router_info_cache=router_info_cache,
#         aseID=aseID,
#     )

# import asyncio
# from bacpypes3.app import Application, DeviceInfo
# from bacpypes3.local.device import DeviceObject
# from bacpypes3.pdu import Address
# from bacpypes3.debugging import bacpypes_debugging, ModuleLogger
#
# _debug = 0
# _log = ModuleLogger(globals())
#
# @bacpypes_debugging
# class SampleApplication(Application):
#     def __init__(self, device, address):
#         if _debug: SampleApplication._debug("__init__ %r %r", device, address)
#         Application.__init__(self, device, address)
#
#     async def request(self, apdu):
#         if _debug: SampleApplication._debug("request %r", apdu)
#         await Application.request(self, apdu)
#
#     async def indication(self, apdu):
#         if _debug: SampleApplication._debug("indication %r", apdu)
#         await Application.indication(self, apdu)
#
#     async def response(self, apdu):
#         if _debug: SampleApplication._debug("response %r", apdu)
#         await Application.response(self, apdu)
#
#     async def confirmation(self, apdu):
#         if _debug: SampleApplication._debug("confirmation %r", apdu)
#         await Application.confirmation(self, apdu)
#
# async def main():
#     device_object = DeviceObject(objectIdentifier=123, objectName='MyDevice')
#     device_info = DeviceInfo(device_object, address=Address(1))
#     app = SampleApplication(device_info, address=device_info.address)
#     app.bind()
#
#     await app.run()


if __name__ == "__main__":
    asyncio.run(main())
