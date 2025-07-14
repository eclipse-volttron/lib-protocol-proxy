import asyncio
import json
import logging
import sys
from typing import Optional, Type # Added Type

from argparse import ArgumentParser # Added
from math import floor # Added

from bacpypes3.app import Application
from bacpypes3.constructeddata import AnyAtomic
from bacpypes3.pdu import Address, PDUData
from bacpypes3.apdu import (
    ConfirmedPrivateTransferACK, ConfirmedPrivateTransferError, ConfirmedPrivateTransferRequest,
    ErrorRejectAbortNack
)
from bacpypes3.primitivedata import ClosingTag, Null, ObjectIdentifier, ObjectType, OpeningTag, Tag, TagList
from bacpypes3.vendor import get_vendor_info

from .ipc.decorator import callback
from .proxy import launch
from .proxy.asyncio import AsyncioIPCConnector, AsyncioProtocolProxy

logging.basicConfig(filename='protoproxy.log', level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(name)s - %(module)s.%(funcName)s:%(lineno)d - %(message)s')
_log = logging.getLogger(__name__)

# Forward declaration for type hinting if BACnet class is defined later in the file
# class BACnet:
#     pass 

class BACnetProxy(AsyncioProtocolProxy):
    def __init__(self, local_device_address, bacnet_network=0, vendor_id=999, object_name='VOLTTRON BACnet Proxy',
                 **kwargs):
        super(BACnetProxy, self).__init__(**kwargs)
        # Ensure BACnet class is defined before this or properly imported
        self.bacnet = BACnet(local_device_address, bacnet_network, vendor_id, object_name, **kwargs)
        self.loop = asyncio.get_event_loop()

        self.register_callback(self.confirmed_private_transfer_endpoint, 'CONFIRMED_PRIVATE_TRANSFER', provides_response=True)
        self.register_callback(self.query_device_endpoint, 'QUERY_DEVICE', provides_response=True)
        self.register_callback(self.read_property_endpoint, 'READ_PROPERTY', provides_response=True)
        self.register_callback(self.send_object_user_lock_time_endpoint, 'SEND_OBJECT_USER_LOCK_TIME', provides_response=True)
        self.register_callback(self.write_property_endpoint, 'WRITE_PROPERTY', provides_response=True)
        self.register_callback(self.read_device_all_endpoint, 'READ_DEVICE_ALL', provides_response=True)
        self.register_callback(self.who_is_endpoint, 'WHO_IS', provides_response=True)
        self.register_callback(self.scan_subnet_endpoint, 'SCAN_SUBNET', provides_response=True)

    @callback
    async def confirmed_private_transfer_endpoint(self, _, raw_message: bytes):
        """Endpoint for confirmed private transfer."""
        message = json.loads(raw_message.decode('utf8'))
        address = Address(message['address'])
        vendor_id = message['vendor_id']
        service_number = message['service_number']
        # TODO: from_json may be an AI hallucination. Need to check this.
        service_parameters = TagList.from_json(message.get('service_parameters', []))
        result = await self.bacnet.confirmed_private_transfer(address, vendor_id, service_number, service_parameters)
        return json.dumps(result).encode('utf8')

    @callback
    async def query_device_endpoint(self, _, raw_message: bytes):
        """Endpoint for querying a device."""
        message = json.loads(raw_message.decode('utf8'))
        address = message['address']
        property_name = message.get('property_name', 'object-identifier')
        result = await self.bacnet.query_device(address, property_name)
        # Handle non-JSON-serializable BACnet error/abort responses
        try:
            from bacpypes3.apdu import ErrorRejectAbortNack
            if isinstance(result, ErrorRejectAbortNack):
                error_response = {
                    "error": type(result).__name__,
                    "details": str(result)
                }
                return json.dumps(error_response).encode('utf8')
            return json.dumps(result).encode('utf8')
        except TypeError as e:
            error_response = {
                "error": "SerializationError",
                "details": str(e),
                "raw_type": str(type(result)),
                "raw_str": str(result)
            }
            return json.dumps(error_response).encode('utf8')

    @callback
    async def read_property_endpoint(self, _, raw_message: bytes):
        """Endpoint for reading a property from a BACnet device."""
        message = json.loads(raw_message.decode('utf8'))
        address = message['device_address']
        object_identifier = message['object_identifier']
        property_identifier = message['property_identifier']
        property_array_index = message.get('property_array_index', None)
        result = await self.bacnet.read_property(address, object_identifier, property_identifier, property_array_index)
        def make_jsonable(val):
            if isinstance(val, (list, tuple)):
                return [make_jsonable(v) for v in val]
            if isinstance(val, (bytes, bytearray)):
                return val.hex()
            if hasattr(val, 'as_tuple'):
                return str(val)
            if hasattr(val, '__dict__') and not isinstance(val, type):
                return {k: make_jsonable(v) for k, v in val.__dict__.items()}
            if hasattr(val, '__class__') and 'Error' in val.__class__.__name__:
                return str(val)
            import ipaddress
            if isinstance(val, (ipaddress.IPv4Address, ipaddress.IPv6Address)):
                return str(val)
            return val
        jsonable_result = make_jsonable(result)
        try:
            from bacpypes3.apdu import ErrorRejectAbortNack
            if isinstance(result, ErrorRejectAbortNack):
                error_response = {
                    "error": type(result).__name__,
                    "details": str(result)
                }
                return json.dumps(error_response).encode('utf8')
            return json.dumps(jsonable_result).encode('utf8')
        except TypeError as e:
            error_response = {
                "error": "SerializationError",
                "details": str(e),
                "raw_type": str(type(result)),
                "raw_str": str(result)
            }
            return json.dumps(error_response).encode('utf8')

    @callback
    async def send_object_user_lock_time_endpoint(self, _, raw_message: bytes):
        """Endpoint for sending an object user lock time to a BACnet device."""
        message = json.loads(raw_message.decode('utf8'))
        address = Address(message['address'])
        device_id = message['device_id']
        object_id = message['object_id']
        lock_interval = message['lock_interval']
        result = await self.bacnet.send_object_user_lock_time(address, device_id, object_id, lock_interval)
        return json.dumps(result).encode('utf8')

    @callback
    async def write_property_endpoint(self, _, raw_message: bytes):
        """Endpoint for writing a property to a BACnet device."""
        message = json.loads(raw_message.decode('utf8'))
        address = message['device_address']
        object_identifier = message['object_identifier']
        property_identifier = message['property_identifier']
        value = message['value']
        priority = message['priority']
        property_array_index = message.get('property_array_index', None)
        result = await self.bacnet.write_property(address, object_identifier, property_identifier, value, priority,
                                            property_array_index)
        return json.dumps(result).encode('utf8')

    @callback
    async def read_device_all_endpoint(self, _, raw_message: bytes):
        """Endpoint for reading all properties from a BACnet device."""
        import traceback
        try:
            message = json.loads(raw_message.decode('utf8'))
            device_address = message['device_address']
            device_object_identifier = message['device_object_identifier']
            result = await self.read_device_all(device_address, device_object_identifier)
            if not result:
                return json.dumps({"error": "No data returned from read_device_all"}).encode('utf8')
            def make_jsonable(val):
                if isinstance(val, (str, int, float, bool)):
                    return val
                if isinstance(val, (list, tuple, set)):
                    return [make_jsonable(v) for v in val]
                if isinstance(val, (bytes, bytearray)):
                    return val.hex()
                if hasattr(val, '__dict__') and not isinstance(val, type):
                    return {str(k): make_jsonable(v) for k, v in val.__dict__.items()}
                import ipaddress
                if isinstance(val, (ipaddress.IPv4Address, ipaddress.IPv6Address)):
                    return str(val)
                # TODO: Replace this forced string conversion with proper BACnet object serialization
                return f"FORCED:{str(val)}"
            jsonable_result = {str(k): make_jsonable(v) for k, v in result.items()}
            return json.dumps(jsonable_result).encode('utf8')
        except Exception as e:
            tb = traceback.format_exc()
            return json.dumps({"error": str(e), "traceback": tb}).encode('utf8')

    @callback
    async def who_is_endpoint(self, _, raw_message: bytes):
        """Endpoint for sending a Who-Is request."""
        message = json.loads(raw_message.decode('utf8'))
        device_instance_low = message['device_instance_low']
        device_instance_high = message['device_instance_high']
        dest = message['dest']
        result = await self.who_is(device_instance_low, device_instance_high, dest)
        try:
            return json.dumps(result).encode('utf8')
        except TypeError as e:
            return json.dumps({"error": "SerializationError", "details": str(e), "raw_type": str(type(result)), "raw_str": str(result)}).encode('utf8')

    @callback
    async def scan_subnet_endpoint(self, _, raw_message: bytes):
        """Endpoint for scanning a subnet for BACnet devices."""
        message = json.loads(raw_message.decode('utf8'))
        network_str = message['network_str']
        _log.debug(f"scan_subnet_endpoint called with network_str: {network_str}")
        result = await self.scan_subnet(network_str) # Using default timeout for now
        try:
            return json.dumps(result).encode('utf8')
        except TypeError as e:
            _log.error(f"SerializationError in scan_subnet_endpoint: {e}, result was: {result}", exc_info=True)
            return json.dumps({"error": "SerializationError", "details": str(e), "raw_type": str(type(result)), "raw_str": str(result)}).encode('utf8')

    async def scan_subnet(self, network_str: str, whois_timeout: float = 3.0) -> list:
        import ipaddress # Moved import inside for clarity, though module level is also fine
        import time
        start_time = time.time()
        max_scan_duration = 280  # 280 seconds to leave buffer for callback timeout
        _log.info(f"Starting IP range scan for network: {network_str} with Who-Is timeout: {whois_timeout}s, max scan duration: {max_scan_duration}s")
        try:
            net = ipaddress.ip_network(network_str, strict=False)
        except ValueError as e:
            _log.error(f"Invalid network string provided to scan_ip_range: {network_str} - {e}")
            return [{"error": "InvalidNetworkString", "details": str(e)}]
        
        # Check if the network is too large and warn
        num_hosts = net.num_addresses - 2 if net.num_addresses > 2 else net.num_addresses  # Subtract network and broadcast
        if num_hosts > 1000:
            _log.warning(f"Large network detected: {num_hosts} hosts. This may take a very long time or timeout.")
            
        tasks = []
        semaphore = asyncio.Semaphore(20)

        async def scan_host(ip_obj):
            ip_str = str(ip_obj)
            # Check if we're running out of time
            if time.time() - start_time > max_scan_duration:
                _log.warning(f"Scan time limit reached, skipping remaining hosts starting with {ip_str}")
                return (ip_str, [{"error": "ScanTimeLimit", "message": "Scan time limit reached"}])

            async with semaphore:
                _log.debug(f"Scanning host: {ip_str} with Who-Is timeout: {whois_timeout}s")
                i_am_responses = []
                try:
                    i_am_responses = await self.who_is(0, 4194303, ip_str, apdu_timeout=whois_timeout)
                except Exception as e:
                    _log.error(f"Exception calling self.who_is for {ip_str}: {e}", exc_info=True)

                _log.debug(f"Who-Is responses for {ip_str}: {i_am_responses}")
                found_devices_on_host = []
                if i_am_responses:
                    for device_data in i_am_responses:
                        # Only add the I-Am response data, do not read object-name or any other property
                        found_devices_on_host.append(device_data)
                _log.debug(f"Finished processing host {ip_str}. Found {len(found_devices_on_host)} potential devices.")
                return (ip_str, found_devices_on_host)

        for ip_address_obj in net.hosts():
            tasks.append(asyncio.create_task(scan_host(ip_address_obj)))

        _log.debug(f"Created {len(tasks)} scan_host tasks for network {network_str}.")
        gathered_results = await asyncio.gather(*tasks, return_exceptions=True)
        elapsed_time = time.time() - start_time
        _log.debug(f"Finished gathering {len(gathered_results)} scan_host results for network {network_str} in {elapsed_time:.2f} seconds.")
        
        discovered_devices_final = []
        scan_aborted = False
        
        for result_item in gathered_results:
            if isinstance(result_item, Exception):
                _log.error(f"Exception returned from a scan_host task: {result_item}", exc_info=result_item)
                continue
            
            if result_item and isinstance(result_item, tuple) and len(result_item) == 2:
                ip_scanned_str, devices_from_this_ip = result_item
                if devices_from_this_ip:
                    # Check if any device indicates scan was aborted due to time limit
                    if any(isinstance(d, dict) and d.get('error') == 'ScanTimeLimit' for d in devices_from_this_ip):
                        scan_aborted = True
                        _log.info(f"Scan was aborted at {ip_scanned_str} due to time limit")
                        break
                    
                    _log.debug(f"Adding {len(devices_from_this_ip)} devices found at {ip_scanned_str} to final list.")
                    for dev in devices_from_this_ip:
                        dev['scanned_ip_target'] = ip_scanned_str 
                    discovered_devices_final.extend(devices_from_this_ip)
                else:
                    _log.debug(f"No devices found or reported for IP: {ip_scanned_str}")
            else:
                _log.warning(f"Unexpected result format from scan_host task: {result_item}")

        final_message = f"IP range scan for {network_str} {'partially completed (time limit reached)' if scan_aborted else 'complete'}. Total devices found: {len(discovered_devices_final)}. Scan time: {elapsed_time:.2f}s"
        _log.info(final_message)
        
        # Add scan metadata to help with debugging
        if scan_aborted:
            discovered_devices_final.append({
                "scan_info": {
                    "status": "partial",
                    "reason": "Time limit reached",
                    "network": network_str,
                    "scan_duration_seconds": elapsed_time,
                    "max_duration_seconds": max_scan_duration,
                    "total_hosts_in_network": num_hosts
                }
            })
        
        return discovered_devices_final

    async def read_device_all(self, device_address: str, device_object_identifier: str) -> dict:
        from bacpypes3.primitivedata import ObjectIdentifier
        from bacpypes3.basetypes import PropertyReference
        from bacpypes3.lib.batchread import BatchRead, DeviceAddressObjectPropertyReference

        properties = [
            "object-identifier",
            "object-name",
            "object-type",
            "system-status",
            "vendor-name",
            "vendor-identifier",
            "model-name",
            "firmware-revision",
            "application-software-version",
            "location",
            "description",
            "protocol-version",
            "protocol-revision",
            "protocol-services-supported",
            "protocol-object-types-supported",
            "object-list",
            "structured-object-list",
            "max-apdu-length-accepted",
            "segmentation-supported",
            "max-segments-accepted",
            "vt-classes-supported",
            "active-vt-sessions",
            "local-time",
            "local-date",
            "utc-offset",
            "daylight-savings-status",
            "apdu-segment-timeout",
            "apdu-timeout",
            "number-of-apdu-retries",
            "time-synchronization-recipients",
            "max-master",
            "max-info-frames",
            "device-address-binding",
            "database-revision",
            "configuration-files",
            "last-restore-time",
            "backup-failure-timeout",
            "backup-preparation-time",
            "restore-preparation-time",
            "restore-completion-time",
            "backup-and-restore-state",
            "active-cov-subscriptions",
            "last-restart-reason",
            "time-of-device-restart",
            "restart-notification-recipients",
            "utc-time-synchronization-recipients",
            "time-synchronization-interval",
            "align-intervals",
            "interval-offset",
            "serial-number",
            "property-list",
            "status-flags",
            "event-state",
            "reliability",
            "event-detection-enable",
            "notification-class",
            "event-enable",
            "acked-transitions",
            "notify-type",
            "event-time-stamps",
            "event-message-texts",
            "event-message-texts-config",
            "reliability-evaluation-inhibit",
            "active-cov-multiple-subscriptions",
            "audit-notification-recipient",
            "audit-level",
            "auditable-operations",
            "device-uuid",
            "tags",
            "profile-location",
            "deployed-profile-location",
            "profile-name",
        ]
        device_obj = ObjectIdentifier(device_object_identifier)
        daopr_list = [
            DeviceAddressObjectPropertyReference(
                key=prop,
                device_address=device_address,
                object_identifier=device_obj,
                property_reference=PropertyReference(prop)
            ) for prop in properties
        ]
        results = {}
        import logging
        def callback(key, value):
            logging.getLogger(__name__).debug(f"BatchRead callback: key={key}, value={value}")
            results[key] = value
        batch = BatchRead(daopr_list)
        try:
            await asyncio.wait_for(batch.run(self.bacnet.app, callback=callback), timeout=30)
        except asyncio.TimeoutError:
            logging.getLogger(__name__).error("BatchRead timed out after 30 seconds!")
            results['error'] = 'Timeout waiting for BACnet device response.'
        except Exception as e:
            logging.getLogger(__name__).exception(f"Exception in BatchRead: {e}")
            results['error'] = str(e)
        return results

    async def who_is(self, device_instance_low: int, device_instance_high: int, dest: str, *, apdu_timeout: Optional[float] = None):
        from bacpypes3.pdu import Address # Keep import here if only used here

        destination_addr = dest if isinstance(dest, Address) else Address(dest)
        _log.debug(f"Sending Who-Is to {destination_addr} (low_id: {device_instance_low}, high_id: {device_instance_high}), APDU timeout: {apdu_timeout}s")
        
        app_instance = None
        try:
            if hasattr(self, 'bacnet') and hasattr(self.bacnet, 'app'):
                app_instance = self.bacnet.app
            elif hasattr(self, 'app'): # Should not be the case based on __init__ structure
                app_instance = self.app
            
            if not app_instance:
                _log.error("BACnet application instance (self.bacnet.app) not found in BACnetProxy for who_is.")
                return []

            i_am_responses = await app_instance.who_is(
                device_instance_low,
                device_instance_high,
                destination_addr,
                timeout=apdu_timeout
            )
            _log.debug(f"Received {len(i_am_responses)} I-Am response(s) from {destination_addr}")
            
            devices_found = []
            if i_am_responses:
                for i_am_pdu in i_am_responses:
                    device_info = {
                        "pduSource": str(i_am_pdu.pduSource),
                        "deviceIdentifier": i_am_pdu.iAmDeviceIdentifier,
                        "maxAPDULengthAccepted": i_am_pdu.maxAPDULengthAccepted,
                        "segmentationSupported": str(i_am_pdu.segmentationSupported),
                        "vendorID": i_am_pdu.vendorID,
                    }
                    devices_found.append(device_info)
            return devices_found
        except asyncio.TimeoutError:
            _log.warning(f"Who-Is request to {destination_addr} timed out (APDU timeout: {apdu_timeout}s). No I-Am responses received.")
            return []
        except ErrorRejectAbortNack as e_bac:
            _log.error(f"BACnet Error/Reject/Abort during Who-Is to {destination_addr}: {e_bac}", exc_info=True)
            return []
        except Exception as e_gen:
            _log.error(f"Unexpected error during Who-Is request to {destination_addr}: {e_gen}", exc_info=True)
            return []

    @classmethod
    def get_unique_remote_id(cls, unique_remote_id: tuple) -> tuple:
        """Get a unique identifier for the proxy server
         given a unique_remote_id and protocol-specific set of parameters."""
        return unique_remote_id[0:2]  # TODO: How can we know what the first two params really are?
                                      #  (Ideally they are address and port.)
                                      #  Consider named tuple?


class BACnet:
    def __init__(self, local_device_address, bacnet_network=0, vendor_id=999, object_name='VOLTTRON BACnet Proxy',
                 device_info_cache=None, router_info_cache=None, ase_id=None, **_):
        _log.debug('WELCOME BAC')
        vendor_info = get_vendor_info(vendor_id)
        device_object_class = vendor_info.get_object_class(ObjectType.device)
        device_object = device_object_class(objectIdentifier=('device', vendor_id), objectName=object_name)
        network_port_object_class = vendor_info.get_object_class(ObjectType.networkPort)
        network_port_object = network_port_object_class(local_device_address,
                                                        objectIdentifier=("network-port", bacnet_network),
                                                        objectName="NetworkPort-1", networkNumber=bacnet_network,
                                                        networkNumberQuality="configured")
        self.app = Application.from_object_list(
            [device_object, network_port_object],
            device_info_cache=device_info_cache,  # TODO: If these should be passed in, add to args & launch.
            router_info_cache=router_info_cache,
            aseID=ase_id
        )

    async def query_device(self, address: str, property_name: str = 'object-identifier'):
        _log.debug(f"BACnet.query_device called with address={address}, property_name={property_name}")
        return await self.read_property(device_address=address, object_identifier='device:4194303',
                                        property_identifier=property_name)

    async def read_property(self, device_address: str, object_identifier: str, property_identifier: str,
                   property_array_index: int | None = None):
        try:
            _log.debug(f"BACnet.read_property called with device_address={device_address}, object_identifier={object_identifier}, property_identifier={property_identifier}, property_array_index={property_array_index}")
            response = await self.app.read_property(
                Address(device_address),
                ObjectIdentifier(object_identifier),
                property_identifier,
                int(property_array_index) if property_array_index is not None else None
            )
            _log.debug(f"BACnet.read_property response: {response}")
        except ErrorRejectAbortNack as err:
            _log.debug(f'Error reading property {err}')
            response = err
        if isinstance(response, AnyAtomic):
            response = response.get_value()
        _log.debug(f"BACnet.read_property final response: {response}")
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


def launch_bacnet(parser: ArgumentParser) -> tuple[ArgumentParser, Type[AsyncioProtocolProxy]]:
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
