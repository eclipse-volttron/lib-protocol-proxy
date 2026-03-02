import logging

from functools import wraps
from types import MethodType

from .base import ProtocolProxy

_log = logging.getLogger(__name__)


class ProxyPlugin:
    """
    Plugin to add functionality to an existing driver interface.
    Use as:
        di = MyDriverInterface()
        MyDriverPlugin.plug_into(di)
        di.method_from_plugin()  # The method is now available as if it were part of MyDriverInterface.

    """
    # Plugins should specify the specific class they are extending unless they are intended
    #  to work with all possible interfaces or proxies (in which case specify BaseInterface/ProtocolProxy).
    EXTENDED_CLASS = ProtocolProxy

    # Plugins should specify a set of method names from the plugin class which will be added to the extended interface.
    PLUGIN_METHODS = set()
    # API_METHODS <-- {api_name: {'method_name': method_name, Optional['provides_response': bool], Optional['timeout': float]]}
    PROTOCOL_PROXY_METHODS: dict[str, dict[str, str | bool | float]] = dict()

    @classmethod
    def plug_into(cls, proxy):
        if not isinstance(proxy, cls.EXTENDED_CLASS):
            raise TypeError(f'Plugin "{cls.__name__} extends {cls.EXTENDED_CLASS.__name__}'
                            f' not {proxy.__class__.__name__}')
        for method_name in cls.PLUGIN_METHODS:
            method = getattr(cls, method_name)
            _log.info(f'Attaching plugin method: "{method_name}" to: {proxy}')
            method = cls._add_overridden(method, getattr(proxy, method_name)) if hasattr(proxy, method_name) else method
            setattr(proxy, method_name, MethodType(method, proxy))
        kwargs = {}
        for api_name, params in cls.PROTOCOL_PROXY_METHODS.items():
            if provides_response := params.get('provides_response'):
                kwargs['provides_response'] = provides_response
            if timeout := params.get('timeout'):
                kwargs['timeout'] = timeout
            proxy.register_callback(MethodType(getattr(cls, params['method_name']), proxy), api_name, **kwargs)

    @staticmethod
    def _add_overridden(func, overridden_method):
        @wraps(func)
        def wrapper(*args, **kwargs):
            kwargs['overridden'] = overridden_method
            return func(*args, **kwargs)

        return wrapper
