"""

  `Cargo SQL Networking Fields`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/cargo-orm

"""
import copy
from netaddr import *

import psycopg2
from psycopg2.extensions import *

from cargo.etc.types import *
from cargo.expressions import *
from cargo.fields.field import Field
from cargo.logic.networking import NetworkingLogic


__all__ = ('IP', 'Inet', 'Cidr', 'MacAddress')


class _IPAdapter(object):

    def __init__(self, value):
        self.value = value

    def prepare(self, conn):
        self.conn = conn

    def getquoted(self):
        adapter = adapt(str(self.value))
        adapter.prepare(self.conn)
        return b"%s::inet" % adapter.getquoted()


class IP(Field, NetworkingLogic):
    """ ======================================================================
        Field object for the PostgreSQL field type |INET|.
    """
    __slots__ = ('field_name', 'primary', 'unique', 'index', 'not_null',
                 'value', 'validator', '_alias', 'table', '_request')
    OID = IPTYPE

    def __init__(self, request=None, *args, **kwargs):
        """`IP Address`
            ==================================================================
            @request: Django, Flask or Bottle-like request object
            ==================================================================
            :see::meth:Field.__init__
        """
        self._request = request
        super().__init__(*args, **kwargs)

    def __getattr__(self, name):
        try:
            return self.__getattribute__(name)
        except AttributeError:
            return self.value.__getattribute__(name)

    def __call__(self, value=Field.empty):
        if value is not Field.empty:
            if value is not None:
                value = IPAddress(value)
            self.value = value
        return self.value

    def __int__(self):
        return int(self.value)

    def from_request(self, request):
        """ Gets an IP address from a typical WSGI request object """
        return self.__call__(self._request_to_ip(request))

    def _request_to_ip(self, request):
        if request is None:
            return None
        if hasattr(request, 'remote_addr'):
            return request.remote_addr
        elif hasattr(request, 'META'):
            x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
            if x_forwarded_for:
                return x_forwarded_for.split(',')[-1].strip()
            else:
                return request.META.get('REMOTE_ADDR')

    @property
    def request_ip(self):
        return self._request_to_ip(self._request)

    def __getstate__(self):
        return dict((slot, getattr(self, slot) if slot != '_request' else None)
                    for slot in self.__slots__)

    def __setstate__(self, state):
        for slot, value in state.items():
            setattr(self, slot, value)

    @staticmethod
    def to_python(value, cur):
        if value is None:
            return value
        return IPAddress(value)

    @staticmethod
    def register_adapter():
        register_adapter(IPAddress, _IPAdapter)
        IPTYPE_ = reg_type('IPTYPE', IPTYPE, IP.to_python)
        reg_array_type('IPARRAYTYPE', IPARRAY, IPTYPE_)

    def copy(self, *args, **kwargs):
        return super().copy(*args, request=self._request, **kwargs)

    __copy__ = copy


Inet = IP


class _CidrAdapter(_IPAdapter):


    def getquoted(self):
        adapter = adapt(str(self.value))
        adapter.prepare(self.conn)
        return b"%s::cidr" % adapter.getquoted()


class Cidr(Field, StringLogic):
    """ ======================================================================
        Field object for the PostgreSQL field type |CIDR|.
    """
    __slots__ = Field.__slots__
    OID = CIDR

    def __init__(self, *args, **kwargs):
        """ `Cidr Addresses`
            ==================================================================
            :see::meth:Field.__init__
        """
        super().__init__(*args, **kwargs)

    def __getattr__(self, name):
        try:
            return self.__getattribute__(name)
        except AttributeError:
            return self.value.__getattribute__(name)

    def __call__(self, value=Field.empty):
        if value is not Field.empty:
            if value is not None:
                value = IPNetwork(value)
            self.value = value
        return self.value

    def __int__(self):
        return int(self.value)

    @staticmethod
    def to_python(value, cur):
        if value is None:
            return value
        return IPNetwork(value)

    @staticmethod
    def register_adapter():
        register_adapter(IPNetwork, _CidrAdapter)
        CIDRTYPE = reg_type('CIDRTYPE', CIDR, Cidr.to_python)
        reg_array_type('CIDRARRAYTYPE', CIDRARRAY, CIDRTYPE)


class _MacAddressAdapter(_IPAdapter):

    def getquoted(self):
        adapter = adapt(str(self.value))
        adapter.prepare(self.conn)
        return b"%s::macaddr" % adapter.getquoted()


class MacAddress(Cidr):
    """ ======================================================================
        Field object for the PostgreSQL field type |MACADDR|.
    """
    OID = MACADDR
    __slots__ = Field.__slots__

    def __init__(self, *args, **kwargs):
        """ `Mac Addresses`
            ==================================================================
            :see::meth:Field.__init__
        """
        super().__init__(*args, **kwargs)

    def __getattr__(self, name):
        try:
            return self.__getattribute__(name)
        except AttributeError:
            return self.value.__getattribute__(name)

    def __call__(self, value=Field.empty):
        if value is not Field.empty:
            if value is not None:
                value = EUI(value)
            self.value = value
        return self.value

    @staticmethod
    def to_python(value, cur):
        if value is None:
            return value
        return EUI(value)

    @staticmethod
    def register_adapter():
        register_adapter(EUI, _MacAddressAdapter)
        MACADDRTYPE = reg_type('MACADDRTYPE', MACADDR, MacAddress.to_python)
        reg_array_type('MACADDRARRAYTYPE', MACADDRARRAY, MACADDRTYPE)

    def trunc(self, *args, **kwargs):
        """ Sets last 3 bytes to zero
            -> (:class:Function)
        """
        return Function(trunc, self, *args, **kwargs)
