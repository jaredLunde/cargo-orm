"""

  `Bloom SQL Networking Fields`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/bloom-orm

"""
import copy
from netaddr import *

import psycopg2
from psycopg2.extensions import *

from bloom.etc.types import *
from bloom.expressions import *
from bloom.fields.field import Field


__all__ = ('IP', 'Inet', 'Cidr', 'MacAddress')


class NetworkingLogic(BaseLogic):
    __slots__ = tuple()
    '''
    <	is less than	inet '192.168.1.5' < inet '192.168.1.6'
    <=	is less than or equal	inet '192.168.1.5' <= inet '192.168.1.5'
    =	equals	inet '192.168.1.5' = inet '192.168.1.5'
    >=	is greater or equal	inet '192.168.1.5' >= inet '192.168.1.5'
    >	is greater than	inet '192.168.1.5' > inet '192.168.1.4'
    <>	is not equal	inet '192.168.1.5' <> inet '192.168.1.4'
    <<	is contained by	inet '192.168.1.5' << inet '192.168.1/24'
    <<=	is contained by or equals	inet
        '192.168.1/24' <<= inet '192.168.1/24'
    >>	contains	inet '192.168.1/24' >> inet '192.168.1.5'
    >>=	contains or equals	inet '192.168.1/24' >>= inet '192.168.1/24'
    &&	contains or is contained by	inet
        '192.168.1/24' && inet '192.168.1.80/28'
    ~	bitwise NOT	~ inet '192.168.1.6'
    &	bitwise AND	inet '192.168.1.6' & inet '0.0.0.255'
    |	bitwise OR	inet '192.168.1.6' | inet '0.0.0.255'
    +	addition	inet '192.168.1.6' + 25
    -	subtraction	inet '192.168.1.43' - 36
    -	subtraction	inet '192.168.1.43' - inet '192.168.1.19'

    abbrev(inet)	text	abbreviated display format as text
        abbrev(inet '10.1.0.0/16')	10.1.0.0/16
    abbrev(cidr)	text	abbreviated display format as text
        abbrev(cidr '10.1.0.0/16')	10.1/16
    broadcast(inet)	inet	broadcast address for network
        broadcast('192.168.1.5/24')	192.168.1.255/24
    family(inet)	int	extract family of address; 4 for IPv4, 6 for IPv6
        family('::1')	6
    host(inet)	text	extract IP address as text	host('192.168.1.5/24')
        192.168.1.5
    hostmask(inet)	inet	construct host mask for network
        hostmask('192.168.23.20/30')	0.0.0.3
    masklen(inet)	int	extract netmask length	masklen('192.168.1.5/24')	24
    netmask(inet)	inet	construct netmask for network
        netmask('192.168.1.5/24')	255.255.255.0
    network(inet)	cidr	extract network part of address
        network('192.168.1.5/24')	192.168.1.0/24
    set_masklen(inet, int)	inet	set netmask length for inet value
        set_masklen('192.168.1.5/24', 16)	192.168.1.5/16
    set_masklen(cidr, int)	cidr	set netmask length for cidr value
        set_masklen('192.168.1.0/24'::cidr, 16)	192.168.0.0/16
    text(inet)	text	extract IP address and netmask length as text
        text(inet '192.168.1.5')	192.168.1.5/32
    inet_same_family(inet, inet)	boolean	are the addresses from the same
        family?	inet_same_family('192.168.1.5/24', '::1')	false
    inet_merge(inet, inet)	cidr	the smallest network which includes both of
        the given networks	inet_merge('192.168.1.5/24', '192.168.2.5/24')
        192.168.0.0/22
    '''


class IP(Field, NetworkingLogic):
    """ =======================================================================
        Field object for the PostgreSQL field type |INET|.
    """
    __slots__ = ('field_name', 'primary', 'unique', 'index', 'not_null',
                 'value', 'validator', '_alias', '_default', 'table',
                 '_request')
    OID = IPTYPE
    current = -1

    def __init__(self, request=None, *args, default=None, **kwargs):
        """ `IP Address`
            :see::meth:Field.__init__
            @request: Django, Flask or Bottle-like request object
        """
        self._default = default
        self._request = request
        super().__init__(*args, **kwargs)

    def __getattr__(self, name):
        try:
            return self.__getattribute__(name)
        except AttributeError:
            return self.value.__getattribute__(name)

    def __call__(self, value=Field.empty):
        if value is not Field.empty:
            if value == self.current:
                value = self.request_ip
            if value is not None:
                value = IPAddress(value)
            self.value = value
        return self.value

    def __int__(self):
        return int(self.value)

    @property
    def request_ip(self):
        if self._request is None:
            return None
        if hasattr(self._request, 'remote_addr'):
            return self._request.remote_addr
        elif hasattr(self._request, 'META'):
            x_forwarded_for = self._request.META.get('HTTP_X_FORWARDED_FOR')
            if x_forwarded_for:
                return x_forwarded_for.split(',')[-1].strip()
            else:
                return self._request.META.get('REMOTE_ADDR')
        return None

    @property
    def default(self):
        if self._default == self.current:
            return self.request_ip
        return self._default

    def __getstate__(self):
        return dict((slot, getattr(self, slot))
                    for slot in self.__slots__
                    if hasattr(self, slot))

    def __setstate__(self, state):
        for slot, value in state.items():
            setattr(self, slot, value)

    @staticmethod
    def to_db(value):
        return AsIs("%s::inet" % adapt(str(value)).getquoted().decode())

    @staticmethod
    def to_python(value, cur):
        if value is None:
            return value
        return IPAddress(value)

    @staticmethod
    def register_adapter():
        register_adapter(IPAddress, IP.to_db)
        IPTYPE_ = reg_type('IPTYPE', IPTYPE, IP.to_python)
        reg_array_type('IPARRAYTYPE', IPARRAY, IPTYPE_)

    def copy(self, *args, **kwargs):
        cls = self._copy(self._request, *args, **kwargs)
        cls._default = self._default
        return cls

    __copy__ = copy


Inet = IP


class Cidr(Field, StringLogic):
    """ =======================================================================
        Field object for the PostgreSQL field type |CIDR|.
    """
    __slots__ = Field.__slots__
    OID = CIDR

    def __init__(self, *args, **kwargs):
        """ `Cidr Addresses`
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
        register_adapter(IPNetwork, Cidr.to_db)
        CIDRTYPE = reg_type('CIDRTYPE', CIDR, Cidr.to_python)
        reg_array_type('CIDRARRAYTYPE', CIDRARRAY, CIDRTYPE)

    @staticmethod
    def to_db(value):
        return AsIs("%s::cidr" % adapt(str(value)).getquoted().decode())


class MacAddress(Cidr):
    """ =======================================================================
        Field object for the PostgreSQL field type |MACADDR|.
    """
    OID = MACADDR
    __slots__ = Field.__slots__

    def __init__(self, *args, **kwargs):
        """ `Mac Addresses`
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
    def to_db(value):
        return AsIs("%s::macaddr" % adapt(str(value)).getquoted().decode())

    @staticmethod
    def register_adapter():
        register_adapter(EUI, MacAddress.to_db)
        MACADDRTYPE = reg_type('MACADDRTYPE', MACADDR, MacAddress.to_python)
        reg_array_type('MACADDRARRAYTYPE', MACADDRARRAY, MACADDRTYPE)

    def trunc(self, *args, **kwargs):
        """ Sets last 3 bytes to zero
            -> (:class:Function)
        """
        return Function(trunc, self, *args, **kwargs)
