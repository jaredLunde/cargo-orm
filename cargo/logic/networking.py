"""
  `Networking Logic and Operations`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
from cargo.expressions import *


__all__ = ('NetworkingLogic',)


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
