"""
  `Bitwise Logic and Operations`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
from cargo.expressions import *


__all__ = ('BitLogic',)


class BitLogic(object):
    __slots__ = tuple()
    '''
    ||	concatenation	B'10001' || B'011'	10001011
    &	bitwise AND	B'10001' & B'01101'	00001
    |	bitwise OR	B'10001' | B'01101'	11101
    #	bitwise XOR	B'10001' # B'01101'	11100
    ~	bitwise NOT	~ B'10001'	01110
    <<	bitwise shift left	B'10001' << 3	01000
    >>	bitwise shift right	B'10001' >> 2	00100
    '''
