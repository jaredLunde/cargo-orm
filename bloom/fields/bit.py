"""

  `Bloom SQL Bit Fields`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/bloom-orm

"""
from bitstring import BitArray
from psycopg2.extensions import new_type, register_type, register_adapter


from bloom.etc.types import *
from bloom.expressions import *
from bloom.fields.field import Field
from bloom.validators import BitValidator, VarbitValidator


__all__ = ('BitLogic', 'Bit', 'Varbit',)


class BitLogic(object):
    '''
    ||	concatenation	B'10001' || B'011'	10001011
    &	bitwise AND	B'10001' & B'01101'	00001
    |	bitwise OR	B'10001' | B'01101'	11101
    #	bitwise XOR	B'10001' # B'01101'	11100
    ~	bitwise NOT	~ B'10001'	01110
    <<	bitwise shift left	B'10001' << 3	01000
    >>	bitwise shift right	B'10001' >> 2	00100
    '''
    pass


class Bit(Field, BitLogic):
    """ =======================================================================
        Field object for the PostgreSQL field type |BIT|.
    """
    OID = BIT
    __slots__ = (
        'field_name', 'primary', 'unique', 'index', 'not_null', 'value',
        'default', '_validator', '_alias', 'table', 'length')

    def __init__(self, length, *args, validator=BitValidator, **kwargs):
        """ `Bit`
            :see::meth:Field.__init__
        """
        self.length = length
        super().__init__(*args, **kwargs)

    def __call__(self, value=Field.empty):
        if value is not self.empty:
            if value is not None:
                value = BitArray(value)
            self._set_value(value)
        return self.value

    @staticmethod
    def to_python(value, cur):
        return value

    def copy(self, *args, **kwargs):
        cls = self._copy(self.length, *args, **kwargs)
        return cls


BITTYPE = reg_type("BIT", BIT, Bit.to_python)
BITARRAYTYPE = reg_array_type('BITARRAYTYPE', BITARRAY, BITTYPE)


class Varbit(Bit):
    """ =======================================================================
        Field object for the PostgreSQL field type |VARBIT|.
    """
    OID = VARBIT
    __slots__ = (
        'field_name', 'primary', 'unique', 'index', 'not_null', 'value',
        'default', '_validator', '_alias', 'table', 'length')

    def __init__(self, length, *args, validator=VarbitValidator, **kwargs):
        """ `Bit Varying`
            :see::meth:Field.__init__
        """
        super().__init__(length, *args, **kwargs)
