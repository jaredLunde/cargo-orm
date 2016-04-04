"""

  `Cargo SQL Binary Fields`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/cargo-orm

"""
import binascii
import psycopg2
from base64 import b64encode, b64decode

from psycopg2.extensions import register_adapter, adapt

from vital.tools.encoding import uniorbytes

from cargo.etc.types import *
from cargo.expressions import *
from cargo.fields.field import Field
from cargo.logic.binary import BinaryLogic


__all__ = ('Binary',)


class cargobytes(bytes):
    def __iadd__(self, other):
        return self.__class__(self.__add__(other))

    def __isub__(self, other):
        return self.__class__(self.__sub__(other))

    @staticmethod
    def to_db(value):
        return adapt(psycopg2.Binary(b64encode(value)))


class Binary(Field, BinaryLogic):
    OID = BINARY
    __slots__ = Field.__slots__

    def __init__(self, *args, **kwargs):
        """ `Binary`
            :see::meth:Field.__init__
        """
        super().__init__(*args, **kwargs)

    def __call__(self, value=Field.empty):
        if value is not self.empty:
            if value is not None:
                value = cargobytes(uniorbytes(value, bytes))
            self.value = value
        return self.value

    def __bytes__(self):
        return self.value

    @staticmethod
    def register_adapter():
        register_adapter(cargobytes, cargobytes.to_db)
        BINARYTYPE = reg_type('BINARYTYPE', BINARY, Binary.to_python)
        reg_array_type('BINARYARRAYTYPE', BINARYARRAY, BINARYTYPE)

    @staticmethod
    def to_python(value, cur):
        try:
            return b64decode(psycopg2.BINARY(value, cur))
        except (TypeError, binascii.Error):
            return psycopg2.BINARY(value, cur)

    def to_json(self):
        try:
            return self.value.decode()
        except AttributeError:
            return None
