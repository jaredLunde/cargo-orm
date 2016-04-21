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


class _BytesAdapter(object):

    def __init__(self, value):
        self.value = value

    def prepare(self, conn):
        self.conn = conn

    def getquoted(self):
        adapter = adapt(psycopg2.Binary(self.value))
        adapter.prepare(self.conn)
        return adapter.getquoted()


class cargobytes(bytes):
    def __iadd__(self, other):
        return self.__class__(self.__add__(other))

    def __isub__(self, other):
        return self.__class__(self.__sub__(other))


class Binary(Field, BinaryLogic):
    OID = BINARY
    __slots__ = Field.__slots__

    def __init__(self, *args, **kwargs):
        """ `Binary`
            ==================================================================
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
        register_adapter(cargobytes, _BytesAdapter)
        BINARYTYPE = reg_type('BINARYTYPE', BINARY, Binary.to_python)
        reg_array_type('BINARYARRAYTYPE', BINARYARRAY, BINARYTYPE)

    @staticmethod
    def to_python(value, cur):
        bin_ = psycopg2.BINARY(value, cur)
        try:
            return b64decode(bin_)
        except (TypeError, binascii.Error):
            return bin_

    def for_json(self):
        """:see::meth:Field.for_json"""
        try:
            return self.value.decode()
        except AttributeError:
            return None
