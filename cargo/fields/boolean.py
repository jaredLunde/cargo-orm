"""

  `Cargo SQL Boolean Fields`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/cargo-orm

"""
from cargo.etc.types import *
from cargo.expressions import *
from cargo.fields.field import Field
from cargo.validators import BooleanValidator


__all__ = ('Bool',)


class Bool(Field):
    """ Field object for the PostgreSQL field type |BOOL| """
    __slots__ = Field.__slots__
    OID = BOOL

    def __init__(self, *args, validator=BooleanValidator, **kwargs):
        """`Bool`
            ==================================================================
            :see::meth:Field.__init__
        """
        super().__init__(*args, validator=validator, **kwargs)

    def __call__(self, value=Field.empty):
        if value is not Field.empty:
            self.value = bool(value) if value is not None else value
        return self.value

    def __int__(self):
        return 1 if self.value is True else 0

    def for_json(self):
        """:see::meth:Field.for_json"""
        if self.value in {True, False}:
            return self.value
        return None
