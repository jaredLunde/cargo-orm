"""

  `Bloom SQL Boolean Fields`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/bloom-orm

"""
from bloom.etc.types import *
from bloom.expressions import *
from bloom.fields.field import Field


__all__ = ('Bool',)


class Bool(Field):
    """ Field object for the PostgreSQL field type |BOOL| """
    __slots__ = (
        'field_name', 'primary', 'unique', 'index', 'not_null', 'value',
        'validation', 'validation_error', '_alias', 'default', 'table')
    sqltype = BOOL

    def __init__(self, value=Field.empty, **kwargs):
        """ `Bool`
            :see::meth:Field.__init__
        """
        super().__init__(value=value, **kwargs)

    def __call__(self, value=Field.empty):
        if value is not Field.empty:
            self._set_value(bool(value) if value is not None else value)
        return self.value

    def validate(self):
        if self.not_null:
            return self.value in {True, False}
        return self.value in {True, False, None, Field.empty}
