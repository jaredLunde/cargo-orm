#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""

  `Vital SQL Integer Fields`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/VitalSQL

"""
from vital.debug import prepr

from vital.sql.etc.types import *
from vital.sql.expressions import *
from vital.sql.fields.field import Field


__all__ = ('SmallInt', 'Int', 'BigInt')
class SmallInt(Field, NumericLogic):
    """ - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        Field object for the PostgreSQL field type |INT2|
    """
    __slots__ = (
        'field_name', 'primary', 'unique', 'index', 'notNull', 'value',
        'validation', 'validation_error', '_alias', 'default', 'minval',
        'maxval', 'table')
    sqltype = INT

    def __init__(self, value=None, minval=-32768, maxval=32767, **kwargs):
        """ `SmallInt`
            :see::meth:Field.__init__
            @minval: (#int) minimum interger value
            @maxval: (#int) maximum integer value
        """
        super().__init__(value=value, **kwargs)
        self.minval = minval
        self.maxval = maxval

    def __call__(self, value=Field.empty):
        if value is not Field.empty:
            self._set_value(int(value) if value is not None else None)
        return self.value

    def copy(self, *args, **kwargs):
        cls = self._copy(*args, **kwargs)
        cls.minval = self.minval
        cls.maxval = self.maxval
        return cls

    __copy__ = copy


class Int(Field, NumericLogic):
    """ - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        Field object for the PostgreSQL field type |INT4|
    """
    __slots__ = (
        'field_name', 'primary', 'unique', 'index', 'notNull', 'value',
        'validation', 'validation_error', '_alias', 'default', 'minval',
        'maxval', 'table')
    sqltype = INT

    def __init__(self, value=None, minval=-2147483648, maxval=2147483647,
                 **kwargs):
        """ `Int`
            :see::meth:Field.__init__
            @minval: (#int) minimum interger value
            @maxval: (#int) maximum integer value
        """
        super().__init__(value=value, **kwargs)
        self.minval = minval
        self.maxval = maxval

    @prepr('name', 'minval', 'maxval', 'value')
    def __repr__(self): return

    def __call__(self, value=Field.empty):
        if value is not Field.empty:
            self._set_value(int(value) if value is not None else None)
        return self.value


class BigInt(Field, NumericLogic):
    """ - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        Field object for the PostgreSQL field type |INT8|
    """
    __slots__ = (
        'field_name', 'primary', 'unique', 'index', 'notNull', 'value',
        'validation', 'validation_error', '_alias', 'default', 'minval',
        'maxval', 'table')
    sqltype = INT

    def __init__(self, value=None, minval=-9223372036854775808,
                 maxval=9223372036854775807, **kwargs):
        """ `BigInt`
            :see::meth:Field.__init__
            @minval: (#int) minimum interger value
            @maxval: (#int) maximum integer value
        """
        super().__init__(value=value, **kwargs)
        self.minval = minval
        self.maxval = maxval

    @prepr('name', 'minval', 'maxval', 'value')
    def __repr__(self): return

    def __call__(self, value=Field.empty):
        if value is not Field.empty:
            self._set_value(int(value) if value is not None else None)
        return self.value
