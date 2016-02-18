#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""

  `Vital SQL Numeric and Float Fields`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/VitalSQL

"""
from vital.debug import prepr

from vital.sql.etc.types import *
from vital.sql.expressions import *
from vital.sql.fields.field import Field


__all__ = ('Decimal', 'Numeric', 'Float')


class Decimal(Field, NumericLogic):
    """ - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        Field object for the PostgreSQL field type |DECIMAL|
    """
    __slots__ = (
        'field_name', 'primary', 'unique', 'index', 'notNull', 'value',
        'validation', 'validation_error', '_alias', 'default', 'minval',
        'maxval', 'digits', 'table')
    sqltype = DECIMAL

    def __init__(self, value=None, digits=15, minval=-9223372036854775808.0,
                 maxval=9223372036854775807.0, **kwargs):
        """ `Decimal`
            :see::meth:SmallInt.__init__
            @digits: (#int) maximum digit precision
        """
        self.digits = digits
        super().__init__(value=value, **kwargs)
        self.minval = minval
        self.maxval = maxval

    @prepr('name', 'value', 'digits')
    def __repr__(self): return

    def __str__(self):
        if self.value and self.digits and self.digits != -1:
            return str(round(self.value, self.digits))
        else:
            return str(self.value)

    def __call__(self, value=Field.empty):
        if value is not Field.empty:
            if value is None:
                self._set_value(None)
            elif self.digits and self.digits != -1:
                self._set_value(round(float(value), self.digits))
            else:
                self._set_value(float(value))
        return self.value

    def copy(self, *args, **kwargs):
        cls = self._copy(*args, **kwargs)
        cls.minval = self.minval
        cls.maxval = self.maxval
        cls.digits = self.digits
        return cls

    __copy__ = copy


class Numeric(Decimal):
    """ - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        Field object for the PostgreSQL field type |NUMERIC|
    """
    __slots__ = (
        'field_name', 'primary', 'unique', 'index', 'notNull', 'value',
        'validation', 'validation_error', '_alias', 'default', 'minval',
        'maxval', 'digits', 'table')
    sqltype = DECIMAL

    def __init__(self, value=None, **kwargs):
        """ `Numeric`
            :see::meth:Decimal.__init__
            @digit: (#int) maximum digit precision
        """
        super().__init__(value=value, **kwargs)


class Float(Decimal):
    """ Field object for the PostgreSQL field type |FLOAT| """
    __slots__ = (
        'field_name', 'primary', 'unique', 'index', 'notNull', 'value',
        'validation', 'validation_error', '_alias', 'default', 'minval',
        'maxval', 'digits', 'table')
    sqltype = FLOAT

    def __init__(self, value=None, **kwargs):
        """ `Float`
            :see::meth:Decimal.__init__
            @digit: (#int) maximum digit precision
        """
        super().__init__(value=value, **kwargs)
