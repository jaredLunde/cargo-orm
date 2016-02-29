#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""

  `Bloom SQL Character Fields`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/bloom-orm

"""
from bloom.etc.types import *
from bloom.expressions import *
from bloom.fields.field import Field


__all__ = ('Char', 'Varchar', 'Text')


class Char(Field, StringLogic):
    """ - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        Field object for the PostgreSQL field type |CHAR|
    """
    __slots__ = (
        'field_name', 'primary', 'unique', 'index', 'notNull', 'value',
        'validation', 'validation_error', '_alias', 'default', 'minlen',
        'maxlen', 'table')
    sqltype = CHAR

    def __init__(self, value=Field.empty, minlen=0, maxlen=255, **kwargs):
        """ `Char`
            Fixed-length character field

            :see::meth:Field.__init__
            @minlen: (#int) minimum length of string value
            @maxlen: (#int) minimum length of string value
        """
        super().__init__(value=value, **kwargs)
        self.maxlen = maxlen
        self.minlen = minlen

    def __call__(self, value=Field.empty):
        if value is not Field.empty:
            self._set_value(str(value) if value is not None else None)
        return self.value

    def copy(self, *args, **kwargs):
        cls = self._copy(*args, **kwargs)
        cls.minlen = self.minlen
        cls.maxlen = self.maxlen
        return cls

    __copy__ = copy


class Varchar(Char):
    """ - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        Field object for the PostgreSQL field type |VARCHAR|
    """
    __slots__ = (
        'field_name', 'primary', 'unique', 'index', 'notNull', 'value',
        'validation', 'validation_error', '_alias', 'default', 'minlen',
        'maxlen', 'table')
    sqltype = VARCHAR

    def __init__(self, value=Field.empty, minlen=0, maxlen=10485760, **kwargs):
        """ `Varchar`
            Variable-length character field.

            :see::meth:Field.__init__
            @minlen: (#int) minimum length of string value
            @maxlen: (#int) minimum length of string value
        """
        super().__init__(value=value, minlen=minlen, maxlen=maxlen, **kwargs)


class Text(Char):
    """ - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        Field object for the PostgreSQL field type |TEXT|
    """
    __slots__ = (
        'field_name', 'primary', 'unique', 'index', 'notNull', 'value',
        'validation', 'validation_error', '_alias', 'default', 'minlen',
        'maxlen', 'table')
    sqltype = TEXT

    def __init__(self, value=Field.empty, minlen=0, maxlen=-1, **kwargs):
        """ `Text`
            Variable-length character field.

            :see::meth:Field.__init__
            @minlen: (#int) minimum length of string value
            @maxlen: (#int) minimum length of string value
        """
        super().__init__(value=value, minlen=minlen, maxlen=maxlen, **kwargs)
