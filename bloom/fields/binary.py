#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""

  `Bloom SQL Binary Fields`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/bloom-orm

"""
import psycopg2.extensions

from vital.tools.encoding import uniorbytes

from bloom.etc.types import *
from bloom.expressions import *
from bloom.fields.field import Field


__all__ = ('Binary',)


class Binary(Field, BinaryLogic):
    sqltype = BINARY
    __slots__ = (
        'field_name', 'primary', 'unique', 'index', 'notNull', 'value',
        'default', 'validation', 'validation_error', '_alias', 'table')

    def __init__(self, *args, **kwargs):
        """ `Binary`
            :see::meth:Field.__init__
        """
        super().__init__(*args, **kwargs)

    def __call__(self, value=Field.empty):
        if value is not self.empty:
            self._set_value(uniorbytes(value, bytes))
        return self.value

    @property
    def real_value(self):
        if self.value is not self.empty and self.value is not None:
            return psycopg2.extensions.Binary(self.value)
        else:
            return self.default
