#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""

  `Bloom SQL Key-Value Fields`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/bloom-orm

"""
import psycopg2.extras

try:
    import ujson as json
except ImportError:
    import json

from vital.debug import prepr

from bloom.etc.types import *
from bloom.expressions import *
from bloom.fields.field import Field

# TODO: HStore field 'hstore'

__all__ = ('JSONb', 'JSON')


class JSONb(Field):
    """ - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        Field object for the PostgreSQL field type |JSONb|

        The value given to this field must be able JSON serializable. It is
        automatically encoded and decoded on insertion and retrieval.
    """
    __slots__ = (
        'field_name', 'primary', 'unique', 'index', 'notNull', 'value',
        'default', 'validation', 'validation_error', '_alias', 'table',
        'cast')
    sqltype = JSONB

    def __init__(self, value=None, cast=None, **kwargs):
        """ `JSONb`
            :see::meth:Field.__init__
            @cast: type cast for specifying the type of data should be expected
                for the value property, e.g. |dict| or |list|
        """
        self.cast = cast
        value = value or (self.cast() if self.cast is not None else None)
        super().__init__(value=value, **kwargs)

    @prepr('name', 'primary', 'index', 'value', 'cast')
    def __repr__(self): return

    def __call__(self, value=Field.empty):
        if value is not Field.empty:
            if self.cast:
                self._set_value(self.cast(value))
            else:
                self._set_value(value)
        return self.value

    @property
    def real_value(self):
        if self.value:
            return psycopg2.extras.Json(self.value, dumps=json.dumps)

    def from_json(self, value):
        """ Loads @value from JSON and inserts it as the value of the field """
        self.__call__(json.loads(value))

    def __contains__(self, name):
        return name in self.value

    def __getitem__(self, name):
        return self.value[name]

    def __setitem__(self, name, value):
        self.value[name] = value

    def __delitem__(self, name):
        del self.value[name]

    def __len__(self):
        return len(self.value)

    def __iter__(self):
        return self.value.__iter__()

    def items(self):
        return self.value.items()

    def values(self):
        return self.value.values()

    def get(self, name, default=None):
        return self.value.get(name, default)

    def remove(self, name):
        self.value.remove(name)

    def pop(self, index):
        return self.value.pop(index)

    def append(self, value):
        self.value.append(value)

    def update(self, value):
        self.value.update(value)

    def insert(self, index, value):
        self.value.insert(index, value)

    def extend(self, value):
        """ Extends a list with @value """
        self.value.extend(value)

    def reverse(self):
        """ Reverses a list in place """
        self.value.reverse()

    def sort(self, key=None, reverse=False):
        """ Sorts a list in place """
        self.value.sort(key=key, reverse=reverse)


class JSON(JSONb):
    """ - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        Field object for the PostgreSQL field type |JSON|

        The value given to this field must be JSON serializable. It is
        automatically encoded and decoded on insertion and retrieval.
    """
    __slots__ = (
        'field_name', 'primary', 'unique', 'index', 'notNull', 'value',
        'default', 'validation', 'validation_error', '_alias', 'table',
        'cast')
    sqltype = JSONTYPE

    def __init__(self, value=None, **kwargs):
        """ `JSONb`
            :see::meth:Field.__init__
        """
        super().__init__(value=value, **kwargs)
