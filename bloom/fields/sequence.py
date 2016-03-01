#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""

  `Bloom SQL Sequenced Fields`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/bloom-orm

"""
import psycopg2.extensions

from vital.tools.encoding import uniorbytes
from vital.debug import prepr

from bloom.etc.types import *
from bloom.expressions import *
from bloom.fields.field import Field
from bloom.fields.character import Text
from bloom.validators import ValidationValue


__all__ = ('Enum', 'Array')


# TODO: http://www.postgresql.org/docs/9.3/static/functions-enum.html
class Enum(Field, NumericLogic, StringLogic):
    """ - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        Field object for PostgreSQL enumerated types.

        Validates that a given value is in the specified group of enumerated
        types.

        - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        ``Usage Example``
        ..
            enum = Enum(['cat', 'dog', 'mouse'])
            enum('goat')
        ..
        |ValueError: `goat` not in ('cat', 'dog', 'mouse')|
    """
    __slots__ = (
        'field_name', 'primary', 'unique', 'index', 'notNull', 'value',
        'validation', 'validation_error', '_alias', 'default', 'types',
        'table')
    sqltype = ENUM

    def __init__(self, types, value=Field.empty, **kwargs):
        """ `Enum`
            :see::meth:Field.__init__
            @types: (#tuple) or #list of allowed enum values

            See [this postgres guide](http://postgresguide.com/sexy/enums.html)
            for more valuable information.
        """
        self.types = tuple(types)  # An iterable containing a chain of types
        super().__init__(value=value, **kwargs)

    @prepr('types', 'name', 'value', _no_keys=True)
    def __repr__(self):
        return

    def __call__(self, value=Field.empty):
        if value is not Field.empty:
            if value is None or value in self.types:
                self._set_value(value)
            else:
                raise ValueError("`{}` not in {}".format(value, self.types))
        return self.value

    def validate(self):
        value = self.value
        if value is Field.empty:
            value = None
        if (value is None and self.notNull) or \
           (value is not None and value not in self.types):
            self.validation_error = "`{}` not in {}".format(
                value, self.types)
            return False
        return self._validate()

    def copy(self, *args, **kwargs):
        cls = self._copy(*args, types=self.types, **kwargs)
        return cls

    __copy__ = copy


class Array(Field, ArrayLogic):
    """ - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        Field object for the PostgreSQL field type |ARRAY|.
        The values passed to this object must be iterable.

        It can be manipulated similar to a python list.
    """
    __slots__ = (
        'field_name', 'primary', 'unique', 'index', 'notNull', 'value',
        'validation', 'validation_error', '_alias', 'default', 'minlen',
        'maxlen', 'cast', 'type', 'dimensions', 'table')
    sqltype = ARRAY

    def __init__(self, type=None, value=Field.empty, cast=None, dimensions=1,
                 minlen=0, maxlen=-1,  default=None, **kwargs):
        """ `Array`
            :see::meth:Field.__init__
            @cast: (#callable) to cast the values with the given callable
            @type: (initialized :class:Field) the data type represented by this
                array, defaults to :class:Text
            @dimensions: (#int) number of array dimensions or depth assigned
                to the field
        """
        self.minlen = minlen
        self.maxlen = maxlen
        self.type = type.copy() if type is not None else Text()
        if cast is None:
            if self.type.sqltype in {TEXT, CHAR, VARCHAR, KEY,
                                     USERNAME, EMAIL, PASSWORD, UUIDTYPE}:
                cast = str
            elif self.type.sqltype in ValidationValue.INTS:
                cast = int
            elif self.type.sqltype in ValidationValue.FLOATS:
                cast = float
            elif self.type.sqltype == BINARY:
                def cast(x):
                    return uniorbytes(x, bytes)
        self.cast = cast or self._nocast
        self.dimensions = dimensions
        super().__init__(value=value, **kwargs)
        self.default = default

    @prepr('type', 'name', 'value', _no_keys=True)
    def __repr__(self): return

    def __call__(self, value=Field.empty):
        if value is not Field.empty:
            self._set_value(self._cast(value) if value is not None else None)
        return self.value

    def _make_list(self):
        if self.value is None or self.value is Field.empty:
            self.value = []

    def _nocast(self, val):
        return val

    def __getitem__(self, index):
        """ -> the item at @index """
        return self.value[index]

    def __setitem__(self, index, value):
        """ Sets the item at @index to @value """
        self._make_list()
        self.value[index] = value
        self.__call__(self.value)

    def __delitem__(self, index):
        """ Deletes the item at @index """
        del self.value[index]
        self.__call__(self.value)

    def __contains__(self, value):
        """ Checks if @value is in the local array data """
        return value in self.value

    def __iter__(self):
        return iter(self.value)

    def __reversed__(self):
        return reversed(self.value)

    def _cast(self, value, dimension=1):
        """ Casts @value to its correct type, e.g. #int or #str """
        if dimension > self.dimensions:
            raise ValueError('Invalid dimensions ({}): max depth is set to {}'
                             .format(dimension, repr(self.dimensions)))
        next_dimension = dimension + 1
        return [self.cast(x) if not isinstance(x, (tuple, list)) else
                self._cast(x, next_dimension)
                for x in value]

    def _select_cast(self, value, dimension=1):
        return self.cast(value)\
               if not isinstance(value, (tuple, list)) else\
               self._cast(value, dimension=dimension)

    def append(self, value):
        """ Appends @value to the array """
        self._make_list()
        self.value.append(self._select_cast(value, dimension=2))

    def pop(self, index=0):
        """ Pops @index from the array """
        return self.value.pop(index)

    def insert(self, index, value):
        """ Inserts @value to the array at @index """
        self._make_list()
        self.value.insert(index, self._select_cast(value, dimension=2))

    def remove(self, value):
        """ Removes @value from the array """
        self.value.remove(value)

    def array_index(self, value):
        """ Finds the index of @value in the array """
        return self.value.index(value)

    def extend(self, value):
        """ Extends the array with @value """
        self._make_list()
        self.value.extend(self._select_cast(value))

    def reverse(self):
        """ Reverses the array in place """
        self.value.reverse()

    def sort(self, key=None, reverse=False):
        """ Sorts the array in place """
        self.value.sort(key=key, reverse=reverse)

    def _cast_real(self, value):
        call = self.type.__call__
        return [self.type.real_value
                if not isinstance(v, (list, tuple)) else
                self._cast_real(v)
                for v in value
                if call(v) is not Field.empty]

    @property
    def real_value(self):
        if self.value is not self.empty and self.value is not None:
            value = self._cast_real(self.value)
            return value
        else:
            return self.default

    def copy(self, *args, **kwargs):
        cls = self._copy(*args, type=self.type.copy(), **kwargs)
        cls.minlen = self.minlen
        cls.maxlen = self.maxlen
        cls.cast = self.cast
        cls.dimensions = self.dimensions
        return cls

    __copy__ = copy
