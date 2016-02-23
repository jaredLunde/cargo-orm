#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""

  `Bloom SQL Sequenced Fields`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/bloom-orm

"""
from vital.debug import prepr

from bloom.etc.types import *
from bloom.expressions import *
from bloom.fields.field import Field


__all__ = ('Enum', 'Array')


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

    def __init__(self, types, value=None, **kwargs):
        """ `Enum`
            :see::meth:Field.__init__
            @types: (#tuple) or #list of allowed enum values

            See [this postgres guide](http://postgresguide.com/sexy/enums.html)
            for more valuable information.
        """
        self.types = tuple(types)  # An iterable containing a chain of types
        super().__init__(value=value, **kwargs)

    @prepr('name', ('types', 'purple'), 'value')
    def __repr__(self):
        return

    def __str__(self):
        return str(self.value)

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


class Array(Field):
    """ - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        Field object for the PostgreSQL field type |ARRAY|.
        The values passed to this object must be iterable.

        It can be manipulated similar to a python list.
    """
    __slots__ = (
        'field_name', 'primary', 'unique', 'index', 'notNull', 'value',
        'validation', 'validation_error', '_alias', 'default', 'minlen',
        'maxlen', 'cast', 'dimensions', 'table')
    sqltype = ARRAY

    def __init__(self, value=None, cast=str, dimensions=1, minlen=0, maxlen=-1,
                 default=None, **kwargs):
        """ `Array`
            :see::meth:Field.__init__
            @cast: (#callable) to cast the values with i.e. #str, #int
                or #float
            @dimensions: (#int) number of array dimensions or depth assigned
                to the field
        """
        value = value or []
        self.minlen = minlen
        self.maxlen = maxlen
        self.cast = cast or str
        self.dimensions = dimensions
        super().__init__(value=value, **kwargs)
        self.default = default or []

    @prepr('name', 'cast', 'dimensions', 'value')
    def __repr__(self): return

    def __call__(self, value=Field.empty):
        if value is not Field.empty:
            self._set_value(self._cast(value) if value else [])
        return self.value

    def __str__(self):
        return "{}:{}[]".format(
          str(self._cast(self.value)),
          self.field_name,
        )

    def __getitem__(self, index):
        """ -> the item at @index """
        return self.data[index]

    def __setitem__(self, index, value):
        """ Sets the item at @index to @value """
        self.data[index] = value
        self.__call__(self.data)

    def __delitem__(self, index):
        """ Deletes the item at @index """
        del self.data[index]
        self.__call__(self.data)

    def __contains__(self, value):
        """ Checks if @value is in the local array data """
        return value in self.data

    def __iter__(self):
        return iter(self.data)

    def __reversed__(self):
        return reversed(self.data)

    def _cast(self, value, dimension=1):
        """ Casts @value to its correct type, e.g. #int or #str """
        if not value:
            return []
        if dimension > self.dimensions:
            raise ValueError('Invalid dimensions ({}): max depth is set to {}'
                             .format(dimension, repr(self.dimensions)))
        data = []
        add_data = data.append
        next_dimension = dimension + 1
        for x in value:
            if not isinstance(x, (tuple, list)):
                add_data(self.cast(x))
            else:
                add_data(self._cast(x, next_dimension))
        return data

    def append(self, value):
        """ Appends @value to the array """
        self.data.append(value)
        self.__call__(self.data)

    def pop(self, index=0):
        """ Pops @index from the array """
        val = self.data.pop(index)
        self.__call__(self.data)
        return val

    def insert(self, index, value):
        """ Inserts @value to the array at @index """
        self.data.insert(index, value)
        self.__call__(self.data)

    def remove(self, value):
        """ Removes @value from the array """
        self.data.remove(value)
        self.__call__(self.data)

    def array_index(self, value):
        """ Finds the index of @value in the array """
        return self.data.index(value)

    def extend(self, value):
        """ Extends the array with @value """
        self.data.extend(value)
        self.__call__(self.data)

    def reverse(self):
        """ Reverses the array in place """
        self.data.reverse()
        self.__call__(self.data)

    def sort(self, key=None, reverse=False):
        """ Sorts the array in place """
        self.data.sort(key=key, reverse=reverse)
        self.__call__(self.data)

    def contains(self, array):
        """ Creates a |@>| SQL expression
            @array: (#list) or #tuple object

            -> SQL :class:Expression object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                condition = model.array_field.contains([1, 2])
                model.where(condition)
            ..
            |array_field @> [1,2]|
        """
        return Expression(self, "@>", ArrayItems(array))

    def is_contained_by(self, array):
        """ Creates a |<@| SQL expression
            @array: (#list) or #tuple object

            -> SQL :class:Expression object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                condition = model.array_field.is_contained_by([1, 2])
                model.where(condition)
            ..
            |array_field <@ [1,2]|
        """
        return Expression(self, "<@", ArrayItems(array))

    def overlaps(self, array):
        """ Creates a |&&| (overlaps) SQL expression
            @array: (#list) or #tuple object

            -> SQL :class:Expression object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                condition = model.array_field.overlaps([1, 2])
                model.where(condition)
            ..
            |array_field && [1,2]|
        """
        return Expression(self, "&&", ArrayItems(array))

    def all(self, target):
        """ Creates an |ALL| SQL expression
            @target: (#list) or #tuple object

            -> SQL :class:Expression object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                condition = model.array_field.all([1, 2])
                model.where(condition)
            ..
            |[1,2] = ALL(array_field)|
        """
        return Expression(target, "=", Function("ALL", self))

    def any(self, target):
        """ Creates an |ANY| SQL expression
            @target: (#list) or #tuple object

            -> SQL :class:Expression object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                condition = model.array_field.any([1, 2])
                model.where(condition)
            ..
            |[1,2] = ANY(array_field)|
        """
        return Expression(ArrayItems(target), "=", Function("ANY", self))

    def some(self, target):
        """ Creates a |SOME| SQL expression
            @target: (#list) or #tuple object

            -> SQL :class:Expression object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                condition = model.array_field.some([1, 2])
                model.where(condition)
            ..
            |[1,2] = SOME(array_field)|
        """
        return Expression(ArrayItems(target), "=", Function("SOME", self))

    def copy(self, *args, **kwargs):
        cls = self._copy(*args, **kwargs)
        cls.minlen = self.minlen
        cls.maxlen = self.maxlen
        cls.cast = self.cast
        cls.dimensions = self.dimensions
        return cls

    __copy__ = copy
