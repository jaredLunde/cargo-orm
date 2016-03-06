"""

  `Field-level Validators`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/bloom-orm

"""
from collections import UserList, deque

from bloom.etc.types import *


__all__ = ('ValidationValue', 'Validate')


#
#  ``Field validation``
#


class ValidationValue(object):
    """ Used for validating :class:Field values """
    __slots__ = ('value', 'is_int', 'is_float', 'is_str', 'is_array', 'len')
    CHARS = {TEXT, CHAR, VARCHAR, PASSWORD, KEY, USERNAME, EMAIL}
    INTS = {INT, SMALLINT, BIGINT}
    FLOATS = {FLOAT, DECIMAL, NUMERIC}
    ARRAYS = {ARRAY}

    def __init__(self, field):
        value = field.value if not hasattr(field, 'validation_value') \
            else field.validation_value
        self.value = value
        self.is_int = isinstance(value, int)
        self.is_float = isinstance(value, float)
        self.is_str = isinstance(value, str)
        self.is_array = isinstance(value, (list, tuple, set, deque, UserList))
        if value is None:
            self.len = 0
        elif field.sqltype in self.ARRAYS:
            self.len = len(value)
        else:
            self.len = len(str(value))

    def __eq__(self, other):
        return self.value == other

    def __gt__(self, other):
        return self.value > other

    def __lt__(self, other):
        return self.value < other

    def __ge__(self, other):
        return self.value >= other

    def __le__(self, other):
        return self.value <= other

    def __repr__(self):
        return self.value.__repr__()

    def __str__(self):
        return str(self.value)

    def __get__(self, instance, owner):
        return instance.value


class Validate(object):
    """ Validators for :class:Field values """
    __slots__ = ('field', 'value', 'field_name', 'error')
    CHARS = {TEXT, CHAR, VARCHAR, PASSWORD, USERNAME, EMAIL}
    INTS = {INT, SMALLINT, BIGINT}
    FLOATS = {FLOAT, DECIMAL, NUMERIC}
    ARRAYS = {ARRAY}
    # DATES = {TIME, DATE, TIMESTAMP}

    def __init__(self, field):
        self.field = field
        self.value = ValidationValue(field)
        if self.field.field_name:
            self.field_name = self.field.field_name.title().replace("_", " ")
        else:
            self.field_name = None
        self.error = None

    def minval(self):
        if self.field.minval is None:
            return True
        if self.value is None and self.field.minval <= 0:
            return True
        elif self.value.value is not None and self.value >= self.field.minval:
            return True
        self.error = "{} is less than minimum value ({})".format(
            self.field_name, self.field.minval)
        return False

    def maxval(self):
        if self.field.maxval is None or self.value is None:
            return True
        elif self.value.value is not None and self.value <= self.field.maxval:
            return True
        self.error = "{} exceeded maximum value ({})".format(
            self.field_name, self.field.maxval)
        return False

    def minlen(self):
        if self.field.minlen is None or self.value.len >= self.field.minlen:
            return True
        self.error = \
            "{} did not meet the minimum length requirement ({})".format(
                self.field_name, self.field.minlen)
        return False

    def maxlen(self):
        if self.field.maxlen is None or self.field.maxlen == -1:
            return True
        elif self.value.len <= self.field.maxlen:
            return True
        self.error = \
            "{} exceeded the maximum allowed length ({})".format(
                self.field_name, self.field.maxlen)
        return False

    def str(self):
        if self.value.is_str:
            return True
        self.error = "{} for this datatype must be str(), not {}".format(
            self.field_name, type(self.value.value))
        return False

    def int(self):
        if self.value.is_int:
            return True
        self.error = "{} for this datatype must be int(), not {}".format(
            self.field_name, type(self.value.value))
        return False

    def float(self):
        if self.value.is_float:
            return True
        self.error = "{} for this datatype must be float(), not {}".format(
              self.field_name, type(self.value.value))
        return False

    def array(self):
        if self.value.is_array:
            return True
        self.error = (
            "{} for this datatype must be dict(), set(), list(), " +
            "or tuple(), not {}").format(
                self.field_name, type(self.value.value))
        return False

    def _validate_floats(self):
        if not self.float():
            #: Float required, not a str
            return False
        elif not self.minval() or not self.maxval():
            #: Minimum/Maximum value validation
            return False
        return True

    def _validate_ints(self):
        if not self.int():
            #: Int required, not a str
            return False
        elif not self.minval() or not self.maxval():
            #: Minimum/Maximum value validation
            return False
        return True

    def _validate_chars(self):
        if not self.str():
            #: Str required, not a str
            return False
        elif not self.minlen() or not self.maxlen():
            #: Minimum/Maximum length validation
            return False
        return True

    def _validate_arrays(self):
        if not self.array():
            #: Array required, not an array
            return False
        elif not self.minlen() or not self.maxlen():
            #: Minimum/Maximum length validation
            return False
        return True

    def _validate_not_null(self):
        """ If a field is set to |NOT NULL| and the value is |NULL|,
            the field will not validate.
        """
        if self.field.not_null and (
           self.value.value is self.field.empty or
           self.value.value is None or not len(str(self.value))):
            self.error = "{} cannot be 'Null'".format(self.field_name)
            return False
        return True

    def _is_valid_null(self):
        """ If a field can be |NULL| and it is |NULL|, the field will validate
        """
        if not self.field.not_null and self.value.value is None:
            #: This field can be null and it is
            return True

    def validate(self):
        #: Null validation
        if self._is_valid_null():
            #: This field can be null and it is
            return True
        elif not self._validate_not_null():
            #: This field cannot be null and it is
            return False
        #: Type-specific validation
        if hasattr(self.field, 'maxlen'):
            if self.field.sqltype in self.ARRAYS:
                # Array
                return self._validate_arrays()
            # Char
            return self._validate_chars()
        elif self.field.sqltype in self.INTS:
            # Int
            return self._validate_ints()
        elif self.field.sqltype in self.FLOATS:
            # Float
            return self._validate_floats()
        #: Return true by default otherwise
        return True
