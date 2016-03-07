"""

  `Field-level Validators`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/bloom-orm

"""
import decimal
from bitstring import BitArray
from collections import Iterable

from bloom.etc.types import *
from bloom.exceptions import ValidationTypeError, ValidationValueError


__all__ = ('Validator', 'TypeValidator', 'NullValidator', 'LengthValidator',
           'BoundaryValidator', 'IntValidator', 'CharValidator',
           'NumericValidator')


class Validator(object):
    __slots__ = ('field', 'error', 'code')
    raises = ValidationValueError

    def __init__(self, field):
        self.field = field
        self.code = 0
        self.error = None

    @property
    def value(self):
        try:
            return self.field.validation_value
        except AttributeError:
            return self.field.value

    def validate(self):
        return True


class TypeValidator(Validator):
    TYPE_CODE = 7793
    types = ()
    raises = ValidationTypeError

    def validate_type(self):
        valid = isinstance(self.value, self.types)
        if valid:
            return True
        self.code = self.TYPE_CODE
        self.error = "TypeError ({}): this datatype must be {}, not {}".format(
            self.field.name, self.types, type(self.value))
        return False

    validate = validate_type


class NullValidator(Validator):
    NULL_CODE = 7794
    raises = ValidationValueError

    def is_nullable(self):
        """ If a field can be |NULL| and it is |NULL|, the field
            will validate.
        """
        return not self.field.not_null and (self.field.value is None or
                                            self.field.value is
                                            self.field.empty)

    def validate_null(self):
        """ If a field can be |NULL| and it is |NULL|, the field will
            validate. If a field is set to |NOT NULL| and the value is |NULL|,
            the field will not validate.
        """
        if self.field.not_null and (
           self.field.value is self.field.empty or
           self.field.value is None or
           not len(str(self.value))):
            self.code = self.NULL_CODE
            self.error = "ValueError ({}): this field cannot be NULL".format(
                self.field.name)
            return False
        return True

    validate = validate_null


class LengthValidator(Validator):
    MINLEN_CODE = 7795
    MAXLEN_CODE = 7796
    raises = ValidationValueError

    @property
    def len(self):
        try:
            return len(self.value)
        except TypeError:
            return 0

    def validate_minlen(self):
        if self.field.minlen is None or self.len >= self.field.minlen:
            return True
        self.code = self.MINLEN_CODE
        self.error = ("ValueError ({}): did not meet the minimum length " +
                      "requirement ({})").format(self.field.name,
                                                 self.field.minlen)
        return False

    def validate_maxlen(self):
        if self.field.maxlen is None or self.field.maxlen == -1:
            return True
        elif self.len <= self.field.maxlen:
            return True
        self.code = self.MAXLEN_CODE
        self.error = ("ValueError ({}): exceeded the maximum length " +
                      "boundary ({})").format(self.field.name,
                                              self.field.maxlen)
        return False

    def validate_length(self):
        if self.validate_minlen():
            return self.validate_maxlen()
        return False

    validate = validate_length


class BoundaryValidator(Validator):
    MINVAL_CODE = 7797
    MAXVAL_CODE = 7798
    raises = ValidationValueError

    def __init__(self, field):
        super().__init__(field)
        self.error = None

    @property
    def value(self):
        try:
            return self.field.validation_value
        except AttributeError:
            return self.field.value or 0

    def validate_minval(self):
        if self.field.minval is None:
            return True
        if self.value >= self.field.minval:
            return True
        self.code = self.MINVAL_CODE
        self.error = ("ValueError ({}): `{}` is less than minimum value " +
                      "({})").format(
            self.field.name, self.field.value, self.field.minval)
        return False

    def validate_maxval(self):
        if self.field.maxval is None:
            return True
        elif self.value <= self.field.maxval:
            return True
        self.code = self.MAXVAL_CODE
        self.error = ("ValueError ({}): `{}` is morethan maximum value " +
                      "({})").format(
            self.field.name, self.field.value, self.field.maxval)
        return False

    def validate_boundary(self):
        if self.validate_minval():
            return self.validate_maxval()
        return False

    validate = validate_boundary


class CharValidator(LengthValidator, NullValidator, TypeValidator):
    __slots__ = Validator.__slots__
    types = str
    raises = ValidationValueError

    def validate(self):
        if self.is_nullable():
            return True
        try:
            assert self.validate_null()
            assert self.validate_type()
            return self.validate_length()
        except AssertionError:
            return False


class ArrayValidator(CharValidator):
    __slots__ = Validator.__slots__
    types = Iterable


class BitValidator(CharValidator):
    __slots__ = Validator.__slots__
    types = BitArray


class VarbitValidator(CharValidator):
    __slots__ = Validator.__slots__
    types = BitArray


class IntValidator(BoundaryValidator, NullValidator, TypeValidator):
    __slots__ = Validator.__slots__
    types = int
    raises = ValidationValueError

    def validate(self):
        if self.is_nullable():
            return True
        try:
            assert self.validate_null()
            assert self.validate_type()
            return self.validate_boundary()
        except AssertionError:
            return False


class NumericValidator(IntValidator):
    __slots__ = Validator.__slots__
    types = (decimal.Decimal, float)
