"""

  `Field-level Validators`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2016 Jared Lunde
   http://github.com/jaredlunde/cargo-orm

"""
import decimal
import phonenumbers
from bitstring import BitArray
from collections import Iterable

from vital.tools import strings as string_tools

from cargo.etc.types import *
from cargo.exceptions import ValidationTypeError, ValidationValueError


__all__ = (
    'Validator',
    'TypeValidator',
    'NullValidator',
    'VarLengthValidator',
    'ExactLengthValidator',
    'LengthBoundsValidator',
    'BoundsValidator',
    'IntValidator',
    'CharValidator',
    'NumericValidator',
    'EnumValidator',
    'UsernameValidator',
    'EmailValidator',
    'PasswordValidator',
    'BooleanValidator',
    'PhoneNumberValidator'
)


class Validator(object):
    __slots__ = ('field', 'error', 'code')
    raises = ValidationValueError

    def __init__(self, field):
        self.field = field
        self.code = 0
        self.error = None

    def __getstate__(self):
        return {slot: getattr(self, slot) for slot in self.__slots__}

    def __setstate__(self, state):
        for k, v in state.items():
            setattr(self, k, v)

    @property
    def value(self):
        try:
            return self.field.validation_value
        except AttributeError:
            return self.field.value

    def set_error(self, type, code, message, *args, **kwargs):
        self.code = code
        self.error = ("{} ({}): {}").format(type,
                                            self.field.name,
                                            message.format(*args, **kwargs))

    def set_value_error(self, code, message, *args, **kwargs):
        self.set_error('ValueError', code, message, *args, **kwargs)

    def set_type_error(self, code, message, *args, **kwargs):
        self.set_error('TypeError', code, message, *args, **kwargs)

    def validate(self):
        return True


class TypeValidator(Validator):
    """ Validates whether or not a field can be inserted based on
        the type of the value.
    """
    TYPE_CODE = 7793
    types = ()
    raises = ValidationTypeError

    def validate_type(self):
        valid = isinstance(self.value, self.types)
        if valid:
            return True
        self.set_type_error(self.TYPE_CODE,
                            "this datatype must be `{}`, not `{}`",
                            self.types, type(self.value))
        return False

    validate = validate_type


class NullValidator(Validator):
    """ Validates whether or not a |NULL| field can be inserted based on
        the |not_null| property in the field.
    """
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
            self.set_value_error(self.NULL_CODE, "this field cannot be NULL")
            return False
        return True

    validate = validate_null


class LengthBoundsValidator(Validator):
    """ Validates whether or not a field with a |maxlen| and |minlen| property
        has a value whose length is between those numeric bounds.
    """
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
        self.set_value_error(self.MINLEN_CODE,
                             "did not meet the minimum length requirement " +
                             "({})",
                             self.field.minlen)
        return False

    def validate_maxlen(self):
        if self.field.maxlen is None or self.field.maxlen == -1:
            return True
        elif self.len <= self.field.maxlen:
            return True
        self.set_value_error(self.MAXLEN_CODE,
                             "exceeded the maximum length bounds ({})",
                             self.field.maxlen)
        return False

    def validate_length(self):
        if self.validate_minlen():
            return self.validate_maxlen()
        return False

    validate = validate_length


class ExactLengthValidator(Validator):
    """ Validates whether or not a field with a |length| property has a value
        whose length is exactly equal to the size defined in that |length|
        property.
    """
    LENGTH_CODE = 7799
    raises = ValidationValueError

    @property
    def len(self):
        try:
            return len(self.value)
        except TypeError:
            return 0

    def validate_length(self):
        if self.field.length is not None and self.field.length != -1 and\
           self.len == self.field.length:
            return True
        self.set_value_error(self.LENGTH_CODE,
                             "length of this field must be exactly ({})",
                             self.field.length)
        return False

    validate = validate_length


class VarLengthValidator(ExactLengthValidator):
    """ Validates whether or not a field with a |length| property has a value
        whose length is less than or equal to the size defined in that |length|
        property.
    """
    LENGTH_CODE = 7800

    def validate_length(self):
        if self.field.length is not None and self.field.length != -1 and\
           self.len <= self.field.length:
            return True
        self.set_value_error(self.LENGTH_CODE,
                             "length of this field must be less than or " +
                             "equal to ({})",
                             self.field.length)
        return False

    validate = validate_length


class BoundsValidator(Validator):
    """ Validates whether or not a field with a |minval| and |maxval|
        property has a value within the numeric bounds defined.
    """
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
        self.set_value_error(self.MINVAL_CODE,
                             "`{}` is less than minimum value ({})",
                             self.value,
                             self.field.minval)
        return False

    def validate_maxval(self):
        if self.field.maxval is None:
            return True
        elif self.value <= self.field.maxval:
            return True
        self.set_value_error(self.MAXVAL_CODE,
                             "`{}` is greater than maximum value ({})",
                             self.value,
                             self.field.maxval)
        return False

    def validate_bounds(self):
        if self.validate_minval():
            return self.validate_maxval()
        return False

    validate = validate_bounds


class CharValidator(LengthBoundsValidator, TypeValidator, NullValidator):
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


class BitValidator(ExactLengthValidator, TypeValidator, NullValidator):
    __slots__ = Validator.__slots__
    types = BitArray
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


class VarbitValidator(VarLengthValidator, TypeValidator, NullValidator):
    __slots__ = Validator.__slots__
    types = BitArray
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


class IntValidator(BoundsValidator, TypeValidator, NullValidator):
    __slots__ = Validator.__slots__
    types = int
    raises = ValidationValueError

    def validate(self):
        if self.is_nullable():
            return True
        try:
            assert self.validate_null()
            assert self.validate_type()
            return self.validate_bounds()
        except AssertionError:
            return False


class NumericValidator(IntValidator):
    __slots__ = Validator.__slots__
    types = (decimal.Decimal, float)


class EnumValidator(NullValidator):
    __slots__ = Validator.__slots__
    raises = ValidationValueError

    def validate(self):
        if self.is_nullable():
            return True
        try:
            assert self.validate_null()
            return self.value in self.field.types
        except AssertionError:
            return False


class BooleanValidator(NullValidator):
    __slots__ = Validator.__slots__

    def validate(self):
        if self.is_nullable():
            return True
        try:
            assert self.validate_null()
            return self.value in {True, False}
        except AssertionError:
            return False


class PasswordValidator(CharValidator):
    __slots__ = Validator.__slots__
    BLACKLISTED_CODE = 7801
    HASHING_CODE = 7802

    def validate_not_blacklisted(self):
        if self.value not in self.field.blacklist:
            return True
        self.set_value_error(self.BLACKLISTED_CODE,
                             "this password is blacklisted ({})",
                             self.value)
        return False

    def validate_is_hash(self):
        if self.field.hasher.is_hash(self.field.value):
            return True
        self.code = self.HASHING_CODE
        self.set_value_error(self.HASHING_CODE,
                             "error hashing password ({})",
                             self.value)
        return False

    def validate(self):
        if self.is_nullable():
            return True
        try:
            assert self.validate_null()
            if self.field.validation_value:
                assert self.validate_length()
                assert self.validate_not_blacklisted()
            return self.validate_is_hash()
        except AssertionError:
            return False


class UsernameValidator(CharValidator):
    __slots__ = Validator.__slots__
    types = str
    RESERVED_CODE = 7802
    FORMAT_CODE = 7803

    def validate_not_reserved(self):
        val = self.value.strip().lower()
        if val not in self.field.reserved_usernames:
            return True
        self.set_value_error(self.RESERVED_CODE,
                             "this username is taken ({})",
                             self.value)
        return False

    def validate_format(self):
        if self.field._re.match(self.value):
            return True
        self.set_value_error(self.FORMAT_CODE,
                             "username contains disallowed characters ({})",
                             self.value)
        return False

    def validate(self):
        if self.is_nullable():
            return True
        try:
            assert self.validate_null()
            assert self.validate_type()
            assert self.validate_length()
            assert self.validate_not_reserved()
            return self.validate_format()
        except AssertionError:
            return False


class EmailValidator(CharValidator):
    __slots__ = Validator.__slots__
    types = str
    FORMAT_CODE = 7804

    def validate_format(self):
        if string_tools.is_email(self.value):
            return True
        self.set_value_error(self.FORMAT_CODE,
                             "email address is malformed ({})",
                             self.value)
        return False

    def validate(self):
        if self.is_nullable():
            return True
        try:
            assert self.validate_null()
            assert self.validate_type()
            assert self.validate_length()
            return self.validate_format()
        except AssertionError:
            return False


class PhoneNumberValidator(NullValidator):
    __slots__ = Validator.__slots__
    FORMAT_CODE = 7805

    def validate_number(self):
        if phonenumbers.phonenumberutil.is_possible_number(self.value):
            return True
        self.set_value_error(self.FORMAT_CODE,
                             "phone number is malformed ({})",
                             self.value)
        return False

    def validate(self):
        if self.is_nullable():
            return True
        try:
            assert self.validate_null()
            return self.validate_number()
        except AssertionError:
            return False
