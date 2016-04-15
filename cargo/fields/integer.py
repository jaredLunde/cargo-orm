"""

  `Cargo SQL Integer Fields`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/cargo-orm

"""
import babel.numbers
import humanize

from cargo.etc.types import *
from cargo.expressions import *
from cargo.fields.field import Field
from cargo.validators import IntValidator


__all__ = ('SmallInt', 'Int', 'BigInt')


class SmallInt(Field, NumericLogic):
    """ ======================================================================
        Field object for the PostgreSQL field type |INT2|
    """
    __slots__ = ('field_name', 'primary', 'unique', 'index', 'not_null',
                 'value', 'validator', '_alias', 'default', 'minval',
                 'maxval', 'table', 'locale')
    OID = SMALLINT
    MINVAL = -32768
    MAXVAL = 32767

    def __init__(self, minval=MINVAL, maxval=MAXVAL,
                 validator=IntValidator, locale=babel.numbers.LC_NUMERIC,
                 **kwargs):
        """ `SmallInt`
            ==================================================================
            @minval: (#int) minimum interger value
            @maxval: (#int) maximum integer value
            @locale: (#str) LC locale, .e.g., |en_DE|,
                see::class:babel.core.Locale, defaults to |en_US|
            ==================================================================
            :see::meth:Field.__init__
        """
        super().__init__(validator=validator, **kwargs)
        self.minval = minval
        self.maxval = maxval
        self.locale = locale

    def __call__(self, value=Field.empty):
        if value is not Field.empty:
            self.value = int(value) if value is not None else None
        return self.value

    def __int__(self):
        return int(self.value)

    def format(self, locale=None):
        """ :see::func:babel.numbers.format_number """
        if self.value is None or self.value is self.empty:
            return ''
        return babel.numbers.format_number(self.value,
                                           locale=locale or self.locale)

    def to_scientific(self, format=None, locale=None):
        """ :see::func:babel.numbers.format_scientific """
        if self.value is None or self.value is self.empty:
            return ''
        return babel.numbers.format_scientific(self.value,
                                               format=format,
                                               locale=locale or self.locale)

    def to_word(self):
        return humanize.intword(self.value)

    def to_apnumber(self):
        return humanize.to_apnumber(self.value)

    def for_json(self):
        """:see::meth:Field.for_json"""
        if self.value_is_not_null:
            return int(self)
        return None

    def copy(self, *args, **kwargs):
        return Field.copy(self, *args, minval=self.minval, maxval=self.maxval,
                          locale=self.locale, **kwargs)


class Int(SmallInt):
    """ ======================================================================
        Field object for the PostgreSQL field type |INT4|
    """
    __slots__ = SmallInt.__slots__
    OID = INT
    MINVAL = -2147483648
    MAXVAL = 2147483647

    def __init__(self, minval=MINVAL, maxval=MAXVAL,  **kwargs):
        """`Int`
            ==================================================================
            @minval: (#int) minimum interger value
            @maxval: (#int) maximum integer value
            @locale: (#str) LC locale, .e.g., |en_DE|,
                see::class:babel.core.Locale, defaults to |en_US|
            ==================================================================
            :see::meth:Field.__init__
        """
        super().__init__(minval=minval, maxval=maxval, **kwargs)


class BigInt(SmallInt):
    """ ======================================================================
        Field object for the PostgreSQL field type |INT8|
    """
    __slots__ = SmallInt.__slots__
    OID = BIGINT
    MINVAL = -9223372036854775808
    MAXVAL = 9223372036854775807

    def __init__(self, minval=-9223372036854775808, maxval=9223372036854775807,
                 **kwargs):
        """`BigInt`
            ==================================================================
            @minval: (#int) minimum interger value
            @maxval: (#int) maximum integer value
            @locale: (#str) LC locale, .e.g., |en_DE|,
                see::class:babel.core.Locale, defaults to |en_US|
            ==================================================================
            :see::meth:Field.__init__
        """
        super().__init__(minval=minval, maxval=maxval, **kwargs)
