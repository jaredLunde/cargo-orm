"""

  `Bloom SQL Integer Fields`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/bloom-orm

"""
import babel.numbers
import humanize

from bloom.etc.types import *
from bloom.expressions import *
from bloom.fields.field import Field
from bloom.validators import IntValidator


__all__ = ('SmallInt', 'Int', 'BigInt')


class SmallInt(Field, NumericLogic):
    """ =======================================================================
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
            :see::meth:Field.__init__
            @minval: (#int) minimum interger value
            @maxval: (#int) maximum integer value
            @locale: (#str) LC locale, .e.g., |en_DE|,
                see::class:babel.core.Locale, defaults to |en_US|
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

    def copy(self, *args, **kwargs):
        cls = self._copy(*args, **kwargs)
        cls.minval = self.minval
        cls.maxval = self.maxval
        cls.locale = self.locale
        return cls

    __copy__ = copy


class Int(SmallInt):
    """ =======================================================================
        Field object for the PostgreSQL field type |INT4|
    """
    __slots__ = SmallInt.__slots__
    OID = INT
    MINVAL = -2147483648
    MAXVAL = 2147483647

    def __init__(self, minval=MINVAL, maxval=MAXVAL,  **kwargs):
        """ `Int`
            :see::meth:Field.__init__
            @minval: (#int) minimum interger value
            @maxval: (#int) maximum integer value
            @locale: (#str) LC locale, .e.g., |en_DE|,
                see::class:babel.core.Locale, defaults to |en_US|
        """
        super().__init__(minval=minval, maxval=maxval, **kwargs)


class BigInt(SmallInt):
    """ =======================================================================
        Field object for the PostgreSQL field type |INT8|
    """
    __slots__ = SmallInt.__slots__
    OID = BIGINT
    MINVAL = -9223372036854775808
    MAXVAL = 9223372036854775807

    def __init__(self, minval=-9223372036854775808, maxval=9223372036854775807,
                 **kwargs):
        """ `BigInt`
            :see::meth:Field.__init__
            @minval: (#int) minimum interger value
            @maxval: (#int) maximum integer value
            @locale: (#str) LC locale, .e.g., |en_DE|,
                see::class:babel.core.Locale, defaults to |en_US|
        """
        super().__init__(minval=minval, maxval=maxval, **kwargs)
