"""

  `Bloom SQL Numeric and Float Fields`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/bloom-orm

"""
import copy
import decimal
import babel.numbers

from vital.debug import prepr

from bloom.etc.types import *
from bloom.expressions import *
from bloom.fields.field import Field
from bloom.validators import NumericValidator


__all__ = ('Decimal', 'Numeric', 'Float', 'Double', 'Currency', 'Money')


class BaseDecimal(Field, NumericLogic):
    def __init__(self, value=Field.empty, precision=-1,
                 minval=-9223372036854775808.0, maxval=9223372036854775807.0,
                 context=None, rounding=decimal.ROUND_UP, digits=None,
                 locale=babel.numbers.LC_NUMERIC, validator=NumericValidator,
                 **kwargs):
        if precision == -1:
            precision = None
        else:
            precision = precision + 1
        self._context = context or decimal.Context(prec=precision,
                                                   rounding=rounding)
        self.digits = digits if digits is not None else -1
        self._quantize = None
        if self.digits > -1:
            self._quantize = decimal.Decimal(10) ** (-1 * self.digits)
        self.locale = locale
        super().__init__(value=value, validator=validator, **kwargs)
        self.minval = minval
        self.maxval = maxval

    def __call__(self, value=Field.empty):
        if value is not Field.empty:
            if value is not None:
                if isinstance(value, float):
                    value = self._context.create_decimal_from_float(value)
                else:
                    value = self._context.create_decimal(self._to_dec(value))
                if self._quantize is not None:
                    value = value.quantize(self._quantize)
            self._set_value(value)
        return self.value

    def __float__(self):
        return float(self.value)

    def copy(self, *args, **kwargs):
        cls = self._copy(*args, **kwargs)
        cls.minval = self.minval
        cls.maxval = self.maxval
        cls.locale = self.locale
        cls._context = copy.copy(self._context)
        return cls

    __copy__ = copy


class BaseDecimalFormat(object):
    def format(self, locale=None):
        """ @locale: (#str) LC locale, .e.g., |en_DE|,
                see::class:babel.core.Locale
            -> (#str) formatted decimal
        """
        if self.value is None or self.value is self.empty:
            return ''
        return babel.numbers.format_decimal(self.value,
                                            locale=locale or self.locale)

    def to_percent(self, format=None, locale=None):
        if self.value is None or self.value is self.empty:
            return ''
        return babel.numbers.format_percent(self.value,
                                            format=format,
                                            locale=locale or self.locale)

    def to_scientific(self, format=None, locale=None):
        if self.value is None or self.value is self.empty:
            return ''
        return babel.numbers.format_scientific(self.value,
                                               format=format,
                                               locale=locale or self.locale)

    @staticmethod
    def _to_dec(value):
        if isinstance(value, (decimal.Decimal, float)):
            return value
        value = ''.join(digit if digit.isdigit() or digit in {'.', ','} else ''
                        for digit in str(value))
        return babel.numbers.parse_decimal(value)

    def from_string(self, value):
        """ Fills the field with @value in its numeric/decimal form, removing
            any currency symbols or punctuation.
        """
        return self.__call__(self._to_dec(value))


class Decimal(BaseDecimal, BaseDecimalFormat):
    """ =======================================================================
        Field object for the PostgreSQL field type |DECIMAL|. Use this
        field as opposed to :class:Float or :class:Double when exact precision
        is necessary.
    """
    __slots__ = (
        'field_name', 'primary', 'unique', 'index', 'not_null', 'value',
        '_validator', '_alias', 'default', 'minval',
        'maxval', 'table', '_context', 'digits', '_quantize', 'locale')
    OID = DECIMAL
    TWOPLACES = decimal.Decimal(10) ** -2

    def __init__(self, value=Field.empty, precision=-1,
                 minval=-9223372036854775808.0, maxval=9223372036854775807.0,
                 context=None, rounding=decimal.ROUND_UP, digits=None,
                 **kwargs):
        """ `Decimal`
            :see::meth:SmallInt.__init__
            @digits: (#int) number of fixed digits to quantize
                :class:decimal.Decimal to
            @precision: (#int) maximum digit precision
            @context: (:class:decimal.Context)
            @rounding: (:attr:decimal.ROUND_UP or :attr:decimal.ROUND_DOWN)
            @locale: (#str) LC locale, .e.g., |en_DE|,
                see::class:babel.core.Locale, defaults to |en_US|
        """
        super().__init__(value, precision=precision, minval=minval,
                         maxval=maxval, context=context, rounding=rounding,
                         digits=digits, **kwargs)

    def quantize(self, *args, **kwargs):
        return self.value.quantize(*args, **kwargs)


class Numeric(Decimal):
    """ =======================================================================
        Field object for the PostgreSQL field type |NUMERIC|. Use this
        field as opposed to :class:Float or :class:Double when exact precision
        is necessary.
    """
    __slots__ = (
        'field_name', 'primary', 'unique', 'index', 'not_null', 'value',
        '_validator', '_alias', 'default', 'minval', 'maxval', 'table',
        '_context', 'digits', '_quantize')
    OID = NUMERIC

    def __init__(self, value=Field.empty, *args, **kwargs):
        """ `Numeric`
            :see::meth:Decimal.__init__
        """

        super().__init__(value=value, *args, **kwargs)


class Float(Field, NumericLogic, BaseDecimalFormat):
    """ =======================================================================
        Field object for the PostgreSQL field type |FLOAT4|
    """
    __slots__ = (
        'field_name', 'primary', 'unique', 'index', 'not_null', 'value',
        '_validator', '_alias', 'default', 'minval', 'maxval', 'digits',
        'table', 'locale')
    OID = FLOAT

    def __init__(self, value=Field.empty, digits=6,
                 minval=-9223372036854775808.0, maxval=9223372036854775807.0,
                 locale=babel.numbers.LC_NUMERIC, validator=NumericValidator,
                 **kwargs):
        """ `Float`
            :see::meth:SmallInt.__init__
            @digits: (#int) maximum digit precision
            @locale: (#str) LC locale, .e.g., |en_DE|,
                see::class:babel.core.Locale, defaults to |en_US|
        """
        self.digits = digits
        self.minval = minval
        self.maxval = maxval
        self.locale = locale
        super().__init__(value=value, validator=validator, **kwargs)

    def __call__(self, value=Field.empty):
        if value is not Field.empty:
            if value is not None:
                value = self._to_dec(value)
                if self.digits and self.digits != -1:
                    value = round(float(value), self.digits)
                else:
                    value = float(value)
            self._set_value(value)
        return self.value

    def copy(self, *args, **kwargs):
        cls = self._copy(*args, **kwargs)
        cls.minval = self.minval
        cls.maxval = self.maxval
        cls.digits = self.digits
        cls.locale = self.locale
        return cls

    __copy__ = copy


class Double(Float):
    """ =======================================================================
        Field object for the PostgreSQL field type |FLOAT8|
    """
    __slots__ = (
        'field_name', 'primary', 'unique', 'index', 'not_null', 'value',
        '_validator', '_alias', 'default', 'minval', 'maxval', 'digits',
        'table', 'locale')
    OID = DOUBLE

    def __init__(self, value=Field.empty, digits=15, **kwargs):
        """ `Double`
            :see::meth:Float.__init__
        """
        super().__init__(value=value, digits=digits, **kwargs)


class Currency(BaseDecimal):
    """ =======================================================================
        Field object for the PostgreSQL field type |DECIMAL| with
        currency formatting options. There is no strict fixed 2-digit
        precision with this field type, the precision can be mutable.
    """
    __slots__ = (
        'field_name', 'primary', 'unique', 'index', 'not_null', 'value',
        '_validator', '_alias', 'default', 'minval', 'maxval', 'table',
        '_context', 'digits', '_quantize', 'locale', 'code')
    OID = CURRENCY

    def __init__(self, value=Field.empty, code='USD',
                 minval=-92233720368547758.08, maxval=92233720368547758.07,
                 digits=2, **kwargs):
        """ `Currency`
            :see::meth:Decimal.__init__
            @code: (#str) the currency code e.g., |BTC| for Bitcoin
                or |GBP| British pounds.
        """
        self.code = code
        super().__init__(value, minval=minval, maxval=maxval,
                         precision=decimal.MAX_PREC-1, digits=digits, **kwargs)

    @prepr('name', 'code', 'str', _no_keys=True)
    def __repr__(self): return

    def __str__(self):
        return self.format()

    @property
    def str(self):
        return self.format()

    @property
    def symbol(self):
        return babel.numbers.get_currency_symbol(self.code, self.locale)

    def format(self, currency=None, format=None, locale=None,
               currency_digits=True, format_type="standard"):
        """ :see::func:babel.numbers.format_currency """
        if self.value is None or self.value is self.empty:
            return ''
        return babel.numbers.format_currency(self.value,
                                             currency or self.code,
                                             format=format,
                                             locale=locale or self.locale,
                                             currency_digits=currency_digits,
                                             format_type=format_type)

    _to_dec = staticmethod(BaseDecimalFormat._to_dec)

    def from_string(self, value):
        """ Fills the field with @value in its numeric/decimal form, removing
            any currency symbols or punctuation.
        """
        return self.__call__(self._to_dec(value))


class Money(Currency):
    """ =======================================================================
        Field object for the PostgreSQL field type |MONEY| with
        currency formatting options. There is a strict fixed 2-digit
        precision with this field type.
    """
    __slots__ = Currency.__slots__
    OID = MONEY

    def __init__(self, value=Field.empty, *args, **kwargs):
        """ `Money`
            :see::meth:SmallInt.__init__
            @currency: (#str) the currency code e.g., |BTC| for Bitcoin
                or |GBP| British pounds.
            @rounding: (:attr:decimal.ROUND_DOWN or :attr:decimal.ROUND_UP)
            @locale: (#str) LC locale, .e.g., |en_DE|,
                see::class:babel.core.Locale, defaults to |en_US|
        """
        super().__init__(value, *args, **kwargs)

    @staticmethod
    def _to_dec(value):
        if isinstance(value, decimal.Decimal):
            return value
        value = ''.join(digit if digit.isdigit() or digit in {'.', ','} else ''
                        for digit in str(value))
        return babel.numbers.parse_decimal(value).quantize(Decimal.TWOPLACES)

    @staticmethod
    def to_python(value, cur):
        if value is None:
            return value
        return Money._to_dec(value)


MONEYTYPE = reg_type('MONEYTYPE', MONEY, Money.to_python)
MONEYARRAYTYPE = reg_array_type('MONEYARRAYTYPE',
                                MONEYARRAY,
                                MONEYTYPE)
