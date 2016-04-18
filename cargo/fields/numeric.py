"""

  `Cargo SQL Numeric and Float Fields`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/cargo-orm

"""
import copy
import decimal
import babel.numbers

from vital.debug import preprX

from cargo.etc.types import *
from cargo.expressions import *
from cargo.fields.field import Field
from cargo.validators import NumericValidator


__all__ = ('Decimal', 'Float', 'Double', 'Currency', 'Money')


class BaseDecimal(Field, NumericLogic):
    __slots__ = ('field_name', 'primary', 'unique', 'index', 'not_null',
                 'value', 'validator', '_alias', 'default', 'minval', 'maxval',
                 'table', '_context', 'decimal_places', '_quantize', 'locale',
                 'digits')
    MINVAL = -9223372036854775808.0
    MAXVAL = 9223372036854775807.0

    def __init__(self, decimal_places=None, digits=-1, minval=MINVAL,
                 maxval=MAXVAL, context=None, rounding=decimal.ROUND_UP,
                 locale=babel.numbers.LC_NUMERIC, validator=NumericValidator,
                 **kwargs):
        prec = decimal.MAX_PREC if digits is None or digits < 0 else digits
        self.digits = digits
        self._context = context or decimal.Context(prec=prec,
                                                   rounding=rounding)
        self.decimal_places = decimal_places if decimal_places is not None \
            else -1
        self._quantize = None
        if self.decimal_places > -1:
            self._quantize = decimal.Decimal(10) ** (-1 * self.decimal_places)
        self.locale = locale
        super().__init__(validator=validator, **kwargs)
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
            self.value = value
        return self.value

    def for_json(self):
        """:see::meth:Field.for_json"""
        if self.value_is_not_null:
            return float(self)
        return None

    def copy(self, *args, **kwargs):
        return Field.copy(self, *args, decimal_places=self.decimal_places,
                          minval=self.minval, maxval=self.maxval,
                          locale=self.locale, context=self._context.copy(),
                          **kwargs)

    __copy__ = copy


class BaseDecimalFormat(object):
    __slots__ = tuple()

    def format(self, locale=None):
        """ @locale: (#str) LC locale, .e.g., |en_DE|,
                see::class:babel.core.Locale
            -> (#str) formatted decimal
        """
        if self.value_is_null:
            return ''
        return babel.numbers.format_decimal(self.value,
                                            locale=locale or self.locale)

    def to_percent(self, format=None, locale=None):
        if self.value_is_null:
            return ''
        return babel.numbers.format_percent(self.value,
                                            format=format,
                                            locale=locale or self.locale)

    def to_scientific(self, format=None, locale=None):
        if self.value_is_null:
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
    """ ======================================================================
        Field object for the PostgreSQL field type |DECIMAL|. Use this
        field as opposed to :class:Float or :class:Double when exact digits
        is necessary.
    """
    __slots__ = BaseDecimal.__slots__
    OID = DECIMAL
    TWOPLACES = decimal.Decimal(10) ** -2

    def __init__(self, decimal_places=None, digits=-1,
                 minval=BaseDecimal.MINVAL, maxval=BaseDecimal.MAXVAL,
                 context=None, rounding=decimal.ROUND_UP, **kwargs):
        """`Decimal`
            ==================================================================
            @decimal_places: (#int) number of fixed decimal_places to quantize
                :class:decimal.Decimal to, will also be used as |scale| in
                the db if @digits is provided
            @digits: (#int) the total count of significant digits in the
                whole number, i.e., the number of digits to both sides of the
                decimal point, i.e. |52.01234| has |7| significant digits.
            @context: (:class:decimal.Context)
            @rounding: (:attr:decimal.ROUND_UP or :attr:decimal.ROUND_DOWN)
            @locale: (#str) LC locale, .e.g., |en_DE|,
                see::class:babel.core.Locale, defaults to |en_US|
            ==================================================================
            :see::meth:SmallInt.__init__
        """
        super().__init__(minval=minval, maxval=maxval, context=context,
                         rounding=rounding, digits=digits,
                         decimal_places=decimal_places, **kwargs)

    def quantize(self, *args, **kwargs):
        return self.value.quantize(*args, **kwargs)


class Float(Field, NumericLogic, BaseDecimalFormat):
    """ ======================================================================
        Field object for the PostgreSQL field type |FLOAT4|
    """
    __slots__ = ('field_name', 'primary', 'unique', 'index', 'not_null',
                 'value', 'validator', '_alias', 'default', 'minval', 'maxval',
                 'decimal_places', 'table', 'locale')
    OID = FLOAT
    MINVAL = BaseDecimal.MINVAL
    MAXVAL = BaseDecimal.MAXVAL

    def __init__(self, decimal_places=6, minval=MINVAL, maxval=MAXVAL,
                 locale=babel.numbers.LC_NUMERIC, validator=NumericValidator,
                 **kwargs):
        """`Float`
            ==================================================================
            @decimal_places: (#int) number of digits after the decimal point to
                round to
            @locale: (#str) LC locale, .e.g., |en_DE|,
                see::class:babel.core.Locale, defaults to |en_US|
            ==================================================================
            :see::meth:SmallInt.__init__
        """
        self.decimal_places = decimal_places
        self.minval = minval
        self.maxval = maxval
        self.locale = locale
        super().__init__(validator=validator, **kwargs)

    def __call__(self, value=Field.empty):
        if value is not Field.empty:
            if value is not None:
                value = self._to_dec(value)
                if self.decimal_places and self.decimal_places != -1:
                    value = round(float(value), self.decimal_places)
                else:
                    value = float(value)
            self.value = value
        return self.value

    for_json = Decimal.for_json

    def copy(self, *args, **kwargs):
        return Field.copy(self, *args, minval=self.minval, maxval=self.maxval,
                          decimal_places=self.decimal_places,
                          locale=self.locale, **kwargs)

    __copy__ = copy


class Double(Float):
    """ ======================================================================
        Field object for the PostgreSQL field type |FLOAT8|
    """
    __slots__ = Float.__slots__
    OID = DOUBLE

    def __init__(self, decimal_places=15, **kwargs):
        """`Double`
            ==================================================================
            :see::meth:Float.__init__
        """
        super().__init__(decimal_places=decimal_places, **kwargs)


class Currency(BaseDecimal):
    """ ======================================================================
        Field object for the PostgreSQL field type |DECIMAL| with
        currency formatting options. There is no strict fixed 2-digit
        digits with this field type, the digits can be mutable.
    """
    __slots__ = ('field_name', 'primary', 'unique', 'index', 'not_null',
                 'value', 'validator', '_alias', 'default', 'minval', 'maxval',
                 'table', '_context', 'decimal_places', '_quantize', 'locale',
                 'code')
    OID = CURRENCY
    MINVAL = -92233720368547758.08
    MAXVAL = 92233720368547758.07

    def __init__(self, decimal_places=2, digits=-1, code='USD',
                 minval=MINVAL, maxval=MAXVAL, **kwargs):
        """`Currency`
            ==================================================================
            @code: (#str) the currency code e.g., |BTC| for Bitcoin
                or |GBP| British pounds.
            ==================================================================
            :see::meth:Decimal.__init__
        """
        self.code = code
        super().__init__(minval=minval, maxval=maxval, digits=digits,
                         decimal_places=decimal_places, **kwargs)

    __repr__ = preprX('name', 'code', 'str', keyless=True)

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
    """ ======================================================================
        Field object for the PostgreSQL field type |MONEY| with
        currency formatting options. There is a strict fixed 2-digit
        digits with this field type.
    """
    __slots__ = Currency.__slots__
    OID = MONEY

    def __init__(self, *args, **kwargs):
        """`Money`
            ==================================================================
            @currency: (#str) the currency code e.g., |BTC| for Bitcoin
                or |GBP| British pounds.
            @rounding: (:attr:decimal.ROUND_DOWN or :attr:decimal.ROUND_UP)
            @locale: (#str) LC locale, .e.g., |en_DE|,
                see::class:babel.core.Locale, defaults to |en_US|
            ==================================================================
            :see::meth:SmallInt.__init__
        """
        super().__init__(*args, decimal_places=2, **kwargs)

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

    @staticmethod
    def register_adapter():
        MONEYTYPE = reg_type('MONEYTYPE', MONEY, Money.to_python)
        reg_array_type('MONEYARRAYTYPE', MONEYARRAY, MONEYTYPE)

    def copy(self, *args, **kwargs):
        return Field.copy(self,
                          *args,
                          minval=self.minval,
                          maxval=self.maxval,
                          locale=self.locale,
                          context=self._context.copy(),
                          **kwargs)

    __copy__ = copy
