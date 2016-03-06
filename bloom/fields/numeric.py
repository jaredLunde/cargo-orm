"""

  `Bloom SQL Numeric and Float Fields`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/bloom-orm

"""
from bloom.etc.types import *
from bloom.expressions import *
from bloom.fields.field import Field


__all__ = ('Decimal', 'Numeric', 'Float', 'Double')


class Decimal(Field, NumericLogic):
    """ =======================================================================
        Field object for the PostgreSQL field type |DECIMAL|. Use this
        field as opposed to :class:Float or :class:Double when exact precision
        is necessary.
    """
    __slots__ = (
        'field_name', 'primary', 'unique', 'index', 'not_null', 'value',
        'validation', 'validation_error', '_alias', 'default', 'minval',
        'maxval', 'digits', 'table')
    sqltype = DECIMAL

    def __init__(self, value=Field.empty, digits=-1,
                 minval=-9223372036854775808.0, maxval=9223372036854775807.0,
                 **kwargs):
        """ `Decimal`
            :see::meth:SmallInt.__init__
            @digits: (#int) maximum digit precision
        """
        self.digits = digits
        super().__init__(value=value, **kwargs)
        self.minval = minval
        self.maxval = maxval

    def __call__(self, value=Field.empty):
        if value is not Field.empty:
            if value is None:
                self._set_value(None)
            elif self.digits and self.digits != -1:
                self._set_value(round(float(value), self.digits))
            else:
                self._set_value(float(value))
        return self.value

    def __float__(self):
        return float(self.vlaue)

    def copy(self, *args, **kwargs):
        cls = self._copy(*args, **kwargs)
        cls.minval = self.minval
        cls.maxval = self.maxval
        cls.digits = self.digits
        return cls

    __copy__ = copy


class Numeric(Decimal):
    """ =======================================================================
        Field object for the PostgreSQL field type |NUMERIC|. Use this
        field as opposed to :class:Float or :class:Double when exact precision
        is necessary.
    """
    __slots__ = (
        'field_name', 'primary', 'unique', 'index', 'not_null', 'value',
        'validation', 'validation_error', '_alias', 'default', 'minval',
        'maxval', 'digits', 'table')
    sqltype = NUMERIC

    def __init__(self, value=Field.empty, **kwargs):
        """ `Numeric`
            :see::meth:Decimal.__init__
            @digit: (#int) maximum digit precision
        """
        super().__init__(value=value, **kwargs)


class Float(Decimal):
    """ Field object for the PostgreSQL field type |FLOAT| """
    __slots__ = (
        'field_name', 'primary', 'unique', 'index', 'not_null', 'value',
        'validation', 'validation_error', '_alias', 'default', 'minval',
        'maxval', 'digits', 'table')
    sqltype = FLOAT

    def __init__(self, value=Field.empty, digits=6, **kwargs):
        """ `Float`
            :see::meth:Decimal.__init__
            @digit: (#int) maximum digit precision
        """
        super().__init__(value=value, digits=digits, **kwargs)


class Double(Float):
    """ Field object for the PostgreSQL field type |FLOAT| """
    __slots__ = (
        'field_name', 'primary', 'unique', 'index', 'not_null', 'value',
        'validation', 'validation_error', '_alias', 'default', 'minval',
        'maxval', 'digits', 'table')
    sqltype = DOUBLE

    def __init__(self, value=Field.empty, digits=15, **kwargs):
        """ `Float`
            :see::meth:Decimal.__init__
            @digit: (#int) maximum digit precision
        """
        super().__init__(value=value, digits=digits, **kwargs)
