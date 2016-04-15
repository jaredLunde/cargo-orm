"""

  `Cargo SQL Range Fields`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/cargo-orm

"""
import decimal

from psycopg2.extensions import register_adapter, AsIs, adapt
from psycopg2.extras import Range, DateRange as _DateRange, DateTimeRange,\
                            DateTimeTZRange, NumericRange as _NumericRange,\
                            RangeAdapter

from cargo.etc.translator.postgres import OID_map
from cargo.etc.types import *
from cargo.expressions import *
from cargo.logic import RangeLogic
from cargo.fields import Field, Date, Timestamp, TimestampTZ


__all__ = (
    'Range',
    'IntRange',
    'BigIntRange',
    'NumericRange',
    'TimestampRange',
    'TimestampTZRange',
    'DateRange')


class _RangeAdapter(RangeAdapter):
    name = "__cargo__"

    def getquoted(self):
        r = self.adapted
        if r.isempty:
            return b("'empty'::" + self.name)

        if r.lower is not None:
            a = adapt(r.lower)
            if hasattr(a, 'prepare'):
                a.prepare(self._conn)
            lower = a.getquoted()
        else:
            lower = b'NULL'

        if r.upper is not None:
            a = adapt(r.upper)
            if hasattr(a, 'prepare'):
                a.prepare(self._conn)
            upper = a.getquoted()
        else:
            upper = b'NULL'
        try:
            name = r.typname
            return name.encode() + b'(' + lower + b', ' + upper +\
                (", '%s')" % r._bounds).encode()
        except AttributeError:
            return b"%s%s, %s%s" % (r._bounds[0].encode(), lower, upper,
                                    r._bounds[1].encode())


class IntRange(Field, RangeLogic):
    OID = INTRANGE
    __slots__ = Field.__slots__
    _range_cls = _NumericRange
    _cast = int

    def __init__(self, *args, **kwargs):
        """`Integer Range`
            ==================================================================
            @value: (#tuple (lower_bound, upper_bound) or
                :class:psycopg2.extras.Range)
            ==================================================================
            :see::meth:Field.__init__
            ==================================================================
            See also: :class:Range and
                http://initd.org/psycopg/docs/extras.html#range-data-types
        """
        super().__init__(*args, **kwargs)

    def __getattr__(self, name):
        try:
            return self.__getattribute__(name)
        except AttributeError:
            return self.value.__getattribute__(name)

    def __call__(self, value=Field.empty):
        if value is not Field.empty:
            if value is not None:
                try:
                    value = self._range_cls(*map(self.cast, value))
                except TypeError:
                    if not isinstance(value, Range):
                        raise TypeError(('`%s` values must be of type tuple or'
                                         ' psycopg2.extras.Range')
                                        % self.__class__.__name__)
                    value = self._range_cls(lower=self.cast(value.lower),
                                            upper=self.cast(value.upper))
                value.typname = OID_map[self.OID]
            self.value = value
        return self.value

    def cast(self, value):
        if value is None:
            return value
        return self._cast(value)

    @property
    def upper(self):
        try:
            return self.value._upper
        except AttributeError:
            return None

    @property
    def lower(self):
        try:
            return self.value._lower
        except AttributeError:
            return None

    def set_upper(self, upper):
        try:
            self.value._upper = self._cast(upper)
        except AttributeError:
            self.__call__(self._range_cls(upper=upper))
        return

    def set_lower(self, lower):
        try:
            self.value._lower = self._cast(lower)
        except AttributeError:
            self.__call__(self._range_cls(lower=lower))
        return

    def for_json(self):
        """:see::meth:Field.for_json"""
        if self.value_is_not_null:
            return (self.lower, self.upper)
        return None

    @staticmethod
    def register_adapter():
        register_adapter(_NumericRange, IntRange.to_db)

    @staticmethod
    def to_db(value):
        return _RangeAdapter(value)


class BigIntRange(IntRange):
    OID = BIGINTRANGE
    __slots__ = Field.__slots__


class NumericRange(IntRange):
    OID = NUMRANGE
    __slots__ = Field.__slots__
    _cast = decimal.Decimal

    @staticmethod
    def register_adapter():
        register_adapter(_DateRange, DateRange.to_db)


class TimestampRange(IntRange):
    OID = TSRANGE
    __slots__ = Field.__slots__
    _cast = Timestamp().__call__
    _range_cls = DateTimeRange

    @staticmethod
    def register_adapter():
        register_adapter(DateTimeRange, TimestampRange.to_db)


class TimestampTZRange(IntRange):
    OID = TSTZRANGE
    __slots__ = Field.__slots__
    _cast = TimestampTZ().__call__
    _range_cls = DateTimeTZRange

    @staticmethod
    def register_adapter():
        register_adapter(DateTimeTZRange, TimestampTZRange.to_db)


class DateRange(IntRange):
    OID = DATERANGE
    __slots__ = Field.__slots__
    _cast = Date().__call__
    _range_cls = _DateRange

    @staticmethod
    def register_adapter():
        register_adapter(_DateRange, DateRange.to_db)
