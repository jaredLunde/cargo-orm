"""

  `Cargo SQL Date and Time Fields`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/cargo-orm

"""
import copy

from psycopg2.extensions import *

from dateutil import parser as dateparser
import arrow

from cargo.etc import operators
from cargo.etc.types import *
from cargo.expressions import *
from cargo.fields import Field
from cargo.logic import TimeLogic, DateLogic, DateTimeLogic


__all__ = (
    'Time',
    'Date',
    'Timestamp',
    'TimestampTZ',
    'TimeTZ'
)


class _DateFields(Field):
    __slots__ = ('_arrow',)

    def __init__(self, *args, **kwargs):
        self._arrow = None
        super().__init__(*args, **kwargs)

    def to_natural(self, *args, **kwargs):
        """ :see::meth:arrow.Arrow.humanize """
        return self._arrow.humanize(*args, **kwargs)

    @property
    def validation_value(self):
        if isinstance(self.value, arrow.Arrow):
            return self._arrow.datetime
        else:
            return self.value if self.value is not self.empty else None

    @property
    def arrow(self):
        """ Read-only access to the local :class:arrow.Arrow instance """
        return self._arrow

    @property
    def since_epoch(self):
        return self._arrow.timestamp

    @property
    def microsecond(self):
        return self._arrow.datetime.microsecond

    @property
    def second(self):
        return self._arrow.datetime.second

    @property
    def minute(self):
        return self._arrow.datetime.minute

    @property
    def hour(self):
        return self._arrow.datetime.hour

    @property
    def day(self):
        return self._arrow.datetime.day

    @property
    def month(self):
        return self._arrow.datetime.month

    @property
    def year(self):
        return self._arrow.datetime.year

    @property
    def tzinfo(self):
        return self._arrow.datetime.tzinfo

    @property
    def timezone(self):
        return self._arrow.datetime.tzname()

    def replace(self, *args, **kwargs):
        """ Replaces the current value with the specified :mod: arrow options
            :see::meth:arrow.Arrow.replace
        """
        return self.__call__(self._arrow.replace(*args, **kwargs))

    def fromdate(self, *args, **kwargs):
        """ Sets the value of the field from a date object using :mod:arrow

            :see::meth:arrow.Arrow.fromdate
        """
        return self.__call__(self._arrow.fromdate(*args, **kwargs))

    def fromdatetime(self, *args, **kwargs):
        """ Sets the value of the field from a datetime object using :mod:arrow

            :see::meth:arrow.Arrow.fromdatetime
        """
        return self.__call__(self._arrow.fromdatetime(*args, **kwargs))

    def fromtimestamp(self, *args, **kwargs):
        """ Sets the value of the field from a timestamp using :mod:arrow

            :see::meth:arrow.Arrow.fromtimestamp
        """
        self.__call__(self._arrow.fromtimestamp(*args, **kwargs))

    def isocalendar(self, *args, **kwargs):
        """ :see::meth:arrow.Arrow.isocalendar """
        return self._arrow.isocalendar(*args, **kwargs)

    def isoformat(self, *args, **kwargs):
        """ :see::meth:arrow.Arrow.isoformat """
        return self._arrow.isoformat(*args, **kwargs)

    def isoweekday(self, *args, **kwargs):
        """ :see::meth:arrow.Arrow.isoweekday """
        return self._arrow.isoweekday(*args, **kwargs)

    def toordinal(self, *args, **kwargs):
        """ :see::meth:arrow.Arrow.toordinal """
        return self._arrow.toordinal(*args, **kwargs)

    def for_json(self, *args, **kwargs):
        """ :see::meth:arrow.Arrow.for_json """
        if self.value_is_not_null:
            return self._arrow.for_json(*args, **kwargs)
        return None

    def utcoffset(self, *args, **kwargs):
        """ :see::meth:arrow.Arrow.utcoffset """
        return self._arrow.utcoffset(*args, **kwargs)

    def format(self, *args, **kwargs):
        """ :see::meth:arrow.Arrow.format """
        if self._arrow is None:
            return None
        return self._arrow.format(*args, **kwargs)

    def clear(self):
        super().clear()
        self._arrow = None

        
class _TimeFields(_DateFields):
    __slots__ = ('_arrow',)

    def to(self, *args, **kwargs):
        """ Replaces the current value with a value reflecting the new
            timezone.

            :see::meth:arrow.Arrow.to
        """
        return self.__call__(self._arrow.to(*args, **kwargs))

    def utctimetuple(self, *args, **kwargs):
        """ :see::meth:arrow.Arrow.utctimetuple """
        return self._arrow.utctimetuple(*args, **kwargs)

    def timetz(self, *args, **kwargs):
        """ :see::meth:arrow.Arrow.timetz """
        return self._arrow.timetz(*args, **kwargs)

    def time(self, *args, **kwargs):
        """ :see::meth:arrow.Arrow.time """
        return self._arrow.time(*args, **kwargs)


class ArrowTime(arrow.Arrow):
    __slots__ = tuple()

    @staticmethod
    def to_db(val):
        return adapt(val.time())


class ArrowTimeTZ(ArrowTime):
    __slots__ = tuple()

    @staticmethod
    def to_db(val):
        return adapt(val.datetime.timetz())


class ArrowDate(ArrowTime):
    __slots__ = tuple()

    @staticmethod
    def to_db(val):
        return adapt(val.date())


class ArrowTimestamp(ArrowTime):
    __slots__ = tuple()

    @staticmethod
    def to_db(val):
        return adapt(val.naive)


class ArrowTimestampTZ(ArrowTime):
    __slots__ = tuple()

    @staticmethod
    def to_db(val):
        return adapt(val.datetime)


def _get_arrow(typ, value):
    try:
        return typ.fromdatetime(arrow.get(value))
    except TypeError:
        year, month, day = 1, 1, 1
        try:
            year, month, day = value.year, value.month, value.day
        except AttributeError:
            pass
        return typ(year=year,
                   month=month,
                   day=day,
                   hour=value.hour,
                   minute=value.minute,
                   second=value.second,
                   microsecond=value.microsecond,
                   tzinfo=value.tzinfo)


class Time(_TimeFields, TimeLogic, DateLogic):
    """ ======================================================================
        Field object for the PostgreSQL field type |TIME|
        backed by :class:arrow.Arrow
    """
    __slots__ = ('field_name', 'primary', 'unique', 'index', 'not_null',
                 'value', 'validator', '_alias', 'default', '_arrow', 'table')
    OID = TIME
    _arrow_type = ArrowTime

    def __init__(self, *args, **kwargs):
        """`Time`
            ==================================================================
            :see::meth:Field.__init__
        """
        super().__init__(*args, **kwargs)

    def __call__(self, value=Field.empty):
        if value is not Field.empty:
            if not isinstance(value, arrow.Arrow) and value is not None:
                try:
                    value = dateparser.parse(str(value))
                except ValueError:
                    pass
                self._arrow = _get_arrow(self._arrow_type, value)
            else:
                self._arrow = value
            self.value = self._arrow
        return self.value

    def __str__(self):
        return self.format() or ''

    @staticmethod
    def register_adapter():
        register_adapter(ArrowTime, ArrowTime.to_db)

    def copy(self):
        cls = Field.copy(self)
        if self._arrow is not None:
            cls._arrow = self._arrow.clone()
        return cls

    __copy__ = copy


class TimeTZ(Time):
    """ ======================================================================
        Field object for the PostgreSQL field type |TIMETZ|.
    """
    __slots__ = ('field_name', 'primary', 'unique', 'index', 'not_null',
                 'value', 'validator', '_alias', 'default', '_arrow', 'table')
    OID = TIMETZ
    _arrow_type = ArrowTimeTZ

    def __init__(self, *args, **kwargs):
        """`Time WITH timezone`
            ==================================================================
            :see::meth:Field.__init__
        """
        super().__init__(*args, **kwargs)

    @staticmethod
    def register_adapter():
        register_adapter(ArrowTimeTZ, ArrowTimeTZ.to_db)


class Date(_DateFields, DateLogic):
    """ ======================================================================
        Field object for the PostgreSQL field type |DATE|
        backed by :class:arrow.Arrow
    """
    __slots__ = ('field_name', 'primary', 'unique', 'index', 'not_null',
                 'value', 'validator', '_alias', 'default', '_arrow', 'table')
    OID = DATE
    _arrow_type = ArrowDate

    def __init__(self, value=Field.empty, **kwargs):
        """`Date`
            ==================================================================
            :see::meth:Field.__init__
        """
        self._arrow = None
        super().__init__(value=value, **kwargs)

    __call__ = Time.__call__
    __str__ = Time.__str__

    @staticmethod
    def register_adapter():
        register_adapter(ArrowDate, ArrowDate.to_db)

    copy = Time.copy
    __copy__ = copy


class Timestamp(Time):
    """ ======================================================================
        Field object for the PostgreSQL field type |TIMESTAMP|
        backed by :class:arrow.Arrow
    """
    __slots__ = ('field_name', 'primary', 'unique', 'index', 'not_null',
                 'value', 'validator', '_alias', 'default', '_arrow', 'table')
    OID = TIMESTAMP
    _arrow_type = ArrowTimestamp

    def __init__(self, value=Field.empty, **kwargs):
        """`Timestamp`
            ==================================================================
            :see::meth:Field.__init__
        """
        super().__init__(value=value, **kwargs)

    @staticmethod
    def register_adapter():
        register_adapter(ArrowTimestamp, ArrowTimestamp.to_db)


class TimestampTZ(Time):
    """ ======================================================================
        Field object for the PostgreSQL field type |TIMESTAMPTZ|
        backed by :class:arrow.Arrow
    """
    __slots__ = ('field_name', 'primary', 'unique', 'index', 'not_null',
                 'value', 'validator', '_alias', 'default', '_arrow', 'table')
    OID = TIMESTAMPTZ
    _arrow_type = ArrowTimestampTZ

    def __init__(self, value=Field.empty, **kwargs):
        """`Timestamp WITH timezone`
            ==================================================================
            :see::meth:Field.__init__
        """
        super().__init__(value=value, **kwargs)

    @staticmethod
    def register_adapter():
        register_adapter(ArrowTimestampTZ, ArrowTimestampTZ.to_db)
