#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""

  `Bloom SQL Date and Time Fields`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/bloom-orm

"""
import copy

from dateutil import parser as dateparser
import arrow

from vital.debug import prepr

from bloom.etc.types import *
from bloom.expressions import *
from bloom.fields import Field


__all__ = ('Time', 'Date', 'Timestamp')


class _DateFields(Field):
    __slots__ = ('_arrow',)

    def __init__(self, *args, **kwargs):
        self._arrow = None
        super().__init__(*args, **kwargs)

    def humanize(self, *args, **kwargs):
        """ :see::meth:arrow.Arrow.humanize """
        return self._arrow.humanize(*args, **kwargs)

    @property
    def real_value(self):
        if isinstance(self.value, arrow.Arrow):
            return self._arrow.datetime
        else:
            return self.value if self.value is not self.empty else self.default
    validation_value = real_value

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
        return self._arrow.for_json(*args, **kwargs)

    def utcoffset(self, *args, **kwargs):
        """ :see::meth:arrow.Arrow.utcoffset """
        return self._arrow.utcoffset(*args, **kwargs)


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


class Time(_TimeFields, TimeLogic, DateLogic):
    """ Field object for the PostgreSQL field type |TIME|. """
    __slots__ = (
        'field_name', 'primary', 'unique', 'index', 'notNull', 'value',
        'validation', 'validation_error', '_alias', 'default', '_arrow',
        'table')
    sqltype = TIME

    def __init__(self, *args, **kwargs):
        """ `Time`
            :see::meth:Field.__init__
        """
        super().__init__(*args, **kwargs)

    @prepr('name', 'value')
    def __repr__(self): return

    def __call__(self, value=Field.empty):
        if value is not Field.empty:
            if isinstance(value, (Expression, Function, Clause)):
                self._arrow = None
                self._set_value(value)
            elif not isinstance(value, arrow.Arrow):
                if isinstance(value, str):
                    value = dateparser.parse(value)
                self._arrow = arrow.get(value)
                self._set_value(self._arrow)
            else:
                self._arrow = value
                self._set_value(self._arrow)
        return self.value

    def copy(self, *args, **kwargs):
        cls = self._copy(*args, **kwargs)
        cls._arrow = copy.copy(self._arrow)
        return cls

    __copy__ = copy


class Date(_DateFields, DateLogic):
    """ - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        Field object for the PostgreSQL field type |DATE|.
    """
    __slots__ = (
        'field_name', 'primary', 'unique', 'index', 'notNull', 'value',
        'validation', 'validation_error', '_alias', 'default', '_arrow',
        'table')
    sqltype = DATE

    def __init__(self, value=None, **kwargs):
        """ `Date`
            :see::meth:Field.__init__
        """
        self._arrow = None
        super().__init__(value=value, **kwargs)

    @prepr('name', 'value')
    def __repr__(self): return

    __call__ = Time.__call__

    def copy(self, *args, **kwargs):
        cls = self._copy(*args, **kwargs)
        cls._arrow = copy.copy(self._arrow)
        return cls

    __copy__ = copy


class Timestamp(Time):
    """ - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        Field object for the PostgreSQL field type |TIMESTAMP|.
    """
    __slots__ = (
        'field_name', 'primary', 'unique', 'index', 'notNull', 'value',
        'validation', 'validation_error', '_alias', 'default', '_arrow',
        'table')
    sqltype = TIMESTAMP

    def __init__(self, value=None, **kwargs):
        """ `Timestamp`
            :see::meth:Field.__init__
        """
        super().__init__(value=value, **kwargs)

    @prepr('name', 'value')
    def __repr__(self): return
