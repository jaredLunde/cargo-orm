"""
  `Date/Time Logic and Operations`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
from cargo.etc import operators
from cargo.expressions import *


__all__ = ('TimeLogic', 'DateLogic', 'DateTimeLogic')


class TimeLogic(BaseNumericLogic):
    __slots__ = tuple()

    def interval(self, length, alias=None):
        return Expression(_empty, 'interval', length, alias=alias)

    def last(self, length):
        right = Expression(
            self.now(), operators.SUB, self.interval(length)).group()
        return Expression(self, operators.GE, right)

    def age(self, *args, alias=None):
        """ :see::meth:F.age """
        return F.age(self, *args, alias=alias)

    @staticmethod
    def clock_timestamp(alias=None):
        return F.clock_timestamp(alias=alias)

    @staticmethod
    def current_time(precision=None, alias=None):
        return Expression(_empty, "current_time", _empty, alias=alias)

    @staticmethod
    def current_timestamp(alias=None):
        return Expression(_empty, "current_timestamp", _empty, alias=alias)

    @staticmethod
    def localtime(alias=None):
        return Expression(_empty, "localtime", _empty, alias=alias)

    @staticmethod
    def localtimestamp(alias=None):
        return Expression(_empty, "localtimestamp", _empty, alias=alias)

    @staticmethod
    def transaction_timestamp(alias=None):
        return F.transaction_timestamp(alias=alias)

    @staticmethod
    def now(alias=None):
        return Function("now", alias=alias)

    def isfinite(self, **kwargs):
        """ isfinite(timestamp '2001-02-16 21:28:30') """
        return F.isfinite(self, **kwargs)


class DateLogic(BaseNumericLogic):
    __slots__ = tuple()

    def interval(self, length, alias=None):
        return Expression(_empty, 'interval', length)

    def last(self, length):
        interval = Expression(
            self.now(), operators.SUB, self.interval(length)).group()
        return Expression(self, operators.GE, interval)

    def age(self, *args, alias=None):
        """ :see::meth:F.age """
        return F.age(self, *args, alias=alias)

    def current_date(self, alias=None):
        return Expression(_empty, 'current_date', _empty, alias=alias)

    def date_part(self, text, **kwargs):
        """ :see::meth:F.date_part """
        return F.date_part(text, self, **kwargs)

    def date_trunc(self, text, **kwargs):
        """ :see::meth:F.date_trunc """
        return F.date_trunc(text, self, **kwargs)

    def extract(self, text, **kwargs):
        """ :see::meth:F.extract """
        return F.extract(text, self, **kwargs)

    def justify_days(self, *args, **kwargs):
        """ :see::meth:F.justify_days """
        return F.justify_days(self, *args, **kwargs)

    def justify_interval(self, *args, **kwargs):
        """ :see::meth:F.justify_interval """
        return F.justify_interval(self, *args, **kwargs)

    def timeofday(self, alias=None):
        """ :see::meth:F.timeofday """
        return F.timeofday(alias=alias)


class DateTimeLogic(DateLogic, TimeLogic):
    __slots__ = tuple()
