#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""

  `Bloom SQL Expressions`
  ``Objects for creating SQL expressions, functions and clauses``
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2016 Jared Lunde
   http://github.com/jaredlunde/bloom-orm

"""
import random
import string
from hashlib import sha1

from vital.debug import prepr
from vital.security import randhex
from bloom.etc import passwords, usernames, logic


__all__ = (
    "ArrayItems",
    "BaseExpression",
    "BaseLogic",
    "StringLogic",
    "BaseNumericLogic",
    "NumericLogic",
    "TimeLogic",
    "DateLogic",
    "DateTimeLogic",
    "Case",
    "Clause",
    "Parameterize",
    "Expression",
    "Function",
    "WindowFunctions",
    "Functions",
    "Subquery",
    "aliased",
    "safe",
    "_empty"
)


def _make_param(key):
    return "%(" + key + ")s"


class BaseLogic(object):
    """ Logical expression wrappers for PostgreSQL """

    def and_(self, other):
        """ Creates an |AND| SQL expression

            -> (SQL) :class:Expression object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``

            *Note*: You must use parentheses around expressions to call this
                expression on them, as the '&' will otherwise operate on the 1
                in the example, rather than the Expression
            ..
                condition = (model.field == 1) & (model.field == 2)
                model.where(condition)
            ..
            |field = 1 AND field != 2|
        """
        return Expression(self, logic.AND, other)

    __and__ = and_

    def or_(self, other):
        """ Creates an |OR| SQL expression

            -> SQL :class:Expression object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``

            *Note*: You must use parentheses around expressions to call this
                expression on them, as the '|' will otherwise operate on the 1
                in the example, rather than the Expression
            ..
                condition = (model.field == 1) | (model.field == 2)
                model.where(condition)
            ..
            |field = 1 OR field = 2|
        """
        return Expression(self, logic.OR, other)

    __or__ = or_

    def eq(self, other):
        """ Creates an |=| SQL expression

            -> SQL :class:Expression object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                condition = model.field == 1
                model.where(condition)
            ..
            |field  = 1|
        """
        return Expression(self, logic.EQ, other)

    __eq__ = eq

    def not_eq(self, other):
        """ Creates a |<>| SQL expression

            -> SQL :class:Expression object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                condition = model.field != 2
                model.where(condition)
            ..
            |field <> 2|
        """
        return Expression(self, logic.NE, other)

    __ne__ = not_eq

    def distinct(self, *args, **kwargs):
        """ :see::meth:Functions.distinct """
        return Functions.distinct(self, *args, **kwargs)

    def distinct_on(self, *args, **kwargs):
        """ :see::meth:Functions.distinct_on """
        return Functions.distinct_on(self, *args, **kwargs)

    def distinct_from(self, other):
        """ Creates a |DISTINCT FROM| SQL clause

            -> SQL :class:Clause object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                condition = model.field.distinct_from()
                model.where(condition)
            ..
            |DISTINCT FROM field|
        """
        return Expression(self, "{} {} {}".format(
            logic.IS, logic.DISTINCT, logic.FROM), other)

    def not_distinct_from(self, other):
        """ Creates a |NOT DISTINCT FROM| SQL clause

            -> SQL :class:Clause object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                condition = model.field.not_distinct_from()
                model.where(condition)
            ..
            |NOT DISTINCT FROM field|
        """
        return Expression(self, "{} {} {} {}".format(
            logic.IS, logic.NOT, logic.DISTINCT, logic.FROM), other)

    def in_(self, *others):
        """ Creates an |IN| SQL expression

            -> SQL :class:Expression object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                condition = model.field.in_(1, 2, 3, 4)
                model.where(condition)
            ..
            |field IN (1, 2, 3, 4)|
        """
        return Expression(self, logic.IN, others)

    __rshift__ = in_

    def not_in(self, *others):
        """ Creates a |NOT IN| SQL expression

            -> SQL :class:Expression object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                condition = model.field.not_in(1, 2, 3, 4)
                model.where(condition)
            ..
            |field NOT IN (1, 2, 3, 4)|
        """
        return Expression(self, "{} {}".format(logic.NOT, logic.IN), others)

    __lshift__ = not_in

    def is_null(self):
        """ Creates a |IS NULL| SQL expression

            -> SQL :class:Expression object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                condition = model.field.is_null()
                model.where(condition)
            ..
            |field IS NULL|
        """
        return Expression(self, "{} {}".format(logic.IS, logic.NULL))

    def not_null(self):
        """ Creates a |IS NOT NULL| SQL expression

            -> SQL :class:Expression object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                condition = model.field.not_null()
                model.where(condition)
            ..
            |field IS NOT NULL|
        """
        return Expression(self, "{} {} {}".format(
            logic.IS, logic.NOT, logic.NULL))

    def asc(self, val=False):
        """ Creates an |ASC| SQL expression

            -> SQL :class:Expression object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                model.order_by(model.field.asc())
            ..
            |field ASC|
        """
        return Expression(self if val is not True else self.real_value,
                          logic.ASC)

    def desc(self, val=False):
        """ Creates a |DESC| SQL expression

            -> SQL :class:Expression object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                model.order_by(model.field.desc())
            ..
            |field DESC|
        """
        return Expression(self if val is not True else self.real_value,
                          logic.DESC)

    def nullif(self, other, alias=None, **kwargs):
        """ :see::meth:Functions.nullif """
        return Functions.nullif(self, other, alias=alias, **kwargs)

    def count(self, alias=None, **kwargs):
        """ :see::meth:Functions.count """
        return Functions.count(self, alias=alias, **kwargs)

    def using(self, b, **kwargs):
        """ :see::meth:Functions.using """
        return Functions.using(self, b, **kwargs)

    def lag(self, *expressions, offset=None, default=None,
            window_name=None, partition_by=None, order_by=None, alias=None):
        """ :see::meth:WindowFunctions.lag """
        return Function('lag', self, offset or _empty, default or _empty)\
            .over(*expressions,
                  window_name=window_name,
                  partition_by=partition_by,
                  order_by=order_by,
                  alias=alias)

    def lead(self, *expressions, offset=None, default=None, window_name=None,
             partition_by=None, order_by=None, alias=None):
        """ :see::meth:WindowFunctions.lead """
        return Function('lead', self, offset or _empty, default or _empty)\
            .over(*expressions,
                  window_name=window_name,
                  partition_by=partition_by,
                  order_by=order_by,
                  alias=alias)

    def first_value(self, *expressions, window_name=None,
                    partition_by=None, order_by=None, alias=None):
        """ :see::meth:WindowFunctions.first_value """
        return Function('first_value', self).over(
            *expressions,
            window_name=window_name,
            partition_by=partition_by,
            order_by=order_by,
            alias=alias)

    def last_value(self, *expressions, window_name=None,
                   partition_by=None, order_by=None, alias=None):
        """ :see::meth:WindowFunctions.last_value """
        return Function('last_value', self).over(
            *expressions,
            window_name=window_name,
            partition_by=partition_by,
            order_by=order_by,
            alias=alias)

    def nth_value(self, nth_integer, *expressions, window_name=None,
                  partition_by=None, order_by=None, alias=None):
        """ :see::meth:WindowFunctions.nth_value """
        return Function('nth_value', self, nth_integer).over(
            *expressions,
            window_name=window_name,
            partition_by=partition_by,
            order_by=order_by,
            alias=alias)

    def cast(self, as_):
        """ :see::meth:Functions.func """
        return Functions.func(self, as_)

    def func(self, *args, **kwargs):
        """ :see::meth:Functions.func """
        return Functions.func(args[0], self, *args[1:], **kwargs)


class BaseNumericLogic(object):

    def lt(self, other):
        """ Creates a |<| (less than) SQL expression

            -> SQL :class:Expression object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                condition = model.field < 4
                model.where(condition)
            ..
            |field < 4|
        """
        return Expression(self, logic.LT, other)

    __lt__ = lt

    def le(self, other):
        """ Creates a |<=| (less than or equal) SQL expression

            -> SQL :class:Expression object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                condition = model.field <= 4
                model.where(condition)
            ..
            |field <= 4|
        """
        return Expression(self, logic.LE, other)

    __le__ = le

    def gt(self, other):
        """ Creates a |>| (greater than) SQL expression

            -> SQL :class:Expression object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                condition = model.field > 4
                model.where(condition)
            ..
            |field > 4|
        """
        return Expression(self, logic.GT, other)

    __gt__ = gt

    def ge(self, other):
        """ Creates a |>=| (greater than or equal) SQL expression

            -> SQL :class:Expression object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                condition = model.field >= 4
                model.where(condition)
            ..
            |field >= 4|
        """
        return Expression(self, logic.GE, other)

    __ge__ = ge

    def divide(self, other):
        """ Creates a |/| (division) SQL expression

            -> SQL :class:Expression object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                condition = model.field / 4
                model.where(condition)
            ..
            |field / 4|
        """
        return Expression(self, logic.DIV, other)

    __truediv__ = divide
    __div__ = divide

    def multiply(self, other):
        """ Creates a |*| (multiplication) SQL expression

            -> SQL :class:Expression object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                condition = model.field * 4
                model.where(condition)
            ..
            |field * 4|
        """
        return Expression(self, logic.MUL, other)

    __mul__ = multiply

    def add(self, other):
        """ Creates a |+| (addition) SQL expression

            -> SQL :class:Expression object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                condition = model.field + 4
                model.where(condition)
            ..
            |field + 4|
        """
        return Expression(self, logic.ADD, other)

    __add__ = add

    def power(self, other):
        """ Creates a |^| (power) SQL expression

            -> SQL :class:Expression object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                condition = (model.field ** 2) > 144
                model.where(condition)
            ..
            |field ^ 2|
        """
        return Expression(self, logic.EXP, other)

    __pow__ = power

    def subtract(self, other):
        """ Creates a |-| (subtraction) SQL expression

            -> SQL :class:Expression object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                condition = model.field - 4
                model.where(condition)
            ..
            |field - 4|
        """
        return Expression(self, logic.SUB, other)

    __sub__ = subtract

    def max(self, alias=None, **kwargs):
        """ :see::meth:Functions.max """
        return Functions.max(self, alias=alias, **kwargs)

    def min(self, alias=None, **kwargs):
        """ :see::meth:Functions.min """
        return Functions.min(self, alias=alias, **kwargs)

    def avg(self, alias=None, **kwargs):
        """ :see::meth:Functions.avg """
        return Functions.avg(self, alias=alias, **kwargs)

    def between(self, *others):
        """ Creates a |BETWEEN| SQL expression

            -> SQL :class:Expression object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                condition = model.field.between(10, 20)
                model.where(condition)
            ..
            |field BETWEEN 10 AND 20|
        """
        return Expression(
            self, logic.BETWEEN, Expression(others[0], logic.AND, others[1]))

    def not_between(self, a, b):
        """ Creates a |NOT BETWEEN| SQL expression

            -> SQL :class:Expression object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                condition = model.field.not_between(10, 20)
                model.where(condition)
            ..
            |field NOT BETWEEN 10 AND 20|
        """
        return Expression(
            self, "{} {}".format(logic.NOT, logic.BETWEEN),
            Expression(a, logic.AND, b))


class NumericLogic(BaseNumericLogic):

    def sum(self, alias=None, **kwargs):
        """ :see::meth:Functions.sum """
        return Functions.sum(self, alias=alias, **kwargs)

    def abs(self, alias=None, **kwargs):
        """ :see::meth:Functions.abs """
        return Functions.abs(self, alias=alias, **kwargs)

    def atan(self, alias=None, **kwargs):
        """ :see::meth:Functions.atan """
        return Functions.atan(self, alias=alias, **kwargs)

    def atan2(self, y, alias=None, **kwargs):
        """ :see::meth:Functions.atan2 """
        return Functions.atan2(self, y, alias=alias, **kwargs)

    def acos(self, alias=None, **kwargs):
        """ :see::meth:Functions.acos """
        return Functions.acos(self, alias=alias, **kwargs)

    def asin(self, alias=None, **kwargs):
        """ :see::meth:Functions.asin """
        return Functions.asin(self, alias=alias, **kwargs)

    def cos(self, alias=None, **kwargs):
        """ :see::meth:Functions.cos """
        return Functions.cos(self, alias=alias, **kwargs)

    def cot(self, alias=None, **kwargs):
        """ :see::meth:Functions.cot """
        return Functions.cot(self, alias=alias, **kwargs)

    def degrees(self, alias=None, **kwargs):
        """ :see::meth:Functions.degrees """
        return Functions.degrees(self, alias=alias, **kwargs)

    def exp(self, alias=None, **kwargs):
        """ :see::meth:Functions.exp """
        return Functions.exp(self, alias=alias, **kwargs)

    def floor(self, alias=None, **kwargs):
        """ :see::meth:Functions.floor """
        return Functions.floor(self, alias=alias, **kwargs)

    def ceil(self, alias=None, **kwargs):
        """ :see::meth:Functions.ceil """
        return Functions.ceil(self, alias=alias, **kwargs)

    def log(self, *base, alias=None, **kwargs):
        """ :see::meth:Functions.log """
        return Functions.log(self, *base, alias=alias, **kwargs)

    def mod(self, dmod=0, alias=None, **kwargs):
        """ :see::meth:Functions.mod """
        return Functions.mod(self, dmod, alias=alias, **kwargs)

    def pow(self, power=0, alias=None, **kwargs):
        """ :see::meth:Functions.pow """
        return Functions.pow(self, power, alias=alias, **kwargs)

    def radians(self, alias=None, **kwargs):
        """ :see::meth:Functions.radians """
        return Functions.radians(self, alias=alias, **kwargs)

    def round(self, alias=None, **kwargs):
        """ :see::meth:Functions.round """
        return Functions.round(self, alias=alias, **kwargs)

    def sign(self, alias=None, **kwargs):
        """ :see::meth:Functions.sign """
        return Functions.sign(self, alias=alias, **kwargs)

    def sin(self, alias=None, **kwargs):
        """ :see::meth:Functions.sin """
        return Functions.sin(self, alias=alias, **kwargs)

    def sqrt(self, alias=None, **kwargs):
        """ :see::meth:Functions.sqrt """
        return Functions.sqrt(self, alias=alias, **kwargs)

    def tan(self, alias=None, **kwargs):
        """ :see::meth:Functions.tan """
        return Functions.tan(self, alias=alias, **kwargs)


class StringLogic(object):

    def like(self, other):
        """ Creates a |LIKE| SQL expression

            -> SQL :class:Expression object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                condition = model.field.like('some%')
                model.where(condition)
            ..
            |field LIKE 'some%'|
        """
        return Expression(self, logic.LIKE, other)

    __mod__ = like

    def not_like(self, other):
        """ Creates a |NOT LIKE| SQL expression

            -> SQL :class:Expression object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                condition = model.field.not_like('some%')
                model.where(condition)
            ..
            |field NOT LIKE 'some%'|
        """
        return Expression(self, "{} {}".format(logic.NOT, logic.LIKE), other)

    def ilike(self, other):
        """ Creates an |ILIKE| SQL expression

            -> SQL :class:Expression object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                condition = model.field.ilike('some%')
                model.where(condition)
            ..
            |field ILIKE 'some%'|
        """
        return Expression(self, logic.ILIKE, other)

    __xor__ = ilike

    def not_ilike(self, other):
        """ Creates a |NOT ILIKE| SQL expression

            -> SQL :class:Expression object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                condition = model.field.not_ilike('some%')
                model.where(condition)
            ..
            |field NOT ILIKE 'some%'|
        """
        return Expression(self, "{} {}".format(logic.NOT, logic.ILIKE), other)

    def startswith(self, other):
        """ Creates an |ILIKE| SQL expression

            -> SQL :class:Expression object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                condition = model.field.startswith('hello')
                model.where(condition)
            ..
            |field ILIKE 'hello%'|
        """
        return Expression(self, logic.ILIKE, "{}%".format(other))

    def endswith(self, other):
        """ Creates an |ILIKE| SQL expression

            -> SQL :class:Expression object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                condition = model.field.endswith('world')
                model.where(condition)
            ..
            |field ILIKE '%world'|
        """
        return Expression(self, logic.ILIKE, "%{}".format(other))

    def contains(self, other):
        """ Creates an |ILIKE| SQL expression

            -> SQL :class:Expression object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                condition = model.field.contains('llo wor')
                model.where(condition)
            ..
            |field ILIKE '%llo wor%'|
        """
        return Expression(self, logic.ILIKE, "%{}%".format(other))

    def similar_to(self, other):
        """ Creates a |SIMILAR TO| SQL expression

            -> SQL :class:Expression object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                condition = model.field.similar_to('%(b|d)%')
                model.where(condition)
            ..
            |field SIMILAR TO '%(b|d)%'|
        """
        return Expression(self, logic.SIMILAR_TO, other)

    def not_similar_to(self, other):
        """ Creates a |NOT SIMILAR TO| SQL expression

            -> SQL :class:Expression object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                condition = model.field.not_similar_to('%(b|d)%')
                model.where(condition)
            ..
            |field NOT SIMILAR TO '%(b|d)%'|
        """
        return Expression(
            self, "{} {}".format(logic.NOT, logic.SIMILAR_TO), other)

    def posix(self, other, op="~", invert=False):
        """ Creates a |POSIX| SQL expression

            -> SQL :class:Expression object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                condition = model.field.posix('(b|d)', op='~')
                model.where(condition)
            ..
            |field ~ '(b|d)'|
        """
        return Expression(
            self if not invert else other, op, other if not invert else self)

    def concat_ws(self, *others, separator=',', **kwargs):
        """ :see::meth:Functions.concat_ws """
        return Functions.concat_ws(self, *others, separator=separator,
                                   **kwargs)

    def regexp_replace(self, pattern, repl, *flags, **kwargs):
        """ :see::meth:Functions.regexp_replace """
        return Functions.regexp_replace(self, pattern, repl, *flags, **kwargs)

    def regexp_matches(self, pattern, *flags, **kwargs):
        """ :see::meth:Functions.regexp_matches """
        return Functions.regexp_matches(self, pattern, *flags, **kwargs)

    def concat(self, *others, **kwargs):
        """ :see::meth:Functions.concat """
        return Functions.concat(self, *others, **kwargs)


class TimeLogic(BaseNumericLogic):

    def interval(self, length, alias=None):
        return Expression(_empty, 'interval', length, alias=alias)

    def last(self, length):
        return Expression(
            self, logic.GE, Expression(self.now(), logic.SUB, self.interval(
                length)).group())

    def age(self, *args, alias=None):
        """ :see::meth:Functions.age """
        return Functions.age(self, *args, alias=alias)

    @staticmethod
    def clock_timestamp(alias=None):
        return Functions.clock_timestamp(alias=alias)

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
        return Functions.transaction_timestamp(alias=alias)

    @staticmethod
    def now(alias=None):
        return Function("now", alias=alias)

    def isfinite(self, **kwargs):
        """ isfinite(timestamp '2001-02-16 21:28:30') """
        return Functions.isfinite(self, **kwargs)


class DateLogic(BaseNumericLogic):

    def interval(self, length, alias=None):
        return Expression(_empty, 'interval', length)

    def last(self, length):
        interval = Expression(
            self.Today(), logic.SUB, self.interval(length)).group()
        return Expression(self, logic.GE, interval)

    def age(self, *args, alias=None):
        """ :see::meth:Functions.age """
        return Functions.age(self, *args, alias=alias)

    def current_date(self, alias=None):
        return Expression(_empty, 'current_date', _empty, alias=alias)

    def date_part(self, text, **kwargs):
        """ :see::meth:Functions.date_part """
        return Functions.date_part(text, self, **kwargs)

    def date_trunc(self, text, **kwargs):
        """ :see::meth:Functions.date_trunc """
        return Functions.date_trunc(text, self, **kwargs)

    def extract(self, text, **kwargs):
        """ :see::meth:Functions.extract """
        return Functions.extract(text, self, **kwargs)

    def justify_days(self, *args, **kwargs):
        """ :see::meth:Functions.justify_days """
        return Functions.justify_days(self, *args, **kwargs)

    def justify_interval(self, *args, **kwargs):
        """ :see::meth:Functions.justify_interval """
        return Functions.justify_interval(self, *args, **kwargs)

    def timeofday(self, alias=None):
        return Functions.timeofday(alias=alias)


class DateTimeLogic(DateLogic, TimeLogic):
    pass


class Subquery(BaseLogic, NumericLogic,  DateTimeLogic, StringLogic):
    """ You must use this wrapper if you want to use subqueries within the
        models, as plain text is parameterized for safety.

        Incorrect::
        |model.where("(SELECT MAX(posts.likes) FROM posts) > 1000")|

        Corrected::
        ..
            model = Model()
            posts = PostsModel()

            # Creates a subquery which will not be executed until its parent
            # query is
            post.subquery()
            posts_subquery = posts.select(posts.likes.max())

            # Executes query in model
            model.where(posts_subquery > 1000)
            model.select()
        ..
    """
    __slots__ = ('subquery', 'alias', 'query', 'string', 'params')

    def __init__(self, query, alias=None):
        """ `Subquery`
            Thin wrapper for including subqueries within models.

            @query: :class:Query object
            @alias: #str name to alias the subquery with
        """
        self.subquery = query
        self.alias = str(alias) if alias else query.alias
        self.query = "({}) {}".format(
            query.query, self.alias if self.alias else "").strip()
        self.string = query.string
        self.params = query.params

    @prepr('query')
    def __repr__(self): return

    def __str__(self):
        return self.query

    def exists(self, alias=None):
        return Functions.exists(self, alias=alias)

    def not_exists(self, alias=None):
        return Functions.not_exists(self, alias=alias)


class BaseExpression(object):

    def _get_param_key(self, item):
        """ Sets the parameter key for @item """
        for key, item2 in self.params.items():
            if item == item2:
                return _make_param(key)
        key = randhex(12, random)
        self.params[key] = item
        return _make_param(key)

    def _inherit_parameters(self, *items):
        """ Inherits the parameters of other :mod:bloom objects into this
            one.

            @items: :mod:bloom objects
        """
        self.params.update({
            k: v
            for item in items
            for k, v in (
                item.params if hasattr(item, 'params') and
                isinstance(item.params, dict) else {}
            ).items()
        })

    def _parameterize(self, item, use_field_name=False):
        str_item = (BaseExpression, BaseLogic, NumericLogic, DateLogic,
                    TimeLogic, safe)
        if isinstance(item, str_item):
            #: These should already be parameterized
            if hasattr(item, 'sqltype'):
                #: Fields
                string = str(
                    item.name if not use_field_name else item.field_name)
            elif use_field_name is not None and \
                    hasattr(item, 'use_field_name'):
                #: Expressions and functions
                string = item.compile(use_field_name=use_field_name)
            else:
                #: Queries and other
                string = str(item)
            exp = string
        elif item is _empty:
            #: Empty item
            exp = ""
        else:
            #: Anything else gets parameterized
            if isinstance(item, bytes):
                raise TypeError("Cannot cast type `bytes`. Bytes objects " +
                                "must be wrapped with `psycopg2.Binary`")
            item = item if not hasattr(item, 'real_value') else \
                item.real_value
            exp = self._get_param_key(item)
        return exp

    def _compile_expressions(self, *items, use_field_name=False):
        """ Decides what to do with the @items this object receives based on
            the type of object it is receiving and formats accordingly,
            inheriting parameters when necessary.

            -> #str expression
        """
        return [self._parameterize(item, use_field_name)
                for item in items]


class __empty(object):
    @property
    def string(self):
        return self

    def __len__(self):
        return 0


_empty = __empty()


class Expression(BaseExpression, BaseLogic, NumericLogic):
    """ You must use this wrapper if you want to make expressions within the
        models, as plain text is parameterized for safety.

        Incorrect::
        |model.where("model.id > 1000")|

        Corrected::
        |model.where(model.id > 1000)|
          or
        |model.where(Expression(model.id, ">", 1000))|

        - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        ``Usage Example``
        ..
            e = Expression(model.id, "BETWEEN", Expression(10, 'AND', 20))
        ..
        |model.id BETWEEN 10 AND 20|
    """
    __slots__ = ('string', 'left', 'right', 'operator', 'group_op', 'params',
                 'alias', 'use_field_name')

    def __init__(self, left, operator, right=_empty, params=None, alias=None,
                 use_field_name=False):
        """ `Expression`
            Formats and parameterizes SQL expressions.

            @left: #str operator or object
            @operator: #str operator or object. This does NOT get parameterized
                so it is completely unsafe to put user submitted data here.
            @right: #str operator or object
        """
        self.string = None
        self.left = left
        self.right = right
        self.operator = operator
        self.group_op = None
        self.params = params or dict()
        self.alias = alias
        self.use_field_name = use_field_name
        self.compile()

    @prepr('operator', 'string', _break=False)
    def __repr__(self): return

    def __str__(self):
        return self.string or ""

    def group_and(self):
        self.group_op = "AND"
        self.compile()
        return self

    def group_or(self):
        self.group_op = "OR"
        self.compile()
        return self

    def group(self):
        self.group_op = " "
        self.compile()
        return self

    def compile(self, use_field_name=None):
        """ Turns the expression into a string """
        if use_field_name is not None:
            self.use_field_name = use_field_name
        self._inherit_parameters(self.left, self.operator, self.right)
        left, right = self._compile_expressions(
            self.left, self.right, use_field_name=self.use_field_name)
        self.string = "{} {} {}".format(left, self.operator, right).strip()
        if self.alias:
            self.string = "{} {}".format(self.string, self.alias)
        if self.group_op is not None:
            self.string = "({}) {}".format(
                self.string, self.group_op or "").strip()
        return self.string


class Parameterize(Expression):
    """ ``Usage Example``
        ..
            ORM().subquery().where(Parameterize(1)).select()
        ..
        |model.id BETWEEN 10 AND 20|
    """
    __slots__ = ('string', 'values', 'params', 'alias')

    def __init__(self, *values, alias=None, params=None):
        """`Parameterize`
            Parameterizes plain text
            @value: the value to parameterize
        """
        self.string = None
        self.values = values
        self.params = params or dict()
        self.alias = alias
        self.compile()

    @prepr('value', 'string', _break=False)
    def __repr__(self): return

    def compile(self):
        """ Turns the expression into a string """
        parameter = ""
        for v in self.values:
            parameter += self._parameterize(v)
            if hasattr(v, 'params'):
                self.params.update(v.params)
        self.string = "{} {}".format(
            parameter, self.alias if self.alias else "").strip()
        return self.string


class Clause(BaseExpression):
    """ You must use this wrapper if you want to create clauses within the
        models. Clauses are tied to the :class:Query objects to ensure
        proper formatting.
    """
    __slots__ = ('clause', 'params', 'args', 'alias', 'string',
                 'use_field_name', 'join_with', 'wrap')

    def __init__(self, clause, *args, join_with=" ", wrap=False, params=None,
                 use_field_name=False, alias=None):
        """`Clause`
            Formats SQL clauses. These are only intended for use within the
            :class:bloom.Query and bloom.QueryState objects, as they
            are for building the foundation of the SQL query.

            @clause: (#str) name of the clause
            @*args: objects to include in the clause
            @join_with: (#str) delimeter to join @*args with
            @wrap: (#bool) True to wrap @args with parantheses when formatting
            @use_field_name: (#bool) True to use :class:Field field names
                instead of full names when parsing expressions
            @alias: (#str) name to alias the clause with
        """
        self.clause = clause.upper().strip()
        self.params = params or dict()
        self.args = list(args)
        self.alias = alias
        self.string = None
        self.use_field_name = use_field_name
        self.join_with = join_with
        self.wrap = wrap
        self.compile()

    @prepr('string')
    def __repr__(self): return

    def __str__(self):
        return self.string

    def __call__(self):
        return self.string

    def compile(self, join_with=None, wrap=None, use_field_name=None):
        """ Turns the clause into a string """
        if join_with is not None:
            self.join_with = join_with
        if wrap is not None:
            self.wrap = wrap
        if use_field_name is not None:
            self.use_field_name = use_field_name
        clause_params = filter(lambda x: len(x), self._compile_expressions(
                *self.args, use_field_name=self.use_field_name))
        self._inherit_parameters(*self.args)
        clause_params = self.join_with.join(map(str, clause_params)) \
            if clause_params else ""
        if self.wrap:
            clause_params = "({})".format(clause_params)
        self.string = "{} {} {}".format(
            self.clause, clause_params, self.alias or "").strip()
        return self.string


class Case(BaseExpression):
    """ - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        ``Usage Example`
        `
        ..
            orm.select(Case(fielda.eq(1), 'one',
                            fielda.eq(2), 'two',
                            el='three'))
        ..
        |SELECT CASE WHEN foo.bar = 1 THEN 'one'|
        |            WHEN foo.bar = 2 THEN 'two'|
        |            ELSE 'three'               |
        |       END                             |
    """
    __slots__ = ('conditions', '_el', 'params', 'string', 'use_field_name',
                 'alias')

    def __init__(self, *when_then, el=None, use_field_name=False, alias=None):
        """ `Case Statements`

            @*when_then: a series of |when, then, when, then| expressions
                e.g. |Case(fielda == 1, 'one', fielda == 2, 'two')|
            @el: the default value - creates the |ELSE| statement in the |CASE|
            @use_field_name: (#bool) True to use the short name |field| of the
                field rather than the full |table.field| name
        """
        self.conditions = list(when_then)
        self._el = Parameterize(el) if el is not None else el
        self.params = {}
        self.alias = alias
        self.use_field_name = use_field_name
        self.compile()

    @prepr('string')
    def __repr__(self): return

    def __str__(self):
        return self.string

    def __call__(self):
        return self.string

    def when(self, *when_then):
        """ Creates the |WHEN ... THEN| clause for the |CASE|

            @*when_then: a series of |when, then, when, then| expressions
                e.g. |Case(fielda == 1, 'one', fielda == 2, 'two')|
        """
        self.conditions.extend(when_then)
        self.compile()

    def el(self, val):
        """ Creates the |ELSE| clause for the |CASE|

            @val: the default value - creates the |ELSE| statement
                in the |CASE|
        """
        self._el = Parameterize(val) if val is not None else val
        self.compile()

    def compile(self, use_field_name=None):
        """ Compiles the object to a string """
        if use_field_name is not None:
            self.use_field_name = use_field_name
        self._inherit_parameters(*self.conditions)
        expressions = list(filter(lambda x: len(x), self._compile_expressions(
            *self.conditions, use_field_name=self.use_field_name)))
        when_thens = " ".join(
            "WHEN {} THEN {}".format(when, then)
            for when, then in zip(expressions[0::2], expressions[1::2])
        )
        el = ""
        if self._el is not None:
            self._inherit_parameters(self._el)
            el = "ELSE " + self._el.string
        string = "CASE {} {}".format(when_thens, el)
        self.string = "{} END {}".format(
            string.strip(),
            self.alias or ""
        ).strip()
        return self.string


class Function(BaseExpression, BaseLogic, NumericLogic, DateTimeLogic,
               StringLogic):
    """ You must use this wrapper if you
        want to make function calls within the models, as plain text is
        parameterized for safety.

        Incorrect::
        |model.where("age(model.timestamp, '1980-03-19')")|

        Corrected::
        |model.where(Function('age', model.timestamp, '1980-03-19'))|

        - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        ``Usage Example``
        ..
            f = Function.func('now')
        ..
        |now()|

        ..
            f = Function.func('generate_series', 1, 2, 3, 4)
        ..
        |generate_series(1, 2, 3, 4)|

        ..
            f = Function.func('generate_series', 1, 2, 3, 4,
                              alias="fun_series")
        ..
        |generate_series(1, 2, 3, 4) fun_series|
    """
    __slots__ = ('string', 'func', 'args', 'params', 'alias', 'use_field_name')

    def __init__(self, func, *args, use_field_name=False, params=None,
                 alias=None):
        """ `Function`
            Formats and aliases SQL functions.

            @func: #str name of func
            @*args: arguments to pass to the function
            @use_field_name: #bool True to use the 'field_name' attribute in
                :class:Field objects instead of 'name'
            @alias: #str name to alias the function with
        """
        self.string = None
        self.func = func
        self.args = tuple(arg for arg in args if arg is not _empty)
        self.alias = alias
        self.params = params or dict()
        self.use_field_name = use_field_name
        self.compile()

    @prepr('string', 'alias')
    def __repr__(self): return

    def __str__(self):
        return self.string

    def __call__(self):
        return self.string

    def over(self, *expressions, window_name=None, partition_by=None,
             order_by=None, alias=None):
        """ Creates a |WINDOW| function |OVER| this function.

            @expressions: (:class:BaseExpression) creates an OVER clause
                with these expressions rather than @partition_by and
                @order_by
            @window_name: (#str) name of the WINDOW clause created with
                :meth:ORM.window
            @partition_by: (:class:Field) field to partition by
            @order_by: (:class:Field|:class:BaseExpression) expression clause
                for the ORDER BY clause e.g. |order_by=your_field.desc()|
            ..
            window = model.field.avg().over(partition_by=model.partition_field,
                                            order_by=model.field.desc(),
                                            alias='field_alias')
            model.select(window)
            ..
            |SELECT avg(foo.field) OVER (PARTITION BY foo.partition_field     |
            |                            ORDER BY foo.field DESC) field_alias |

            -> (:class:Expression) window function expression
        """
        wrap = True
        if not expressions:
            expressions = []
            if partition_by is not None:
                expressions.append(Clause('PARTITION BY', partition_by))
            if order_by is not None:
                expressions.append(Clause('ORDER BY', order_by))
        elif len(expressions) == 1 and isinstance(expressions[0], str):
            #: Window name alias
            expressions = [aliased(expressions[0])]
            wrap = False
        return Expression(self,
                          Clause('OVER', *expressions, wrap=wrap,
                                 join_with=" "),
                          alias=alias)

    def compile(self, use_field_name=None):
        """ Turns the function into a string """
        if use_field_name is not None:
            self.use_field_name = use_field_name
        self._inherit_parameters(*self.args)
        func_params = filter(lambda x: len(x), self._compile_expressions(
            *self.args, use_field_name=self.use_field_name))
        func_params = ", ".join(map(str, func_params)) \
            if func_params is not None else ""
        self.string = "{}({}) {}".format(
          self.func, func_params, self.alias or "").strip()
        return self.string


class WindowFunctions(object):

    @staticmethod
    def row_number(*expressions, window_name=None, partition_by=None,
                   order_by=None, alias=None):
        """ Creates a |row_number()| window :class:Function.

            The function returns a bigint as the number of the current row
            within its partition, counting from 1

            -> (:class:Function) object
        """
        return Function('row_number').over(*expressions,
                                           window_name=window_name,
                                           partition_by=partition_by,
                                           order_by=order_by,
                                           alias=alias)

    @staticmethod
    def rank(*expressions, window_name=None, partition_by=None,
             order_by=None, alias=None):
        """ Creates a |rank()| window :class:Function.

            This function returns a bigint as the rank of the current row
            with gaps; same as row_number of its first peer.

            -> (:class:Function) object
        """
        return Function('rank').over(*expressions,
                                     window_name=window_name,
                                     partition_by=partition_by,
                                     order_by=order_by,
                                     alias=alias)

    @staticmethod
    def dense_rank(*expressions, window_name=None, partition_by=None,
                   order_by=None, alias=None):
        """ Creates a |dense_rank()| window :class:Function.

            This function returns a bigint as the rank of the current
            row without gaps; this function counts peer groups.

            -> (:class:Function) object
        """
        return Function('dense_rank').over(*expressions,
                                           window_name=window_name,
                                           partition_by=partition_by,
                                           order_by=order_by,
                                           alias=alias)

    @staticmethod
    def percent_rank(*expressions, window_name=None, partition_by=None,
                     order_by=None, alias=None):
        """ Creates a |percent_rank()| window :class:Function.

            This function returns a double precision as the relative
            rank of the current row: (rank - 1) / (total rows - 1)

            -> (:class:Function) object
        """
        return Function('percent_rank').over(*expressions,
                                             window_name=window_name,
                                             partition_by=partition_by,
                                             order_by=order_by,
                                             alias=alias)

    @staticmethod
    def cume_dist(*expressions, window_name=None, partition_by=None,
                  order_by=None, alias=None):
        """ Creates a |cume_dist()| window :class:Function.

            This function returns a double precision as the relative
            rank of the current row: (number of rows preceding or peer
            with current row) / (total rows)

            -> (:class:Function) object
        """
        return Function('cume_dist').over(*expressions,
                                          window_name=window_name,
                                          partition_by=partition_by,
                                          order_by=order_by,
                                          alias=alias)

    @staticmethod
    def ntile(num_buckets, *expressions, window_name=None,
              partition_by=None, order_by=None, alias=None):
        """ Creates a |ntile()| window :class:Function.

            This function returns a integer as the integer ranging from 1
            to the argument value, dividing the partition as equally
            as possible.

            @num_buckets: (#int) number of partitions to divide into

            -> (:class:Function) object
        """
        return Function('ntile', num_buckets).over(*expressions,
                                                   window_name=window_name,
                                                   partition_by=partition_by,
                                                   order_by=order_by,
                                                   alias=alias)

    @staticmethod
    def lag(field, *expressions, offset=None, default=None,
            window_name=None, partition_by=None, order_by=None, alias=None):
        """ Creates a |lag()| window :class:Function.

            This function returns a same type as value as the value
            evaluated at the row that is offset rows before the current row
            within the partition; if there is no such row, instead return
            default (which must be of the same type as value). Both offset
            and default are evaluated with respect to the current row.
            If omitted, offset defaults to 1 and default to null.

            @field: (:class:Field)
            @offset: (#int) number of offset rows
            @default: (:class:Field) the default if no row is found

            -> (:class:Function) object
        """
        return Function('lag', field, offset or _empty, default or _empty)\
            .over(*expressions,
                  window_name=window_name,
                  partition_by=partition_by,
                  order_by=order_by,
                  alias=alias)

    @staticmethod
    def lead(field, *expressions, offset=None, default=None, window_name=None,
             partition_by=None, order_by=None, alias=None):
        """ Creates a |lead()| window :class:Function.

            This function returns a same type as value as the value
            evaluated at the row that is offset rows after the current row
            within the partition; if there is no such row, instead return
            default (which must be of the same type as value). Both offset
            and default are evaluated with respect to the current row. If
            omitted, offset defaults to 1 and default to null.

            @field: (:class:Field)
            @offset: (#int) number of offset rows
            @default: (:class:Field) the default if no row is found

            -> (:class:Function) object
        """
        return Function('lead', field, offset or _empty, default or _empty)\
            .over(*expressions,
                  window_name=window_name,
                  partition_by=partition_by,
                  order_by=order_by,
                  alias=alias)

    @staticmethod
    def first_value(field, *expressions, window_name=None,
                    partition_by=None, order_by=None, alias=None):
        """ Creates a |first_value()| window :class:Function.

            This function returns a same type as value as the value
            evaluated at the row that is the first row of the window frame.

            @field: (:class:Field)

            -> (:class:Function) object
        """
        return Function('first_value', field).over(
            *expressions,
            window_name=window_name,
            partition_by=partition_by,
            order_by=order_by,
            alias=alias)

    @staticmethod
    def last_value(field, *expressions, window_name=None,
                   partition_by=None, order_by=None, alias=None):
        """ Creates a |last_value()| window :class:Function.

            This function returns a same type as value as the value
            evaluated at the row that is the last row of the window frame.

            @field: (:class:Field)

            -> (:class:Function) object
        """
        return Function('last_value', field).over(
            *expressions,
            window_name=window_name,
            partition_by=partition_by,
            order_by=order_by,
            alias=alias)

    @staticmethod
    def nth_value(field, nth_integer, *expressions, window_name=None,
                  partition_by=None, order_by=None, alias=None):
        """ Creates a |nth_value()| window :class:Function.

            This function returns a same type as value as the value
            evaluated at the row that is the nth row of the window
            frame (counting from 1); null if no such row.

            @field: (:class:Field)
            @nth_integer: (#int) nth row of the window frame

            -> (:class:Function) object
        """
        return Function('nth_value', field, nth_integer).over(
            *expressions,
            window_name=window_name,
            partition_by=partition_by,
            order_by=order_by,
            alias=alias)


class Functions(WindowFunctions):
    """ Wrapper functions for common SQL functions, providing a simpler path
        to respective :class:Function objects.
    """
    @staticmethod
    def coalesce(*vals, alias=None, **kwargs):
        """ Creates a |COALESCE| SQL expression.

            The |COALESCE| function returns the first of its arguments
            that is not null.

            -> SQL :class:Function object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                model.select(Functions.coalesce(1, 2, 3, 4))
            ..
            |COALESCE(1, 2, 3, 4)|
        """
        return Function("COALESCE", *vals, alias=alias, **kwargs)

    @staticmethod
    def greatest(*series, alias=None, **kwargs):
        """ Creates a |GREATEST| SQL expression

            -> SQL :class:Function object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                model.select(Functions.greatest(1, 2, 3, 4))
            ..
            |GREATEST(1, 2, 3, 4)|
        """
        return Function("GREATEST", *series, alias=alias, **kwargs)

    @staticmethod
    def least(*series, alias=None, **kwargs):
        """ Creates a |LEAST| SQL expression

            -> SQL :class:Function object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                model.select(Functions.least(1, 2, 3, 4))
            ..
            |LEAST(1, 2, 3, 4)|
        """
        return Function("LEAST", *series, alias=alias, **kwargs)

    @staticmethod
    def generate_series(start, stop, alias=None, **kwargs):
        """ Creates a |generate_series| SQL expression

            -> SQL :class:Function object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                model.select(Functions.generate_series(1, 5))
            ..
            |generate_series(1, 5)|
        """
        return Function("generate_series", start, stop, alias=alias, **kwargs)

    @staticmethod
    def generate_dates(*series, alias=None, **kwargs):
        """ Creates a |generate_dates| SQL expression

            -> SQL :class:Function object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                model.select(Functions.generate_dates(
                    '2009-01-01', '2009-12-31', 1))
            ..
            |generate_dates('2009-01-01', '2009-12-31', 1) |
        """
        return Function("GENERATE_DATES", *series, alias=alias, **kwargs)

    @staticmethod
    def max(val, alias=None, **kwargs):
        """ Creates a |MAX| SQL expression

            -> SQL :class:Function object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                model.select(model.field.max('max_posts'))
                # or
                model.select(Functions.max(model.field, 'max_posts'))
            ..
            |MAX(field) max_posts|
        """
        return Function("MAX", val, alias=alias, **kwargs)

    @staticmethod
    def min(val, alias=None, **kwargs):
        """ Creates a |MIN| SQL expression

            -> SQL :class:Function object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                model.select(model.field.min('min_posts'))
                # or
                model.select(Functions.min(model.field, 'min_posts'))
            ..
            |MIN(field) min_posts|
        """
        return Function("MIN", val, alias=alias, **kwargs)

    @staticmethod
    def avg(val, alias=None, **kwargs):
        """ Creates a |AVG| SQL expression

            -> SQL :class:Function object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                model.select(model.field.avg('avg_posts'))
                # or
                model.select(Functions.avg(model.field, 'avg_posts'))
            ..
            |AVG(field) avg_posts|
        """
        return Function("AVG", val, alias=alias, **kwargs)

    @staticmethod
    def sum(val, alias=None, **kwargs):
        """ Creates a |SUM| SQL expression

            -> SQL :class:Function object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                model.select(model.field.sum('sum_posts'))
                # or
                model.select(Functions.sum(model.field, 'sum_posts'))
            ..
            |SUM(field) sum_posts|
        """
        return Function("SUM", val, alias=alias, **kwargs)

    @staticmethod
    def abs(val, alias=None, **kwargs):
        """ Creates a |ABS| SQL expression

            -> SQL :class:Function object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                model.where(model.field.abs() == 1)
                # or
                model.where(Functions.abs(model.field)) == 1)
            ..
            |WHERE ABS(field) = 1|
        """
        return Function("ABS", val, alias=alias, **kwargs)

    @staticmethod
    def pi():
        """ Creates a |pi()| SQL expression
            |pi()|
        """
        return Function("pi")

    @staticmethod
    def atan(val, alias=None, **kwargs):
        """ Creates a |atan| SQL expression
            |atan(val)|
        """
        return Function("atan", val, alias=alias, **kwargs)

    @staticmethod
    def atan2(x, y, alias=None, **kwargs):
        """ Creates a |atan2| SQL expression
            |atan2(val)|
        """
        return Function("atan2", x, y, alias=alias, **kwargs)

    @staticmethod
    def acos(val, alias=None, **kwargs):
        """ Creates a |acos| SQL expression
            |acos(val)|
        """
        return Function("acos", val, alias=alias, **kwargs)

    @staticmethod
    def asin(val, alias=None, **kwargs):
        """ Creates a |asin| SQL expression
            |asin(val)|
        """
        return Function("asin", val, alias=alias, **kwargs)

    @staticmethod
    def cos(val, alias=None, **kwargs):
        """ Creates a |cos| SQL expression
            |cos(val)|
        """
        return Function("cos", val, alias=alias, **kwargs)

    @staticmethod
    def cot(val, alias=None, **kwargs):
        """ Creates a |cot| SQL expression
            |cot(val)|
        """
        return Function("cot", val, alias=alias, **kwargs)

    @staticmethod
    def degrees(val, alias=None, **kwargs):
        """ Creates a |degrees| SQL expression
            |degrees(val)|
        """
        return Function("degrees", val, alias=alias, **kwargs)

    @staticmethod
    def exp(val, alias=None, **kwargs):
        """ Creates a |exp| SQL expression
            |exp(val)|
        """
        return Function("exp", val, alias=alias, **kwargs)

    @staticmethod
    def floor(val, alias=None, **kwargs):
        """ Creates a |floor| SQL expression
            |floor(val)|
        """
        return Function("floor", val, alias=alias, **kwargs)

    @staticmethod
    def ceil(val, alias=None, **kwargs):
        """ Creates a |ceil| SQL expression
            |ceil(val)|
        """
        return Function("ceil", val, alias=alias, **kwargs)

    @staticmethod
    def ln(val, alias=None, **kwargs):
        """ Creates a |ln| SQL expression
            |ln(val)|
        """
        return Function("ln", val, alias=alias, **kwargs)

    @staticmethod
    def log(val, *base, alias=None, **kwargs):
        """ Creates a |log| SQL expression
            |log(val, 2.0)|
        """
        return Function("log", val, *base, alias=alias, **kwargs)

    @staticmethod
    def mod(val, dmod=0, alias=None, **kwargs):
        """ Creates a |mod| SQL expression
            |mod(val)|
        """
        return Function("mod", val, dmod, alias=alias, **kwargs)

    @staticmethod
    def pow(val, power=2, alias=None, **kwargs):
        """ Creates a |pow| SQL expression
            |pow(val, 2)|
        """
        return Function("power", val, power, alias=alias, **kwargs)

    @staticmethod
    def radians(val, alias=None, **kwargs):
        """ Creates a |radians| SQL expression
            |radians(360)|
        """
        return Function("radians", val, alias=alias, **kwargs)

    @staticmethod
    def round(val, alias=None, **kwargs):
        """ Creates a |round| SQL expression
            |round(val)|
        """
        return Function("round", val, alias=alias, **kwargs)

    @staticmethod
    def sign(val, alias=None, **kwargs):
        """ Creates a |sign| SQL expression
            |sign(val)|
        """
        return Function("sign", val, alias=alias, **kwargs)

    @staticmethod
    def sin(val, alias=None, **kwargs):
        """ Creates a |sin| SQL expression
            |sin(val)|
        """
        return Function("sin", val, alias=alias, **kwargs)

    @staticmethod
    def sqrt(val, alias=None, **kwargs):
        """ Creates a |sqrt| SQL expression
            |sqrt(val)|
        """
        return Function("sqrt", val, alias=alias, **kwargs)

    @staticmethod
    def tan(val, alias=None, **kwargs):
        """ Creates a |tan| SQL expression
            |tan(val)|
        """
        return Function("tan", val, alias=alias, **kwargs)

    @staticmethod
    def nullif(*args, **kwargs):
        """ Creates a |NULLIF| SQL expression

            -> SQL :class:Function object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                condition = Functions.nullif('some value')
            ..
        """
        return Function(logic.NULLIF, *args, **kwargs)

    @staticmethod
    def count(val, alias=None, **kwargs):
        """ Creates a |COUNT| SQL expression

            -> SQL :class:Function object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                condition = model.field.count()
                model.where(condition > 1)
            ..
            |COUNT(field)|
        """
        return Function("COUNT", val, alias=alias, **kwargs)

    @staticmethod
    def distinct(val, alias=None, **kwargs):
        """ Creates a |DISTINCT| SQL expression

            -> SQL :class:Function object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                condition = model.field.distinct().count()
                # or
                condition = Functions.distinct(model.field).count()
                model.where(condition > 1)
            ..
            |COUNT(DISTINCT(field))|
        """
        return Function("DISTINCT", val, alias=alias, **kwargs)

    @staticmethod
    def distinct_on(val, alias=None, **kwargs):
        """ Creates a |DISTINCT ON| SQL expression

            -> SQL :class:Function object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                condition = model.field.distinct_on().count()
                # or
                condition = Functions.distinct_on(model.field).count()
                model.where(condition > 1)
            ..
            |COUNT(DISTINCT ON(field))|
        """
        return Function("DISTINCT ON", val, alias=alias, **kwargs)

    @staticmethod
    def age(*args, alias=None, **kwargs):
        """ Creates a |age| SQL expression

            -> SQL :class:Function object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                condition = model.field.age()
                # or
                condition = Functions.age(model.field)
                model.where(condition > 86400)
            ..
            |age(field)|
        """
        return Function("age", *args, alias=alias, **kwargs)

    @staticmethod
    def timeofday(**kwargs):
        """ Creates a |timeofday| SQL expression
            |timeofday()|
        """
        return Function("timeofday", **kwargs)

    @staticmethod
    def transaction_timestamp(**kwargs):
        """ Creates a |transaction_timestamp| SQL expression
            |transaction_timestamp()|
        """
        return Function("transaction_timestamp", **kwargs)

    @staticmethod
    def now(**kwargs):
        """ Creates a |now| SQL expression
            |now()|
        """
        return Function("now", **kwargs)

    @staticmethod
    def position():
        pass

    @staticmethod
    def any(val, alias=None, **kwargs):
        """ Creates an |ANY| SQL expression

            -> SQL :class:Function object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                model.field.any()
                # or
                Functions.any(model.field)
            ..
            |ANY(field)|
        """
        return Function("ANY", val, alias=alias, **kwargs)

    @staticmethod
    def all(val, alias=None, **kwargs):
        """ Creates an |ALL| SQL expression

            -> SQL :class:Function object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                model.field.all()
                # or
                Functions.all(model.field)
            ..
            |ALL(field)|
        """
        return Function("ALL", val, alias=alias, **kwargs)

    @staticmethod
    def some(val, alias=None, **kwargs):
        """ Creates a |SOME| SQL expression

            -> SQL :class:Function object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                model.field.some()
                # or
                Functions.some(model.field)
            ..
            |SOME(field)|
        """
        return Function("SOME", val, alias=alias, **kwargs)

    @staticmethod
    def using(a, b, **kwargs):
        """ Creates a |USING| SQL expression

            -> SQL :class:Function object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                model.field.using(field_b)
            ..
            |USING(field)|
        """
        return Function(logic.USING, a, b, **kwargs)

    @staticmethod
    def substring(a, b, **kwargs):
        """ Creates a |substring| SQL expression

            -> SQL :class:Function object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            ``Usage Example``
            ..
                condition = model.field.substring('o.b')
                model.where(condition)
            ..
            |substring(field FROM 'o.b')|
        """
        return Function(
            'substring', a, Expression(_empty, logic.FROM, b), **kwargs)

    @staticmethod
    def regexp_replace(string, *args, **kwargs):
        """ Creates a |regexp_replace| expression
            |regexp_replace('Thomas', '.[mN]a.', 'M')|
        """
        return Function('regexp_replace', string, *args, **kwargs)

    @staticmethod
    def regexp_matches(string, pattern, *flags, **kwargs):
        """ Creates a |regexp_matches| expression
            |regexp_matches('foobarbequebaz', '(bar)(beque)')|
        """
        return Function('regexp_matches', string, pattern, *flags, **kwargs)

    @staticmethod
    def concat(string, *others, **kwargs):
        """ Creates a |concat| expression
            |concat('abcde', 2, 22)|
        """
        return Function('concat', string, *others, **kwargs)

    @staticmethod
    def concat_ws(string, *others, separator=',', **kwargs):
        """ Creates a |concat_ws| (concat with separator) expression
            |concat_ws('abcde', 2, 22, separator=" ")|
        """
        return Function('concat_ws', separator, string, *others, **kwargs)

    @staticmethod
    def clock_timestamp(alias=None):
        return Function('clock_timestamp', alias=alias)

    @staticmethod
    def date_part(text, timestamp, **kwargs):
        """ date_part('hour', timestamp '2001-02-16 20:38:40') """
        return Function('date_part', text, timestamp, **kwargs)

    @staticmethod
    def date_trunc(text, timestamp, **kwargs):
        """ date_trunc('hour', timestamp '2001-02-16 20:38:40') """
        return Function('date_trunc', text, timestamp, **kwargs)

    @staticmethod
    def extract(text, timestamp, **kwargs):
        """ extract(hour from timestamp '2001-02-16 20:38:40') """
        return Function(
            'extract',
            Expression(
                text, Expression(_empty, logic.FROM, timestamp), **kwargs))

    @staticmethod
    def isfinite(timestamp, **kwargs):
        """ isfinite(timestamp '2001-02-16 21:28:30') """
        return Function('isfinite', timestamp, **kwargs)

    @staticmethod
    def exists(val, **kwargs):
        return Function('EXISTS', val, **kwargs)

    @staticmethod
    def not_exists(val, **kwargs):
        return Function('NOT EXISTS', val, **kwargs)

    @staticmethod
    def cast(field, as_):
        """ cast(mytable.myfield AS integer) """
        return Function('cast', Expression(field, 'AS', safe(as_)))

    @staticmethod
    def func(*args, **kwargs):
        """ Use a custom function or one that is not listed

            ..
                f = Function.func('now')
            ..
            |now()|

            ..
                f = Function.func('generate_series', 1, 2, 3, 4)
            ..
            |generate_series(1, 2, 3, 4)|
        """
        return Function(*args, **kwargs)


class aliased(BaseLogic, NumericLogic, DateTimeLogic, StringLogic):
    """ For field aliases.

        This object can be manipulated with expression :class:BaseLogic
    """
    __slots__ = ('value',)

    def __init__(self, name_or_field):
        if hasattr(name_or_field, '_alias'):
            if name_or_field._alias:
                self.value = name_or_field._alias
            else:
                self.value = name_or_field.name
        else:
            self.value = name_or_field

    @prepr('value', _break=False)
    def __repr__(self): return

    def __str__(self):
        return str(self.value)

    def __call__(self):
        return self.value

    def alias(self, alias):
        """ ..
            field.table = 'foo'
            print(field)
            # foo.field_name
            field.set_alias('foo_alias')
            print(aliased(field))
            # foo_alias.field_name
            print(aliased(field).alias('foo_alias_field_name'))
            # foo_alias.field_name AS foo_alias_field_name
            ..
        """
        return aliased(str(self) + ' AS ' + alias)


class safe(BaseLogic, NumericLogic, DateTimeLogic, StringLogic):
    """ !! The value of this object will not be parameterized when
           queries are made. !!

        This object cannot be manipulated with expression :class:BaseLogic
    """
    __slots__ = ('value', 'alias')

    def __init__(self, value, alias=None):
        self.value = value
        self.alias = alias or ""

    @prepr('value')
    def __repr__(self): return

    def __str__(self):
        return "{} {}".format(self.value, self.alias).strip()


class ArrayItems(object):
    """ Thin wrapper for lists in :class:Array """
    __slots__ = ('value',)

    def __init__(self, items):
        self.value = list(items)

    @prepr('value', _break=False)
    def __repr__(self): return

    def __str__(self):
        return str(self.value)

    def __call__(self):
        return self.value
