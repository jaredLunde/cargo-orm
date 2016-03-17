#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
    `Unit tests for cargo.expressions.DateTimeLogic`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
from cargo.expressions import *
from cargo.fields import Time, Field

from unit_tests import configure


class TestDateTimeLogic(configure.LogicTestCase):

    def setUp(self):
        self.base = Time('October 13, 2008 at 11:12:13pm UTC')

    def test_interval(self):
        for val in ('1 day', '2 hours'):
            expr = self.base.interval(val)
            if hasattr(val, 'params'):
                values = list(val.params.values())
            elif isinstance(val, Field):
                values = []
            else:
                values = [val]
            self.validate_expression(
                expr, _empty, 'interval', val, values=values)

    def test_last(self):
        for val in ('1 day', '2 hours'):
            expr = self.base.last(val)
            self.assertIsInstance(expr, Expression)
            self.assertIs(expr.left, self.base)
            self.assertEqual(expr.operator, '>=')
            self.assertIsInstance(expr.right, Expression)
            self.assertIsInstance(expr.right.left, Function)
            self.assertEqual(expr.right.operator, '-')
            self.assertEqual(expr.right.right, val)
            self.assertEqual(expr.right.group_op, " ")

    def test_clock_timestamp(self):
        self.validate_function(
            self.base.clock_timestamp(),
            'clock_timestamp',
            [])
        self.validate_function(
            self.base.clock_timestamp(alias='foo'),
            'clock_timestamp',
            [],
            alias='foo')

    def test_current_date(self):
        expr = self.base.current_date()
        self.validate_expression(
            expr, _empty, 'current_date', _empty, values=[])

    def test_current_time(self):
        expr = self.base.current_time()
        self.validate_expression(
            expr, _empty, 'current_time', _empty, values=[])

    def test_current_timestamp(self):
        expr = self.base.current_timestamp()
        self.validate_expression(
            expr, _empty, 'current_timestamp', _empty, values=[])

    def test_localtime(self):
        expr = self.base.localtime()
        self.validate_expression(
            expr, _empty, 'localtime', _empty, values=[])

    def test_localtimestamp(self):
        expr = self.base.localtimestamp()
        self.validate_expression(
            expr, _empty, 'localtimestamp', _empty, values=[])

    def test_transaction_timestamp(self):
        self.validate_function(
            self.base.transaction_timestamp(),
            'transaction_timestamp',
            [])
        self.validate_function(
            self.base.transaction_timestamp(alias='foo'),
            'transaction_timestamp',
            [],
            alias='foo')

    def test_now(self):
        self.validate_function(
            self.base.now(),
            'now',
            [])
        self.validate_function(
            self.base.now(alias='foo'),
            'now',
            [],
            alias='foo')

    def test_age(self):
        """ :see::meth:Functions.age """
        self.validate_function(
            self.base.age(),
            'age',
            [self.base])
        self.validate_function(
            self.base.age(alias='foo'),
            'age',
            [self.base],
            alias='foo')
        self.validate_function(
            self.base.age(self.base.now(),),
            'age',
            [self.base, self.base.now()])
        self.validate_function(
            self.base.age(self.base.now(), alias='foo'),
            'age',
            [self.base, self.base.now()],
            alias='foo')

    def test_date_part(self):
        """ :see::meth:Functions.date_part """
        for val in ('hour', 'minute', 'second'):
            self.validate_function(
                self.base.date_part(val),
                'date_part',
                [self.base, val])
            self.validate_function(
                self.base.date_part(val, alias='foo'),
                'date_part',
                [self.base, val],
                alias='foo')

    def test_date_trunc(self):
        """ :see::meth:Functions.date_trunc """
        for val in ('hour', 'minute', 'second'):
            self.validate_function(
                self.base.date_trunc(val),
                'date_trunc',
                [self.base, val])
            self.validate_function(
                self.base.date_trunc(val, alias='foo'),
                'date_trunc',
                [self.base, val],
                alias='foo')

    def test_extract(self):
        """ :see::meth:Functions.extract """
        for val in ('hour', 'minute', 'second'):
            function = self.base.extract(val)
            self.assertIsInstance(function, Function)
            self.assertEqual(function.func, 'extract')
            self.assertEqual(function.args[0], Expression)
            self.assertEqual(function.args[0].left, val)
            self.assertIsInstance(function.args[0].operator, Expression)
            self.assertEqual(function.args[0].operator.operator, 'FROM')

    def test_isfinite(self):
        """ isfinite(timestamp '2001-02-16 21:28:30') """
        self.validate_function(
            self.base.isfinite(),
            'isfinite',
            [self.base])
        self.validate_function(
            self.base.isfinite(alias='foo'),
            'isfinite',
            [self.base],
            alias='foo')

    def test_timeofday(self):
        self.validate_function(
            self.base.timeofday(),
            'timeofday',
            [])
        self.validate_function(
            self.base.timeofday(alias='foo'),
            'timeofday',
            [],
            alias='foo')


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestDateTimeLogic, failfast=True, verbosity=2)
