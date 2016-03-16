#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
  `Unit tests for cargo.expressions.NumericLogic`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
import unittest
from kola import config

from cargo.expressions import *
from vital.security import randkey
from cargo import ORM, Model
from cargo.fields import SmallInt, Int, Field


def new_field():
    field = Int()
    keyspace = 'aeioubcdlhzpwnmp'
    name = randkey(24, keyspace)
    table = randkey(24, keyspace)
    field.field_name = name
    field.table = table
    return field


class TestNumericLogic(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.base = SmallInt()
        self.base.field_name = 'test'
        self.base.table = 'tester'

    def validate_expression(self, expression, left, operator, right,
                            params=None, values=None):
        self.assertIsInstance(expression, Expression)
        self.assertIs(expression.left, left)
        self.assertEqual(expression.operator, operator)
        self.assertEqual(expression.right, right)
        if params is not None:
            self.assertDictEqual(expression.params, params)
        elif values:
            for value in values:
                self.assertIn(value, list(expression.params.values()))

    def validate_function(self, function, func, args, alias=None, values=None):
        self.assertIsInstance(function, Function)
        self.assertEqual(function.func, func)
        self.assertTupleEqual(function.args, tuple(args))
        self.assertEqual(function.alias, alias)
        if values:
            for value in values:
                self.assertIn(value, list(function.params.values()))

    def test___lt__(self):
        for val in (160, new_field(), ORM().subquery().where(1).select()):
            expr = (self.base.lt(val))
            if hasattr(val, 'params'):
                values = list(val.params.values())
            elif isinstance(val, Field):
                values = []
            else:
                values = [val]
            self.validate_expression(expr, self.base, '<', val, values=values)

    def test___le__(self):
        for val in (160, new_field(), ORM().subquery().where(1).select()):
            expr = (self.base.le(val))
            if hasattr(val, 'params'):
                values = list(val.params.values())
            elif isinstance(val, Field):
                values = []
            else:
                values = [val]
            self.validate_expression(expr, self.base, '<=', val, values=values)

    def test___gt__(self):
        for val in (160, new_field(), ORM().subquery().where(1).select()):
            expr = (self.base.gt(val))
            if hasattr(val, 'params'):
                values = list(val.params.values())
            elif isinstance(val, Field):
                values = []
            else:
                values = [val]
            self.validate_expression(expr, self.base, '>', val, values=values)

    def test___ge__(self):
        for val in (160, new_field(), ORM().subquery().where(1).select()):
            expr = (self.base.ge(val))
            if hasattr(val, 'params'):
                values = list(val.params.values())
            elif isinstance(val, Field):
                values = []
            else:
                values = [val]
            self.validate_expression(expr, self.base, '>=', val, values=values)

    def test___div__(self):
        for val in (160, new_field(), ORM().subquery().where(1).select()):
            expr = (self.base / val)
            if hasattr(val, 'params'):
                values = list(val.params.values())
            elif isinstance(val, Field):
                values = []
            else:
                values = [val]
            self.validate_expression(expr, self.base, '/', val, values=values)

    def test___mul__(self):
        for val in (160, new_field(), ORM().subquery().where(1).select()):
            expr = (self.base * val)
            if hasattr(val, 'params'):
                values = list(val.params.values())
            elif isinstance(val, Field):
                values = []
            else:
                values = [val]
            self.validate_expression(expr, self.base, '*', val, values=values)

    def test___add__(self):
        for val in (160, new_field(), ORM().subquery().where(1).select()):
            expr = (self.base + val)
            if hasattr(val, 'params'):
                values = list(val.params.values())
            elif isinstance(val, Field):
                values = []
            else:
                values = [val]
            self.validate_expression(expr, self.base, '+', val, values=values)

    def test___pow__(self):
        for val in (160, new_field(), ORM().subquery().where(1).select()):
            expr = (self.base ** val)
            if hasattr(val, 'params'):
                values = list(val.params.values())
            elif isinstance(val, Field):
                values = []
            else:
                values = [val]
            self.validate_expression(expr, self.base, '^', val, values=values)

    def test___sub__(self):
        for val in (160, new_field(), ORM().subquery().where(1).select()):
            expr = (self.base - val)
            if hasattr(val, 'params'):
                values = list(val.params.values())
            elif isinstance(val, Field):
                values = []
            else:
                values = [val]
            self.validate_expression(expr, self.base, '-', val, values=values)

    def test_abs(self):
        self.validate_function(
            self.base.abs(),
            'abs',
            [self.base])
        self.validate_function(
            self.base.abs(alias='foo'),
            'abs',
            [self.base],
            alias='foo')

    def test_atan(self):
        self.validate_function(
            self.base.atan(),
            'atan',
            [self.base])
        self.validate_function(
            self.base.atan(alias='foo'),
            'atan',
            [self.base],
            alias='foo')

    def test_atan2(self):
        self.validate_function(
            self.base.atan2(60),
            'atan2',
            [self.base, 60])
        self.validate_function(
            self.base.atan2(60, alias='foo'),
            'atan2',
            [self.base, 60],
            alias='foo')

    def test_sqrt(self):
        self.validate_function(
            self.base.sqrt(),
            'sqrt',
            [self.base])
        self.validate_function(
            self.base.sqrt(alias='foo'),
            'sqrt',
            [self.base],
            alias='foo')

    def test_min(self):
        self.validate_function(
            self.base.min(),
            'min',
            [self.base])
        self.validate_function(
            self.base.min(alias='foo'),
            'min',
            [self.base],
            alias='foo')

    def test_avg(self):
        self.validate_function(
            self.base.avg(),
            'avg',
            [self.base])
        self.validate_function(
            self.base.avg(alias='foo'),
            'avg',
            [self.base],
            alias='foo')

    def test_floor(self):
        self.validate_function(
            self.base.floor(),
            'floor',
            [self.base])
        self.validate_function(
            self.base.floor(alias='foo'),
            'floor',
            [self.base],
            alias='foo')

    def test_ceil(self):
        self.validate_function(
            self.base.ceil(),
            'ceil',
            [self.base])
        self.validate_function(
            self.base.ceil(alias='foo'),
            'ceil',
            [self.base],
            alias='foo')

    def test_cos(self):
        self.validate_function(
            self.base.cos(),
            'cos',
            [self.base])
        self.validate_function(
            self.base.cos(alias='foo'),
            'cos',
            [self.base],
            alias='foo')

    def test_sign(self):
        self.validate_function(
            self.base.sign(),
            'sign',
            [self.base])
        self.validate_function(
            self.base.sign(alias='foo'),
            'sign',
            [self.base],
            alias='foo')

    def test_asin(self):
        self.validate_function(
            self.base.asin(),
            'asin',
            [self.base])
        self.validate_function(
            self.base.asin(alias='foo'),
            'asin',
            [self.base],
            alias='foo')

    def test_between(self):
        for val in (160, new_field(), ORM().subquery().where(1).select()):
            expr = (self.base.between(val, 100))
            if hasattr(val, 'params'):
                values = list(val.params.values())
            elif isinstance(val, Field):
                values = []
            else:
                values = [val]
            values.append(100)
            self.assertIs(expr.left, self.base)
            self.assertEqual(expr.operator, 'BETWEEN')
            self.assertIsInstance(expr.right, Expression)
            self.assertEqual(expr.right.left, val)
            self.assertEqual(expr.right.operator, 'AND')
            self.assertEqual(expr.right.right, 100)
            for val in values:
                self.assertIn(val, list(expr.params.values()))

    def test_not_between(self):
        for val in (160, new_field(), ORM().subquery().where(1).select()):
            expr = (self.base.not_between(val, 100))
            if hasattr(val, 'params'):
                values = list(val.params.values())
            elif isinstance(val, Field):
                values = []
            else:
                values = [val]
            values.append(100)
            self.assertIs(expr.left, self.base)
            self.assertEqual(expr.operator, 'NOT BETWEEN')
            self.assertIsInstance(expr.right, Expression)
            self.assertEqual(expr.right.left, val)
            self.assertEqual(expr.right.operator, 'AND')
            self.assertEqual(expr.right.right, 100)
            for val in values:
                self.assertIn(val, list(expr.params.values()))

    def test_tan(self):
        self.validate_function(
            self.base.tan(),
            'tan',
            [self.base])
        self.validate_function(
            self.base.tan(alias='foo'),
            'tan',
            [self.base],
            alias='foo')

    def test_round(self):
        self.validate_function(
            self.base.asin(),
            'asin',
            [self.base])
        self.validate_function(
            self.base.asin(alias='foo'),
            'asin',
            [self.base],
            alias='foo')

    def test_acos(self):
        self.validate_function(
            self.base.acos(),
            'acos',
            [self.base])
        self.validate_function(
            self.base.acos(alias='foo'),
            'acos',
            [self.base],
            alias='foo')

    def test_sum(self):
        self.validate_function(
            self.base.sum(),
            'sum',
            [self.base])
        self.validate_function(
            self.base.sum(alias='foo'),
            'sum',
            [self.base],
            alias='foo')

    def test_degrees(self):
        self.validate_function(
            self.base.degrees(),
            'degrees',
            [self.base])
        self.validate_function(
            self.base.degrees(alias='foo'),
            'degrees',
            [self.base],
            alias='foo')

    def test_log(self):
        self.validate_function(
            self.base.log(),
            'log',
            [self.base])
        self.validate_function(
            self.base.log(alias='foo'),
            'log',
            [self.base],
            alias='foo')
        self.validate_function(
            self.base.log(2),
            'log',
            [self.base, 2])
        self.validate_function(
            self.base.log(2, alias='foo'),
            'log',
            [self.base, 2],
            alias='foo')

    def test_exp(self):
        self.validate_function(
            self.base.exp(),
            'exp',
            [self.base])
        self.validate_function(
            self.base.exp(alias='foo'),
            'exp',
            [self.base],
            alias='foo')

    def test_radians(self):
        self.validate_function(
            self.base.radians(),
            'radians',
            [self.base])
        self.validate_function(
            self.base.radians(alias='foo'),
            'radians',
            [self.base],
            alias='foo')

    def test_cot(self):
        self.validate_function(
            self.base.cot(),
            'cot',
            [self.base])
        self.validate_function(
            self.base.cot(alias='foo'),
            'cot',
            [self.base],
            alias='foo')

    def test_mod(self):
        self.validate_function(
            self.base.mod(4),
            'mod',
            [self.base, 4])
        self.validate_function(
            self.base.mod(4, alias='foo'),
            'mod',
            [self.base, 4],
            alias='foo')

    def test_pow(self):
        self.validate_function(
            self.base.pow(4),
            'power',
            [self.base, 4])
        self.validate_function(
            self.base.pow(4, alias='foo'),
            'power',
            [self.base, 4],
            alias='foo')

    def test_sin(self):
        self.validate_function(
            self.base.sin(),
            'sin',
            [self.base])
        self.validate_function(
            self.base.sin(alias='foo'),
            'sin',
            [self.base],
            alias='foo')

    def test_max(self):
        self.validate_function(
            self.base.max(),
            'max',
            [self.base])
        self.validate_function(
            self.base.max(alias='foo'),
            'max',
            [self.base],
            alias='foo')


if __name__ == '__main__':
    # Unit test
    unittest.main()
