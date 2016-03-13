#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
  `Unit tests for bloom.expressions.StringLogic`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
import unittest
from kola import config

from bloom.expressions import *
from vital.security import randkey
from bloom import ORM, Model
from bloom.fields import Varchar, Text, Field


def new_field():
    field = Text()
    keyspace = 'aeioubcdhzpwnmprstln'
    name = randkey(24, keyspace)
    table = randkey(24, keyspace)
    field.field_name = name
    field.table = table
    return field


class TestStringLogic(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.base = Varchar()
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

    def test_concat(self):
        for val in (1234, '5678'):
            self.validate_function(
                self.base.concat(val),
                'concat',
                [self.base, val])
            self.validate_function(
                self.base.concat(val, alias='foo'),
                'concat',
                [self.base, val],
                alias='foo')

    def test_startswith(self):
        for val in (160, '161'):
            expr = self.base.startswith(val)
            if hasattr(val, 'params'):
                values = list(val.params.values())
            elif isinstance(val, Field):
                values = []
            else:
                values = [str(val) + '%']
            self.validate_expression(
                expr, self.base, 'ILIKE', str(val) + '%', values=values)

    def test_endswith(self):
        for val in (160, '161'):
            expr = self.base.endswith(val)
            if hasattr(val, 'params'):
                values = list(val.params.values())
            elif isinstance(val, Field):
                values = []
            else:
                values = ['%' + str(val)]
            self.validate_expression(
                expr, self.base, 'ILIKE', '%' + str(val), values=values)

    def test_contains(self):
        for val in (160, '161'):
            expr = self.base.contains(val)
            if hasattr(val, 'params'):
                values = list(val.params.values())
            elif isinstance(val, Field):
                values = []
            else:
                values = ['%' + str(val) + '%']
            self.validate_expression(
                expr, self.base, 'ILIKE', '%' + str(val) + '%', values=values)

    def test_like(self):
        for val in (160, '161'):
            expr = self.base.like(val)
            if hasattr(val, 'params'):
                values = list(val.params.values())
            elif isinstance(val, Field):
                values = []
            else:
                values = [val]
            self.validate_expression(
                expr, self.base, 'LIKE', val, values=values)

    def test_not_like(self):
        for val in (160, '161'):
            expr = self.base.not_like(val)
            if hasattr(val, 'params'):
                values = list(val.params.values())
            elif isinstance(val, Field):
                values = []
            else:
                values = [val]
            self.validate_expression(
                expr, self.base, 'NOT LIKE', val, values=values)

    def test_not_ilike(self):
        for val in (160, '161'):
            expr = self.base.not_ilike(val)
            if hasattr(val, 'params'):
                values = list(val.params.values())
            elif isinstance(val, Field):
                values = []
            else:
                values = [val]
            self.validate_expression(
                expr, self.base, 'NOT ILIKE', val, values=values)

    def test_ilike(self):
        for val in (160, '161'):
            expr = self.base.ilike(val)
            if hasattr(val, 'params'):
                values = list(val.params.values())
            elif isinstance(val, Field):
                values = []
            else:
                values = [val]
            self.validate_expression(
                expr, self.base, 'ILIKE', val, values=values)

    def test_similar_to(self):
        for val in (160, '161'):
            expr = self.base.similar_to(val)
            if hasattr(val, 'params'):
                values = list(val.params.values())
            elif isinstance(val, Field):
                values = []
            else:
                values = [val]
            self.validate_expression(
                expr, self.base, 'SIMILAR TO', val, values=values)

    def test_not_similar_to(self):
        for val in (160, '161'):
            expr = self.base.not_similar_to(val)
            if hasattr(val, 'params'):
                values = list(val.params.values())
            elif isinstance(val, Field):
                values = []
            else:
                values = [val]
            self.validate_expression(
                expr, self.base, 'NOT SIMILAR TO', val, values=values)

    def test_posix(self):
        expr = self.base.posix('(b|d)', op='~')
        values = ['(b|d)']
        self.validate_expression(expr, self.base, '~', '(b|d)', values=values)

    def test_concat_ws(self):
        for val in (1234, '5678', new_field()):
            self.validate_function(
                self.base.concat_ws(val),
                'concat_ws',
                [',', self.base, val])
            self.validate_function(
                self.base.concat_ws(val, separator='|', alias='foo'),
                'concat_ws',
                ['|', self.base, val],
                alias='foo')

    def test_regexp_replace(self):
        for val in ('[\w]+', '.*'):
            self.validate_function(
                self.base.regexp_replace(val, 'bar'),
                'regexp_replace',
                [self.base, val, 'bar'])
            self.validate_function(
                self.base.regexp_replace(val, 'bar', 'M', alias='foo'),
                'regexp_replace',
                [self.base, val, 'bar', 'M'],
                alias='foo')

    def test_regexp_matches(self):
        for val in ('[\w]+', '.*'):
            self.validate_function(
                self.base.regexp_matches(val),
                'regexp_matches',
                [self.base, val])
            self.validate_function(
                self.base.regexp_matches(val, 'M', alias='foo'),
                'regexp_matches',
                [self.base, val, 'M'],
                alias='foo')


if __name__ == '__main__':
    # Unit test
    unittest.main()
