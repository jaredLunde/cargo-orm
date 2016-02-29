#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
  `Unit tests for bloom.expressions.Expression`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
from types import GeneratorType
import unittest
import psycopg2

from kola import config
from vital.security import randkey

from bloom.expressions import *
from bloom import *
from bloom import fields


def new_field(type='char'):
    field = getattr(fields, type.title())()
    keyspace = 'aeioubcdlhzpwnmp'
    name = randkey(24, keyspace)
    table = randkey(24, keyspace)
    field.field_name = name
    field.table = table
    return field


def new_expression(cast=int):
    if cast == bytes:
        cast = lambda x: psycopg2.Binary(str(x).encode())
    return Expression(new_field(), '=', cast(12345))


def new_function(cast=int):
    if cast == bytes:
        cast = lambda x: psycopg2.Binary(str(x).encode())
    return Function('some_func', cast(12345))


class TestExpression(unittest.TestCase):
    base = Expression(new_field(), '=', new_field('int'))

    def test__get_param_key(self):
        l = [1, 2, 3, 4]
        for val in ('test', 1234, l):
            pkey = self.base._get_param_key(val)
            self.assertIn(val, list(self.base.params.values()))
            self.assertIsNotNone(pkey)
            self.assertEqual(self.base.params["".join(pkey[2:-2])], val)

        for val in ('test', 1234, l):
            pkey = self.base._get_param_key(val)
            print(val, list(self.base.params.values()))
            self.assertEqual(1, list(self.base.params.values()).count(val))
            self.assertEqual(self.base.params["".join(pkey[2:-2])], val)

    def test__inherit_parameters(self):
        types = (
            new_function(), new_function(str), new_function(float),
            new_function(bytes), new_expression(), new_expression(str),
            new_expression(float), new_expression(bytes))
        for val in types:
            base = Expression(new_field(), '=', val)
            for k, v in val.params.items():
                self.assertIn(k, base.params)
                self.assertIs(base.params[k], v)
        array_items = [1, 2, 3, 4]
        base = Expression(new_field(), '=', array_items)
        for type in types:
            base._inherit_parameters(type)
        for val in types:
            for k, v in val.params.items():
                self.assertIn(k, base.params)
                self.assertIs(base.params[k], v)
        self.assertIn(array_items, list(base.params.values()))

    def test__compile_expressions(self):
        types = (
            new_function(), new_function(str), new_function(float),
            new_function(bytes), new_expression(), new_expression(str),
            new_expression(float), new_expression(bytes))
        base = Expression(new_field(), '=', [1, 2, 3, 4])
        exps = base._compile_expressions(*types)
        self.assertIsInstance(exps, GeneratorType)

    def test__parameterize(self):
        types = (
            new_function(), new_function(str), new_function(float),
            new_function(bytes), new_expression(), new_expression(str),
            new_expression(float), new_expression(bytes), new_field('int'),
            new_field('char'), '1234', 1234, '1234'.encode(),
            psycopg2.Binary(b'1234'))
        for val in types:
            nval = self.base._parameterize(val)
            self.assertIsInstance(nval, str)
        for val in types:
            if hasattr(val, 'params') or hasattr(val, 'value') or \
               isinstance(val, bytes):
                continue
            found = False
            for param in self.base.params.values():
                if str(param) == str(val):
                    found = True
                    break;
            self.assertTrue(found)

    def _validate(self, expr, left, op, right):
        self.assertIs(expr.left, left)
        self.assertIs(expr.right, right)
        self.assertIs(expr.operator, op)
        for key, val in expr.params.items():
            self.assertIn('%(' + key + ')s', expr.string)
        if not expr.params:
            return
        found = 0
        for param in expr.params.values():
            if str(param) == str(left) or str(param) == str(right):
                found += 1
        for side in (left, right):
            if hasattr(side, 'params'):
                for key, val in side.params.items():
                    if str(val) == str(expr.params[key]):
                        found += 1
        self.assertTrue(found == len(expr.params))

    def test_compile(self):
        fields = (
            (new_field(), '=', new_field()),
            (new_field(), '>', new_field()),
            (new_field(), '<', new_field()),
            (new_field(), '>=', new_field()),
            (new_field(), '<=', new_field()),
            (new_field(), '*', new_field()),
            (new_field(), '=', new_function(int)),
            (new_field(), '>', new_function(str)),
            (new_field(), '<', new_function(bytes)),
            (new_field(), '>=', 1234),
            (new_field(), '<=', '1234'),
            (new_field(), '*', psycopg2.Binary(b'1234')),
            (new_field(), '*', b'1234'),
            (new_function(int), '=', new_field()),
            (new_function(str), '>', new_field()),
            (new_function(bytes), '<', new_field()),
        )

        for _ in fields:
            left, op, right = _
            expr = Expression(left, op, right)
            self._validate(expr, left, op, right)
            if isinstance(right, Field):
                self.assertIn(right.name, expr.string)
            if isinstance(left, Field):
                self.assertIn(left.name, expr.string)

        for _ in fields:
            left, op, right = reversed(_)
            expr = Expression(left, op, right)
            self._validate(expr, left, op, right)
            if isinstance(right, Field):
                self.assertIn(right.name, expr.string)
            if isinstance(left, Field):
                self.assertIn(left.name, expr.string)

        for _ in fields:
            left, op, right = _
            expr = Expression(left, op, right, use_field_name=True)
            self._validate(expr, left, op, right)
            if isinstance(right, Field):
                self.assertIn(right.field_name, expr.string)
                self.assertNotIn(right.name, expr.string)
            if isinstance(left, Field):
                self.assertIn(left.field_name, expr.string)
                self.assertNotIn(left.name, expr.string)

        for _ in fields:
            left, op, right = reversed(_)
            expr = Expression(left, op, right, use_field_name=True)
            self._validate(expr, left, op, right)
            if isinstance(right, Field):
                self.assertIn(right.field_name, expr.string)
                self.assertNotIn(right.name, expr.string)
            if isinstance(left, Field):
                self.assertIn(left.field_name, expr.string)
                self.assertNotIn(left.name, expr.string)

    def test_group_compile(self):
        fields = (
            (new_field(), '=', new_field()),
            (new_field(), '>', new_field()),
            (new_field(), '<', new_field()),
            (new_field(), '>=', new_field()),
            (new_field(), '<=', new_field()),
            (new_field(), '*', new_field()),
            (new_field(), '=', new_function(int)),
            (new_field(), '>', new_function(str)),
            (new_field(), '<', new_function(bytes)),
            (new_field(), '>=', 1234),
            (new_field(), '<=', '1234'),
            (new_field(), '*', psycopg2.Binary(b'1234')),
        )

        for _ in fields:
            left, op, right = _
            expr = Expression(left, op, right, use_field_name=True).group()
            self._validate(expr, left, op, right)
            if isinstance(right, Field):
                self.assertIn(right.field_name, expr.string)
                self.assertNotIn(right.name, expr.string)
            if isinstance(left, Field):
                self.assertIn(left.field_name, expr.string)
                self.assertNotIn(left.name, expr.string)
            self.assertTrue(
                expr.string.endswith(')') and expr.string.startswith('('))

        for _ in fields:
            left, op, right = _
            expr = Expression(left, op, right, use_field_name=True).group_and()
            self._validate(expr, left, op, right)
            if isinstance(right, Field):
                self.assertIn(right.field_name, expr.string)
                self.assertNotIn(right.name, expr.string)
            if isinstance(left, Field):
                self.assertIn(left.field_name, expr.string)
                self.assertNotIn(left.name, expr.string)
            self.assertTrue(
                expr.string.endswith(') AND') and expr.string.startswith('('))

        for _ in fields:
            left, op, right = _
            expr = Expression(left, op, right, use_field_name=True).group_or()
            self._validate(expr, left, op, right)
            if isinstance(right, Field):
                self.assertIn(right.field_name, expr.string)
                self.assertNotIn(right.name, expr.string)
            if isinstance(left, Field):
                self.assertIn(left.field_name, expr.string)
                self.assertNotIn(left.name, expr.string)
            self.assertTrue(
                expr.string.endswith(') OR') and expr.string.startswith('('))


if __name__ == '__main__':
    # Unit test
    unittest.main()
