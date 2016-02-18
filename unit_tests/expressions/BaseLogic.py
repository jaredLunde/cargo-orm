#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
  `Unit tests for vital.sql.expressions.BaseLogic`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
import unittest
from vital import config

from vital.sql.expressions import *
from vital.security import randkey
from vital.sql import ORM, fields
from vital.sql.fields import Field


def new_field(type='char', table=None, name=None):
    field = getattr(fields, type.title())()
    keyspace = 'aeioubcdlhzpwnmp'
    name = name or randkey(24, keyspace)
    table = table or randkey(24, keyspace)
    field.field_name = name
    field.table = table
    return field


class TestBaseLogic(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.base = Field()
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

    def test_asc(self):
        expr = self.base.asc()
        self.validate_expression(expr, self.base, 'ASC', _empty, {})

    def test_desc(self):
        expr = self.base.desc()
        self.validate_expression(expr, self.base, 'DESC', _empty, {})

    def test___and__(self):
        for x in ['string', 1234]:
            expr = (self.base & x)
            self.validate_expression(expr, self.base, 'AND', x, values=[x])
        for x in [Function('now'), self.base.asc(), self.base.in_(1, 2, 3)]:
            expr = (self.base & x)
            self.validate_expression(expr, self.base, 'AND', x, x.params)

    def test_in_(self):
        for x in [['sting', 'string2', 'string3'], [1, 2, 3, 4, 5, 6]]:
            expr = (self.base.in_(*x))
            self.validate_expression(
                expr, self.base, 'IN', tuple(x), values=[tuple(x)])

    def test_not_in(self):
        for x in [['sting', 'string2', 'string3'], [1, 2, 3, 4, 5, 6]]:
            expr = (self.base.not_in(*x))
            self.validate_expression(
                expr, self.base, 'NOT IN', tuple(x), values=[tuple(x)])

    def test_is_null(self):
        self.validate_expression(
            self.base.is_null(),
            self.base,
            'IS NULL',
            _empty,
            {})

    def test_not_null(self):
        self.validate_expression(
            self.base.not_null(),
            self.base,
            'IS NOT NULL',
            _empty,
            {})

    def test_nullif(self):
        for val in ['some_value', 12345, new_field()]:
            self.validate_function(
                self.base.nullif(val),
                'NULLIF',
                [self.base, val])
        for val in ['some_value', 12345, new_field()]:
            func = self.base.nullif(val, alias='foo')
            self.validate_function(
                func,
                'NULLIF',
                [self.base, val],
                alias='foo')

    def test_distinct_from(self):
        other_field = new_field()
        self.validate_expression(
            self.base.distinct_from(other_field),
            self.base,
            'IS DISTINCT FROM',
            other_field,
            {})

    def test_not_distinct_from(self):
        other_field = new_field()
        self.validate_expression(
            self.base.not_distinct_from(other_field),
            self.base,
            'IS NOT DISTINCT FROM',
            other_field,
            {})

    def test_distinct_on(self):
        self.validate_function(
            self.base.distinct_on(),
            'DISTINCT ON',
            [self.base]
        )
        self.validate_function(
            self.base.distinct_on(alias='test'),
            'DISTINCT ON',
            [self.base],
            alias='test'
        )

    def test_distinct(self):
        self.validate_function(
            self.base.distinct(),
            'DISTINCT',
            [self.base]
        )
        self.validate_function(
            self.base.distinct(alias='test'),
            'DISTINCT',
            [self.base],
            alias='test'
        )

    def test_count(self):
        field = new_field()
        self.validate_function(
            self.base.count(),
            'COUNT',
            [self.base]
        )
        self.validate_function(
            self.base.count(alias='test'),
            'COUNT',
            [self.base],
            alias='test'
        )

    def test_using(self):
        field = new_field()
        self.validate_function(
            self.base.using(field),
            'USING',
            [self.base, field]
        )
        self.validate_function(
            self.base.using(field, alias='test'),
            'USING',
            [self.base, field],
            alias='test'
        )

    def test_lag(self):
        field = new_field(table='foo', name='bar')
        func = field.lag()
        string = 'lag(foo.bar) OVER ()'
        self.assertEqual(func.string % func.params, string)

        func = field.lag('w')
        string = 'lag(foo.bar) OVER w'
        self.assertEqual(func.string % func.params, string)

        func = field.lag(order_by=field.desc())
        string = 'lag(foo.bar) OVER (ORDER BY foo.bar DESC)'
        self.assertEqual(func.string % func.params, string)

        func = field.lag(partition_by=field)
        string = 'lag(foo.bar) OVER (PARTITION BY foo.bar)'
        self.assertEqual(func.string % func.params, string)

        func = field.lag(Clause('ORDER BY', field))
        string = 'lag(foo.bar) OVER (ORDER BY foo.bar)'
        self.assertEqual(func.string % func.params, string)

        func = field.lag(Clause('ORDER BY', field), alias='foo_alias')
        string = 'lag(foo.bar) OVER (ORDER BY foo.bar) foo_alias'
        self.assertEqual(func.string % func.params, string)

    def test_lead(self):
        field = new_field(table='foo', name='bar')
        func = field.lead()
        string = 'lead(foo.bar) OVER ()'
        self.assertEqual(func.string % func.params, string)

        func = field.lead('w')
        string = 'lead(foo.bar) OVER w'
        self.assertEqual(func.string % func.params, string)

        func = field.lead(order_by=field.desc())
        string = 'lead(foo.bar) OVER (ORDER BY foo.bar DESC)'
        self.assertEqual(func.string % func.params, string)

        func = field.lead(partition_by=field)
        string = 'lead(foo.bar) OVER (PARTITION BY foo.bar)'
        self.assertEqual(func.string % func.params, string)

        func = field.lead(Clause('ORDER BY', field))
        string = 'lead(foo.bar) OVER (ORDER BY foo.bar)'
        self.assertEqual(func.string % func.params, string)

        func = field.lead(Clause('ORDER BY', field), alias='foo_alias')
        string = 'lead(foo.bar) OVER (ORDER BY foo.bar) foo_alias'
        self.assertEqual(func.string % func.params, string)

    def test_first_value(self):
        field = new_field(table='foo', name='bar')
        func = field.first_value()
        string = 'first_value(foo.bar) OVER ()'
        self.assertEqual(func.string % func.params, string)

        func = field.first_value('w')
        string = 'first_value(foo.bar) OVER w'
        self.assertEqual(func.string % func.params, string)

        func = field.first_value(order_by=field.desc())
        string = 'first_value(foo.bar) OVER (ORDER BY foo.bar DESC)'
        self.assertEqual(func.string % func.params, string)

        func = field.first_value(partition_by=field)
        string = 'first_value(foo.bar) OVER (PARTITION BY foo.bar)'
        self.assertEqual(func.string % func.params, string)

        func = field.first_value(Clause('ORDER BY', field))
        string = 'first_value(foo.bar) OVER (ORDER BY foo.bar)'
        self.assertEqual(func.string % func.params, string)

        func = field.first_value(Clause('ORDER BY', field), alias='foo_alias')
        string = 'first_value(foo.bar) OVER (ORDER BY foo.bar) foo_alias'
        self.assertEqual(func.string % func.params, string)

    def test_last_value(self):
        field = new_field(table='foo', name='bar')
        func = field.last_value()
        string = 'last_value(foo.bar) OVER ()'
        self.assertEqual(func.string % func.params, string)

        func = field.last_value('w')
        string = 'last_value(foo.bar) OVER w'
        self.assertEqual(func.string % func.params, string)

        func = field.last_value(order_by=field.desc())
        string = 'last_value(foo.bar) OVER (ORDER BY foo.bar DESC)'
        self.assertEqual(func.string % func.params, string)

        func = field.last_value(partition_by=field)
        string = 'last_value(foo.bar) OVER (PARTITION BY foo.bar)'
        self.assertEqual(func.string % func.params, string)

        func = field.last_value(Clause('ORDER BY', field))
        string = 'last_value(foo.bar) OVER (ORDER BY foo.bar)'
        self.assertEqual(func.string % func.params, string)

        func = field.last_value(Clause('ORDER BY', field), alias='foo_alias')
        string = 'last_value(foo.bar) OVER (ORDER BY foo.bar) foo_alias'
        self.assertEqual(func.string % func.params, string)

    def test_nth_value(self):
        field = new_field(table='foo', name='bar')
        func = field.nth_value(5)
        string = 'nth_value(foo.bar, 5) OVER ()'
        self.assertEqual(func.string % func.params, string)

        func = field.nth_value(5, 'w')
        string = 'nth_value(foo.bar, 5) OVER w'
        self.assertEqual(func.string % func.params, string)

        func = field.nth_value(5, order_by=field.desc())
        string = 'nth_value(foo.bar, 5) OVER (ORDER BY foo.bar DESC)'
        self.assertEqual(func.string % func.params, string)

        func = field.nth_value(5, partition_by=field)
        string = 'nth_value(foo.bar, 5) OVER (PARTITION BY foo.bar)'
        self.assertEqual(func.string % func.params, string)

        func = field.nth_value(5, Clause('ORDER BY', field))
        string = 'nth_value(foo.bar, 5) OVER (ORDER BY foo.bar)'
        self.assertEqual(func.string % func.params, string)

        func = field.nth_value(5, Clause('ORDER BY', field), alias='foo_alias')
        string = 'nth_value(foo.bar, 5) OVER (ORDER BY foo.bar) foo_alias'
        self.assertEqual(func.string % func.params, string)

    def test_func(self):
        field = new_field(table='foo', name='bar')
        for x in ['json_encode', 'json_decode', 'find_first']:
            self.validate_function(
                self.base.func(x, field),
                x,
                [self.base, field]
            )
            self.validate_function(
                self.base.func(x, field, alias='test'),
                x,
                [self.base, field],
                alias='test'
            )


if __name__ == '__main__':
    # Unit test
    unittest.main()
