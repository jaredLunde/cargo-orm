#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
    `Unit tests for vital.sql.expressions.Subquery`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
import unittest

from vital.security import randkey
from vital.sql import *


def new_field(type='char'):
    field = getattr(fields, type.title())(value=1234)
    keyspace = 'aeioubcdlhzpwnmp'
    name = randkey(24, keyspace)
    table = randkey(24, keyspace)
    field.field_name = name
    field.table = table
    return field


def new_expression(cast=int):
    if cast == bytes:
        cast = lambda x: psycopg2.Binary(str(x).encode())
    return  Expression(new_field(), '=', cast(12345))


def new_function(cast=int):
    if cast == bytes:
        cast = lambda x: psycopg2.Binary(str(x).encode())
    return  Function('some_func', cast(12345))


class TestSubquery(unittest.TestCase):

    def test___init__(self):
        fields = (new_field('int'), new_field('char'))
        q = INSERT(ORM(), *fields)
        base = Subquery(q)
        self.assertIs(base.subquery, q)
        self.assertIn(q.query, base.query)
        self.assertIs(base.params, q.params)

    def test_alias(self):
        fields = (new_field('int'), new_field('char'))
        q = INSERT(ORM(), *fields)
        base = Subquery(q, alias='subalias')
        self.assertEqual(base.alias, 'subalias')
        self.assertTrue(base.query.endswith('subalias'))

    def test_exists(self):
        fields = (new_field('int'), new_field('char'))
        q = SELECT(ORM(), *fields)
        base = Subquery(q)
        func = base.exists()
        self.assertIsInstance(func, Function)
        self.assertEqual(func.func, 'EXISTS')
        self.assertTupleEqual(func.args, tuple([base,]))
        func = base.exists(alias='foo')
        self.assertIsInstance(func, Function)
        self.assertEqual(func.func, 'EXISTS')
        self.assertTupleEqual(func.args, tuple([base,]))
        self.assertEqual(func.alias, 'foo')

    def test_not_exists(self):
        fields = (new_field('int'), new_field('char'))
        q = SELECT(ORM(), *fields)
        base = Subquery(q)
        func = base.not_exists()
        self.assertIsInstance(func, Function)
        self.assertEqual(func.func, 'NOT EXISTS')
        self.assertTupleEqual(func.args, tuple([base,]))
        func = base.not_exists(alias='foo')
        self.assertIsInstance(func, Function)
        self.assertEqual(func.func, 'NOT EXISTS')
        self.assertTupleEqual(func.args, tuple([base,]))
        self.assertEqual(func.alias, 'foo')


if __name__ == '__main__':
    # Unit test
    unittest.main()
