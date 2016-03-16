#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
    `Unit tests for cargo.expressions.Function`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
import unittest
import psycopg2

from vital.security import randkey

from cargo.expressions import *
from cargo import *
from cargo import fields


# TODO: :class:WindowFunction


def new_field(type='varchar', table=None, name=None):
    field = getattr(fields, type.title())()
    keyspace = 'aeioubcdlhzpwnmp'
    name = name or randkey(24, keyspace)
    table = table or randkey(24, keyspace)
    field.field_name = name
    field.table = table
    return field


def new_expression(cast=int):
    if cast == bytes:
        cast = lambda x: psycopg2.Binary(str(x).encode())
    return  Expression(new_field(), '=', cast(12345))


def new_function(cast=int, alias=None):
    if cast == bytes:
        cast = lambda x: psycopg2.Binary(str(x).encode())
    return  Function('some_func', cast(12345), alias=alias)


class TestFunction(unittest.TestCase):
    vals = (
        (1234, '1234', new_field('int'), safe('frisco')),
        (new_field('varchar'), new_expression(int), new_expression(str)),
        (new_expression(bytes),),
        tuple()
    )

    def test_args(self):
        for val in self.vals:
            base = Function('foo', *val)
            self.assertTupleEqual(base.args, val)
            self.assertEqual(base.func, 'foo')
            for k, v in base.params.items():
                self.assertIn('%(' + k + ')s', base.string)
                self.assertTrue(v in val)
            for v in val:
                if isinstance(v, Field):
                    self.assertIn(v.name, base.string)

    def test_use_field_names(self):
        for val in self.vals:
            base = Function('foo', *val, use_field_name=True)
            self.assertTupleEqual(base.args, val)
            self.assertEqual(base.func, 'foo')
            for k, v in base.params.items():
                self.assertIn('%(' + k + ')s', base.string)
                self.assertTrue(v in val)
            for v in val:
                if isinstance(v, Field):
                    self.assertIn(v.field_name, base.string)
                    self.assertNotIn(v.name, base.string)

    def test_alias(self):
        for val in self.vals:
            base = Function('foo', *val, use_field_name=True, alias='bar')
            self.assertEqual(base.alias, 'bar')

    def test_over(self):
        field = new_field(table='foo', name='bar')
        fieldb = new_field(table='foo', name='bar_partition')
        func = Function('avg', field)
        over = func.over()
        self.assertEqual(over.string, 'avg(foo.bar) OVER ()')
        over = func.over('w')
        self.assertEqual(over.string, 'avg(foo.bar) OVER w')
        over = func.over(order_by=field)
        self.assertEqual(over.string, 'avg(foo.bar) OVER (ORDER BY foo.bar)')
        over = func.over(partition_by=fieldb, order_by=field)
        self.assertEqual(over.string,
                         'avg(foo.bar) OVER (PARTITION BY foo.bar_partition ' +
                         'ORDER BY foo.bar)')
        over = func.over(partition_by=fieldb, order_by=field,
                         alias='foo_alias')
        self.assertEqual(over.string,
                         'avg(foo.bar) OVER (PARTITION BY foo.bar_partition ' +
                         'ORDER BY foo.bar) foo_alias')
        over = func.over(Clause('PARTITION BY', fieldb))
        self.assertEqual(over.string,
                         'avg(foo.bar) OVER (PARTITION BY foo.bar_partition)')
        over = func.over(Clause('PARTITION BY', fieldb), alias='foo_alias')
        self.assertEqual(over.string,
                         'avg(foo.bar) OVER (PARTITION BY ' +
                         'foo.bar_partition) foo_alias')


class TestWindowFunctions(unittest.TestCase):

    def test_row_number(self):
        field = new_field(table='foo', name='bar')
        func = WindowFunctions.row_number()
        string = 'row_number() OVER ()'
        self.assertEqual(func.string % func.params, string)

        func = WindowFunctions.row_number('w')
        string = 'row_number() OVER w'
        self.assertEqual(func.string % func.params, string)

        func = WindowFunctions.row_number(order_by=field.desc())
        string = 'row_number() OVER (ORDER BY foo.bar DESC)'
        self.assertEqual(func.string % func.params, string)

        func = WindowFunctions.row_number(partition_by=field)
        string = 'row_number() OVER (PARTITION BY foo.bar)'
        self.assertEqual(func.string % func.params, string)

        func = WindowFunctions.row_number(Clause('ORDER BY', field))
        string = 'row_number() OVER (ORDER BY foo.bar)'
        self.assertEqual(func.string % func.params, string)

        func = WindowFunctions.row_number(Clause('ORDER BY', field),
                                          alias='foo_alias')
        string = 'row_number() OVER (ORDER BY foo.bar) foo_alias'
        self.assertEqual(func.string % func.params, string)

    def test_rank(self):
        field = new_field(table='foo', name='bar')
        func = WindowFunctions.rank()
        string = 'rank() OVER ()'
        self.assertEqual(func.string % func.params, string)

        func = WindowFunctions.rank('w')
        string = 'rank() OVER w'
        self.assertEqual(func.string % func.params, string)

        func = WindowFunctions.rank(order_by=field.desc())
        string = 'rank() OVER (ORDER BY foo.bar DESC)'
        self.assertEqual(func.string % func.params, string)

        func = WindowFunctions.rank(partition_by=field)
        string = 'rank() OVER (PARTITION BY foo.bar)'
        self.assertEqual(func.string % func.params, string)

        func = WindowFunctions.rank(Clause('ORDER BY', field))
        string = 'rank() OVER (ORDER BY foo.bar)'
        self.assertEqual(func.string % func.params, string)

        func = WindowFunctions.rank(Clause('ORDER BY', field),
                                    alias='foo_alias')
        string = 'rank() OVER (ORDER BY foo.bar) foo_alias'
        self.assertEqual(func.string % func.params, string)

    def test_dense_rank(self):
        field = new_field(table='foo', name='bar')
        func = WindowFunctions.dense_rank()
        string = 'dense_rank() OVER ()'
        self.assertEqual(func.string % func.params, string)

        func = WindowFunctions.dense_rank('w')
        string = 'dense_rank() OVER w'
        self.assertEqual(func.string % func.params, string)

        func = WindowFunctions.dense_rank(order_by=field.desc())
        string = 'dense_rank() OVER (ORDER BY foo.bar DESC)'
        self.assertEqual(func.string % func.params, string)

        func = WindowFunctions.dense_rank(partition_by=field)
        string = 'dense_rank() OVER (PARTITION BY foo.bar)'
        self.assertEqual(func.string % func.params, string)

        func = WindowFunctions.dense_rank(Clause('ORDER BY', field))
        string = 'dense_rank() OVER (ORDER BY foo.bar)'
        self.assertEqual(func.string % func.params, string)

        func = WindowFunctions.dense_rank(Clause('ORDER BY', field),
                                          alias='foo_alias')
        string = 'dense_rank() OVER (ORDER BY foo.bar) foo_alias'
        self.assertEqual(func.string % func.params, string)

    def test_percent_rank(self):
        field = new_field(table='foo', name='bar')
        func = WindowFunctions.percent_rank()
        string = 'percent_rank() OVER ()'
        self.assertEqual(func.string % func.params, string)

        func = WindowFunctions.percent_rank('w')
        string = 'percent_rank() OVER w'
        self.assertEqual(func.string % func.params, string)

        func = WindowFunctions.percent_rank(order_by=field.desc())
        string = 'percent_rank() OVER (ORDER BY foo.bar DESC)'
        self.assertEqual(func.string % func.params, string)

        func = WindowFunctions.percent_rank(partition_by=field)
        string = 'percent_rank() OVER (PARTITION BY foo.bar)'
        self.assertEqual(func.string % func.params, string)

        func = WindowFunctions.percent_rank(Clause('ORDER BY', field))
        string = 'percent_rank() OVER (ORDER BY foo.bar)'
        self.assertEqual(func.string % func.params, string)

        func = WindowFunctions.percent_rank(Clause('ORDER BY', field),
                                            alias='foo_alias')
        string = 'percent_rank() OVER (ORDER BY foo.bar) foo_alias'
        self.assertEqual(func.string % func.params, string)

    def test_cume_dist(self):
        field = new_field(table='foo', name='bar')
        func = WindowFunctions.cume_dist()
        string = 'cume_dist() OVER ()'
        self.assertEqual(func.string % func.params, string)

        func = WindowFunctions.cume_dist('w')
        string = 'cume_dist() OVER w'
        self.assertEqual(func.string % func.params, string)

        func = WindowFunctions.cume_dist(order_by=field.desc())
        string = 'cume_dist() OVER (ORDER BY foo.bar DESC)'
        self.assertEqual(func.string % func.params, string)

        func = WindowFunctions.cume_dist(partition_by=field)
        string = 'cume_dist() OVER (PARTITION BY foo.bar)'
        self.assertEqual(func.string % func.params, string)

        func = WindowFunctions.cume_dist(Clause('ORDER BY', field))
        string = 'cume_dist() OVER (ORDER BY foo.bar)'
        self.assertEqual(func.string % func.params, string)

        func = WindowFunctions.cume_dist(Clause('ORDER BY', field),
                                         alias='foo_alias')
        string = 'cume_dist() OVER (ORDER BY foo.bar) foo_alias'
        self.assertEqual(func.string % func.params, string)

    def test_ntile(self):
        field = new_field(table='foo', name='bar')
        func = WindowFunctions.ntile(4)
        string = 'ntile(4) OVER ()'
        self.assertEqual(func.string % func.params, string)

        func = WindowFunctions.ntile(4, 'w')
        string = 'ntile(4) OVER w'
        self.assertEqual(func.string % func.params, string)

        func = WindowFunctions.ntile(4, order_by=field.desc())
        string = 'ntile(4) OVER (ORDER BY foo.bar DESC)'
        self.assertEqual(func.string % func.params, string)

        func = WindowFunctions.ntile(4, partition_by=field)
        string = 'ntile(4) OVER (PARTITION BY foo.bar)'
        self.assertEqual(func.string % func.params, string)

        func = WindowFunctions.ntile(4, Clause('ORDER BY', field))
        string = 'ntile(4) OVER (ORDER BY foo.bar)'
        self.assertEqual(func.string % func.params, string)

        func = WindowFunctions.ntile(4, Clause('ORDER BY', field),
                                     alias='foo_alias')
        string = 'ntile(4) OVER (ORDER BY foo.bar) foo_alias'
        self.assertEqual(func.string % func.params, string)


if __name__ == '__main__':
    # Unit test
    unittest.main()
