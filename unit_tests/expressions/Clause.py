#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
    `Unit tests for bloom.expressions.Clause`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
import unittest
import psycopg2

from vital.security import randkey

from vital.tools.dicts import merge_dict
from bloom.expressions import *
from bloom import *
from bloom import fields


def new_field(type='varchar'):
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


def new_function(cast=int, alias=None):
    if cast == bytes:
        cast = lambda x: psycopg2.Binary(str(x).encode())
    return Function('some_func', cast(12345), alias=alias)

def new_clause(name='FROM'):
    return Clause(name, 'foobar')



class TestClause(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def test_init_(self):
        for val in ('FROM', 'from'):
            base = Clause(val, 'some_table')
            self.assertEqual(base.clause, 'FROM')
        self.assertListEqual(list(base.args), ['some_table'])

    def test__format_arg(self):
        vals = (
            (new_field(), new_field('varchar')),
            (new_field().alias('test'),),
            (new_field().alias('test', use_field_name=True),),
            (safe('some_table'),),
            ('some_table',),
            (new_expression(),),
            (new_function(),),
            (1234,)
        )
        for val in vals:
            base = Clause('FROM', *val, use_field_name=True)
            self.assertListEqual(list(base.args), list(val))
            if hasattr(val, 'params'):
                self.assertDictEqual(base.params, val.params)
                for k, v in base.params:
                    self.assertIn('%(' + k + ')s', base.string)
        for val in vals:
            base = Clause('FROM', *val, wrap=True)
            self.assertListEqual(list(base.args), list(val))
            pdicts = (v.params if hasattr(v, 'params') else {} for v in val)
            for k, v in merge_dict(*pdicts).items():
                self.assertIn(k, base.params)
            for k, v in base.params.items():
                self.assertIn('%(' + k + ')s', base.string)
            self.assertTrue(
                base.string.endswith(')') and base.string.startswith('FROM ('))


if __name__ == '__main__':
    # Unit test
    unittest.main()
