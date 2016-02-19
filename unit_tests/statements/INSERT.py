#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
    `Unit tests for bloom.statements.Insert`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
import pickle
import unittest
from kola import config

from vital.security import randkey

from bloom import *
from bloom.orm import QueryState
from bloom.statements import Insert


config.bind('/home/jared/apps/xfaps/vital.json')
create_kola_pool()


def new_field(type='char', value=None, name=None, table=None):
    field = getattr(fields, type.title())(value=value)
    field.field_name = name or randkey(24)
    field.table = table or randkey(24)
    return field


def new_expression(cast=int):
    if cast == bytes:
        cast = lambda x: psycopg2.Binary(str(x).encode())
    return Expression(new_field(), '=', cast(12345))


def new_function(cast=int, alias=None):
    if cast == bytes:
        cast = lambda x: psycopg2.Binary(str(x).encode())
    return Function('some_func', cast(12345), alias=alias)


def new_clause(name='FROM', *vals):
    vals = vals or ['foobar']
    return Clause(name, *vals)


class TestInsert(unittest.TestCase):
    orm = ORM()
    fields = [
        new_field('text', 'bar', name='textfield', table='foo'),
        new_field('int', 1234, name='uid', table='foo')]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.orm.set_table('foo')

    def test___init__(self):
        q = Insert(self.orm, *self.fields)
        self.assertIs(q.orm, self.orm)
        self.assertListEqual(q.fields, self.fields)
        self.orm.reset()

    def test_execute(self):
        self.orm.returning("*")
        q = Insert(self.orm, *self.fields)
        cur = q.execute()
        item = q.orm.state.get('VALUES')
        self.assertSetEqual(
            set([x for x in cur.fetchall()[0]]),
            set(item[0].params.popitem()[1]))
        self.orm.reset()

    def test__evaluate_state(self):
        clauses = [
            new_clause('INTO', safe('foo')),
            new_clause('RETURNING', safe('textfield')),
            new_clause('FROM', 'bar')
        ]
        self.orm.state.add(*clauses)
        q = Insert(self.orm, *self.fields)
        self.assertIn(clauses[0].string, q.query)
        self.assertIn(clauses[1].string, q.query)
        self.assertNotIn(clauses[2].string, q.query)
        self.assertIn(self.fields[0].field_name, q.query)
        self.assertIn(self.fields[1].field_name, q.query)

    def test_many(self):
        clauses = [
            new_clause('INTO', safe('foo')),
            new_clause('RETURNING', safe('textfield')),
            new_clause('FROM', 'bar')
        ]
        self.orm.state.add(*clauses)
        values = [
            field.real_value
            for field in self.fields
            if field._should_insert() or field.default is not None
        ]
        self.orm.values(*values)
        self.orm.values(*values)
        q = Insert(self.orm, *self.fields)
        self.assertIn(clauses[0].string, q.query)
        self.assertIn(clauses[1].string, q.query)
        self.assertNotIn(clauses[2].string, q.query)
        self.assertIn(self.fields[0].field_name, q.query)
        self.assertIn(self.fields[1].field_name, q.query)
        self.assertEqual(len(q.execute().fetchall()), 2)

    def test_pickle(self):
        values = [
            field.real_value
            for field in self.fields
            if field._should_insert() or field.default is not None
        ]
        self.orm.values(*values)
        self.orm.values(*values)
        q = Insert(self.orm, *self.fields)
        b = pickle.loads(pickle.dumps(q))
        for k in q.__dict__:
            if k == '_client':
                continue
            if isinstance(
               getattr(q, k), (str, list, tuple, dict, int, float)):
                self.assertEqual(getattr(q, k), getattr(b, k))
            else:
                self.assertTrue(
                    getattr(q, k).__class__ == getattr(b, k).__class__)


if __name__ == '__main__':
    # Unit test
    unittest.main()
